#!/usr/bin/env python
#
#

import os, sys, hashlib, re, multiprocessing
from Queue import Queue
from threading import Thread,current_thread
import time, datetime, errno
import filecmp
import csv

# To stop the queue from consuming all the RAM available
MaxQueue = 1000
validModes = ["SNAPSHOT", "LIVE", "STATS", "REWORK"]
cargoModes = ["SNAPSHOT", "REWORK"]
validFiles = ["log", "failed", "added", "modified", "unchanged", "symlink", "directory", "config", "cargo", "removed"]
includeStats = {'full' : ['added', 'modified', 'unchanged'], 'incr' : ['added', 'modified']}
stats = {}
statsFields = {}

def usage():
    print "Usage: md5cargo.py [OPTIONS]"
    print "-n <dataset name>"
    print "          - a meaningful name for the dataset, this should be consistent for all snapshots"
    print "            of a file structure."
    print "-mode <mode>"
    print "          - a number of modes are possible:"
    print "            SNAPSHOT - use with either a single snapshot or to generate an incremental between"
    print "                       two snapshots. By default this gnerates cargo files (including checksums)."
#    print "            LIVE     - use with either a single snapshot or to generate an incremental between"
#    print "                       a snapshot and a live filesystem. This will not generate a cargo file"
#    print "                       since the checksums are highly likely to change."
    print "            STATS    - gathers snapshots doesn't generate cargo files."
    print "            REWORK   - generate a cargo file based on a list of paths in files."
    print "-t <yyyymmddThhmmss>"
    print "          - if not supplied the timestamp will be generated at the start of a run. Where a"
    print "            filesystem snapshot is being processed then it is more meaningful to use the "
    print "            timestamp from the newer snapshot."
    print "-s        - follow symlinks and ingest their target, defaults to outputing symlinks and their"
    print "            targets in the symlink file."
    print "-n <dir>  - snapshot mode, used when the new source filesystem is a snapshot."
    print "-p <dir>  - previous snapshot, if omitted then a full ingest based on the file tree passed with"
    print "            either the -l or -s options."
    print "-f <file> - a list of files to explicitly attempt ingest for, may be used to re-try failed files"
    print "            from a previous ingest snapshot." 
    print "-o <dir>  - output directory, where to save the output files."
    print "            <name>/<iso timestamp>/log       - messages and errors during the run"
    print "            <name>/<iso timestamp>/removed   - files delete since previous snapshot"
    print "            <name>/<iso timestamp>/unchanged - files unchanged since previous snapshot"
    print "            <name>/<iso timestamp>/added     - files created since previous snapshot"
    print "            <name>/<iso timestamp>/modified  - files modified since previous snapshot"
    print "            <name>/<iso timestamp>/failed    - files that couldn't be processed"
    print "            <name>/<iso timestamp>/directory - a list of the leaf nodes in the folder tree"
    print "            <name>/<iso timestamp>/cargo     - a cargo file is generated in snapshot mode, "
    print "                                               containing all the addeded and modified files "
    print "                                               along with their MD5 checksum, in MD5deep format."
    print "            <name>/<iso timestamp>/symlink   - a list of the symlinks and their targets."
#    print "-jnn      - Controls multi-threading. By default the program will create one thread per CPU core, one thread is used for writing to the output files, the rest for scanning the file system and calculating MD5 hashes. Multi-threading causes output filenames to be in non-deterministic order, as files that take longer to hash will be delayed while they are hashed."
    return;

def openFiles(filelist):
    fileHandles = {}
    try:
        for file in filelist:
            fileHandles[file] = open(os.path.join(filebase,file), "a")
    except ValueError:
        sys.stderr.write("can't open %s"%file)
        sys.exit(-1)
    return fileHandles;

def closeFiles(fileHandles):
    for file in fileHandles:
        fileHandles[file].close()
    return;


def loadBoundaries(file):
    boundaries = []
    global stats
    global statsFields
    fields = {'Category' : ''}
    countfields = {}
    bytesfields = {}

    try:
        with open(file) as csvfile:
            statsBoundaries = csv.DictReader(csvfile)
            for row in statsBoundaries:
                boundaries.append((row['Name'], row['Lower'], row['Upper']))
                bytesfields['bytes '+row['Name']] = 0
                countfields['count '+row['Name']] = 0

    except ValueError:
        sys.stderr.write("Can't load stats boundaries from file %s (error: %s)\n"%(file, ValueError))
        sys.exit(-1)

    fields = dict(fields.items() + bytesfields.items() + countfields.items())
    statsFields = ['Category'] + bytesfields.keys() + countfields.keys()

    for category in includeStats['full']:
        stats[category] = fields.copy()
        stats[category]['Category'] = category
    return boundaries


def updateStats(file, bytes):
    global stats 

    for boundary in statsBoundaries:
        name, lower, upper = boundary
        if (bytes >= int(lower) and bytes < int(upper)) or (bytes >= int(lower) and upper == ""):
            stats[file]['count '+name] += 1
            stats[file]['bytes '+name] += bytes


def exportStats():
    # exports stats based on categories listed in includeStats & includeStats 
    try:
        for statSet in includeStats:
            file = os.path.join(filebase,'stats_'+statSet+'.csv')
            with open(file, "wb") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=statsFields)
                writer.writeheader()
                for category in includeStats[statSet]:
                    writer.writerow(stats[category])

    except ValueError:
        sys.stderr.write("Can't export stats to file %s (error: %s)\n"%(filefull, ValueError))
        print stats
 

# Calculate the MD5 sum of the file
#
def md5sum(filename, blocksize=65536):
    hash = hashlib.md5()
    try:
        with open(filename, "rb") as f:
            for block in iter(lambda: f.read(blocksize), ""):
                hash.update(block)
    except IOError as e:
        r.put(("failed", "", "%s %s%s"%(e, filename, opt_snapshotEOL)))
        sys.stderr.write(message)

    return hash.hexdigest();


# outputWorker used by threads to process output to the various output files. The must never be more
# than of this type of thread running.
#
def outputResult(i, f, q):
    while True:
        file, bytes, message = q.get()

        # update stats for this file
        if file in list( set(includeStats['full']) | set(includeStats['incr']) ):
            updateStats(file, bytes)

        if file not in validFiles:
            sys.stderr.write("invalid output file'%s'"%file)
            sys.exit(-1)
        # write out the message to the end of the file
        try:
           f[file].write(message)
        except:
            sys.stderr.write("Cannot write to %s\n"%file)
            sys.stderr.write("%s: %s\n"%(file, message))
            exit(-1)
        q.task_done()


# pathWorker used by threads to process an incremental snapshot of the file tree
#
def processIncr(i, q, r):
    while not terminateThreads:
        relPath = q.get()
        cargo = (opt_mode in cargoModes)
        mkcargo = False

        if relPath == os.path.abspath(relPath):
            # This is an explict entry from the rework file
            absPath = relPath
            oldPath = ""
        else:
            # This is an entry for the current snapshot
            absPath = os.path.abspath(os.path.join(opt_currentSnapshot, relPath))
            oldPath = os.path.abspath(os.path.join(opt_previousSnapshot, relPath))


        if opt_debug:
            r.put(("log", "", "Thread %s - %s%s"%(current_thread().getName(), relPath, opt_snapshotEOL)))

        # What have we picked up from the queue
        if os.path.isdir(absPath):
            r.put(("directory", "", "%s%s"%(relPath, opt_snapshotEOL)))

            if os.path.isdir(oldPath):
                for removed in set(os.listdir(oldPath)).difference(os.listdir(absPath)):
                    r.put(("removed", "", "%s%s"%(os.path.join(relPath, removed), opt_snapshotEOL)))
                for common in set(os.listdir(absPath)).intersection(os.listdir(oldPath)):
                    q.put(os.path.join(relPath, common))
                for added in set(os.listdir(absPath)).difference(os.listdir(oldPath)):
                    q.put(os.path.join(relPath, added))
            else:
                for childPath in os.listdir(absPath):
                    q.put(os.path.join(relPath, childPath))

        elif (not opt_followSymlink) and os.path.islink(absPath):
            if os.path.realpath(oldPath) == os.path.realpath(absPath):
                r.put(("unchanged", os.path.getsize(absPath), "%s%s"%(relPath, opt_snapshotEOL)))
            else:
                r.put(("symlink", "", "%s %s%s"%(relPath, os.path.realpath(absPath), opt_snapshotEOL)))

        elif os.path.isfile(absPath):
            if os.path.isfile(oldPath):
                if filecmp.cmp(oldPath, absPath):
                    r.put(("unchanged", os.path.getsize(absPath), "%s%s"%(relPath, opt_snapshotEOL)))
                    mkcargo = False
                else:
                    r.put(("modified", os.path.getsize(absPath), "%s%s"%(relPath, opt_snapshotEOL)))
                    mkcargo = True
            else:    
                r.put(("added", os.path.getsize(absPath), "%s%s"%(relPath, opt_snapshotEOL)))
                mkcargo = True
            
            if cargo and mkcargo:
                absPath = absPath.replace("\r","")
                absPath = absPath.replace("\n","")

                hash = md5sum(absPath)
                hash = hash.replace(" ","")

                r.put(("cargo", "", "%s  %s%s"%(hash, absPath, opt_cargoEOL)))

        else:
            r.put(("failed", "", "invalid path: %s%s"%(relPath, opt_snapshotEOL)))
        q.task_done()


# pathWorker used by threads to process a Full snapshot of the file tree
#
def processFull(i, q, r):
    while not terminateThreads:
        relPath = q.get()

        cargo = (opt_mode in cargoModes)

        if relPath == os.path.abspath(relPath):
            # This is an explict entry from the rework file
            absPath = relPath
        else:
            # This is an entry for the current snapshot
            absPath = os.path.abspath(os.path.join(opt_currentSnapshot, relPath))

        if opt_debug:
            r.put(("log", "", "Thread %s - %s%s"%(current_thread().getName(), relPath, opt_snapshotEOL)))

        # What have we picked up from the queue
        if os.path.isdir(absPath):
            for childPath in os.listdir(absPath):
		q.put(os.path.join(relPath, childPath))
            r.put(("directory", "", "%s%s"%(relPath, opt_snapshotEOL)))

        elif (not opt_followSymlink) and os.path.islink(absPath):
            r.put(("symlink", "", "%s %s%s"%(relPath, os.path.realpath(absPath), opt_snapshotEOL)))

        elif os.path.isfile(absPath):
            if not (opt_followSymlink and os.path.islink(absPath)):
                r.put(("added", os.path.getsize(absPath), "%s%s"%(relPath, opt_snapshotEOL)))
                if cargo:
                    absPath = absPath.replace("\r","")
                    absPath = absPath.replace("\n","")

                    hash = md5sum(absPath)
                    hash = hash.replace(" ","")

                    r.put(("cargo", "", "%s  %s%s"%(hash, absPath, opt_cargoEOL)))

        else:
            r.put(("failed", "", "invalid path: %s%s"%(relPath, opt_snapshotEOL)))
        q.task_done()


# for files that can't be processed by dircmp we have to expend some extra energy and use
# MD5 checksums
#
def compareFunnyFile(fileA, fileB, r):
    try:
        hashA = md5sum(fileA)
        hashB = md5sum(fileB)
    except IOError as e:
        message = "%s %s or %s%s"%(e, fileA, fileB, opt_snapshotEOL)
        r.put(("failed", "", message))
        sys.stderr.write(message)
    return hashA == hashB;

def logConfig(r):
    r.put(("config", "", "pathWorkers %s%s"%(opt_threads, opt_snapshotEOL)))
    r.put(("config", "", "reportWorkers 1%s"%(opt_snapshotEOL)))
    r.put(("config", "", "snapshotName %s%s"%(opt_snapshotName, opt_snapshotEOL)))
    r.put(("config", "", "timestamp %s%s"%(opt_snapshotTimestamp, opt_snapshotEOL)))
    r.put(("config", "", "previousSnapshot %s%s"%(opt_previousSnapshot, opt_snapshotEOL)))
    r.put(("config", "", "currentSnapshot %s%s"%(opt_currentSnapshot, opt_snapshotEOL)))
    r.put(("config", "", "previousAbsPath %s%s"%(os.path.abspath(opt_previousSnapshot), opt_snapshotEOL)))
    r.put(("config", "", "currentAbsPath %s%s"%(os.path.abspath(opt_currentSnapshot), opt_snapshotEOL)))
    r.put(("config", "", "sourceType %s%s"%(opt_mode, opt_snapshotEOL)))
    r.put(("config", "", "followSymlinks %s%s"%(opt_followSymlink, opt_snapshotEOL)))
    r.put(("config", "", "explicitFiles %s%s"%(opt_explicitPaths, opt_snapshotEOL)))
    return;

if __name__ == '__main__':
    opt_snapshotEOL = "\n"
    opt_cargoEOL = "\0"
    opt_threads = multiprocessing.cpu_count()-1
    opt_snapshotName =""
    opt_filebase ="./"
    opt_snapshotTimestamp = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
    opt_previousSnapshot = ""
    opt_currentSnapshot = ""
    opt_outputDirectory = ""
    opt_mode = ""
    opt_explicitPaths =""
    opt_followSymlink = False
    opt_debug = False
    opt_boundaryFile = os.path.join(os.path.dirname( os.path.realpath( __file__ )), 'boundaries.csv') 

   
    args = sys.argv[1:]
    it = iter(args)
    for i in it:
        if i.upper() =='-MODE':
            opt_mode = next(it).upper()
            #This needs sorting out
            if not opt_mode in validModes:
                sys.stdout.write("Unrecognised mode: %s\n"%opt_mode)
                sys.exit(-1)
	elif i.upper() == '-DEBUG':
                opt_debug = True
        elif i.upper() == '-NAME':
            opt_snapshotName = next(it)
            continue 
        elif i == '-t':
            tmp_timestamp = next(it)
            if not datetime.datetime.strptime( tmp_timestamp, "%Y%m%dT%H%M%S" ):
                sys.stdout.write("Is not valid ISO timestamp %s\n"%tmp_timestamp)
                sys.exit(-1)
            else:
                opt_snapshotTimestamp = tmp_timestamp
            continue
        elif i == '-o' or i =='-O':
            opt_outputDirectory = next(it)
            if not os.path.isdir(opt_outputDirectory):
                sys.stderr.write("Can not find output directory %s\n"%opt_outputDirectory)
                sys.exit(-1)
            continue
        elif i == '-p' or i =='-P':
            opt_previousSnapshot = next(it)
            if not os.path.isdir(opt_previousSnapshot):
                sys.stderr.write("Can not find open previous snapshot directory %s\n"%opt_previousSnapshot)
                sys.exit(-1)
            continue
        elif i == '-f' or i =='-F':
            opt_explicitPaths = next(it)
            if not os.path.isfile(opt_explicitPaths):
                sys.stderr.write("Can not find open list of files to ingest %s\n"%opt_explicitPaths)
                sys.exit(-1)
        elif i == '-n' or i =='-N':
            opt_currentSnapshot = next(it)
            if not os.path.isdir(opt_currentSnapshot):
                sys.stderr.write("Cannot find open snapshot directory %s\n"%opt_currentSnapshot)
                sys.exit(-1)
            continue
        elif i == '-s' or i =='-S':
            opt_followSymlink = True
            continue
        elif i.startswith('-j'):
            opt_threads = int(i[2:]) -1
            # we need to save 1 thread for processing the results queue
            continue

    # load stats boundaries
    statsBoundaries = loadBoundaries( opt_boundaryFile )

    # We'll need somewhere to output this lot
    filebase = os.path.join(opt_outputDirectory, opt_snapshotName, opt_snapshotTimestamp)
    try:
        os.makedirs(filebase)
    except ValueError:
        sys.stderr.write("Cannot create output directory (error: %s)\n"%ValueError)
        sys.exit(-1)


    terminateThreads = False

    # initialise Queues
    pathQueue = Queue(MaxQueue)
    resultsQueue = Queue(opt_threads*MaxQueue)

    logConfig(resultsQueue)

    fileHandles = openFiles(validFiles)

    # setup the single results worker
    resultsWorker = Thread(target=outputResult, args=(i, fileHandles, resultsQueue))
    resultsWorker.setDaemon(True)
    resultsWorker.start()

    # setup the pool of path workers
    for i in range(opt_threads):
        if opt_previousSnapshot:
            pathWorker = Thread(target=processIncr, args=(i, pathQueue, resultsQueue))
        else:
            pathWorker = Thread(target=processFull, args=(i, pathQueue, resultsQueue))
        pathWorker.setDaemon(True)
        pathWorker.start()

    if opt_mode == "REWORK":
        try:
            for line in open(opt_explicitPaths):
                explicitFile = line.rstrip('\n').rstrip('\r')
                if explicitFile == os.path.abspath(explicitFile):
                    pathQueue.put(explicitFile)
                else:
                    resultsQueue.put(("failed", "must be absolute path: %s%s"%(explicitFile, opt_snapshotEOL)))
        except ValueError:
            sys.stderr.write("Cannot read list of files to ingest from %s (error: %s)\n"%(opt_explicitPaths, ValueError))
            sys.exit(-1)
    else:
        pathQueue.put(".")
     
    try:

        # lets just hang back and wait for the queues to empty
        while not terminateThreads:
             time.sleep(.1)
             if pathQueue.empty():
                 pathQueue.join()
                 resultsQueue.join()
                 exportStats()
                 closeFiles(fileHandles)
                 exit(1)
    except KeyboardInterrupt:
        # Time to tell all the threads to bail out
        terminateThreads = True
        raise
