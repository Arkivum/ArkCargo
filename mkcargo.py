#!/usr/bin/env python
#
#

import os, sys, hashlib, re, multiprocessing
from Queue import Queue
from threading import Thread,current_thread
import time, datetime, errno
import filecmp


# To stop the queue from consuming all the RAM available
MaxQueue = 1000

def usage():
    print "Usage: md5cargo.py [OPTIONS]"
    print "-n <dataset name>"
    print "          - a meaningful name for the dataset, this should be consistent for all snapshots"
    print "            of a file structure."
    print "-t <yyyymmddThhmmss>"
    print "          - if not supplied the timestamp will be generated at the start of a run. Where a"
    print "            filesystem snapshot is being processed then it is more meaningful to use the "
    print "            timestamp from the newer snapshot."
    print "-s        - follow symlinks and ingest their target, defaults to outputing symlinks and their"
    print "            targets in the symlink file."
    print "-l <dir>  - live mode, to be used when the source filesystem does not support snapshots."
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
    print "-jnn      - Controls multi-threading. By default the program will create one thread per CPU core, one thread is used for writing to the output files, the rest for scanning the file system and calculating MD5 hashes. Multi-threading causes output filenames to be in non-deterministic order, as files that take longer to hash will be delayed while they are hashed."
    return;


# Calculate the MD5 sum of the file
#
def md5sum(filename, blocksize=65536):
    hash = hashlib.md5()
    with open(filename, "rb") as f:
        for block in iter(lambda: f.read(blocksize), ""):
            hash.update(block)
    return hash.hexdigest();


# outputWorker used by threads to process output to the various output files. The must never be more
# than of this type of thread running.
#
def outputResult(i, q):
    while True:
        file, message = q.get()
        try:
            with open(filebase+file, "a") as outputFile:
                outputFile.write(message)
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
        cargo = False

        if relPath == os.path.abspath(relPath):
            # This is an explict entry from the rework file
            absPath = relPath
            oldPath = ""
        else:
            # This is an entry for the current snapshot
            absPath = os.path.abspath(opt_currentSnapshot+"/"+relPath)
            oldPath = os.path.abspath(opt_previousSnapshot+"/"+relPath)


        if opt_debug:
            r.put(("log", "Thread %s - %s%s"%(current_thread().getName(), relPath, opt_snapshotEOL)))

        # What have we picked up from the queue
        if os.path.isdir(absPath):
            r.put(("directory", "%s%s"%(relPath, opt_snapshotEOL)))

            if os.path.isdir(oldPath):
                for removed in set(os.listdir(oldPath)).difference(os.listdir(absPath)):
                    r.put(("removed", "%s%s"%(relPath+"/"+removed, opt_snapshotEOL)))
                for common in set(os.listdir(absPath)).intersection(os.listdir(oldPath)):
                    q.put(relPath+"/"+common)
                for added in set(os.listdir(absPath)).difference(os.listdir(oldPath)):
                    q.put(relPath+"/"+added)
            else:
                for childPath in os.listdir(absPath):
                    q.put(relPath+"/"+childPath)

        elif (not opt_followSymlink) and os.path.islink(absPath):
            if os.path.realpath(oldPath) == os.path.realpath(absPath):
                r.put(("unchanged", "%s%s"%(relPath, opt_snapshotEOL)))
            else:
                r.put(("symlink", "%s %s%s"%(relPath, os.path.realpath(absPath), opt_snapshotEOL)))

        elif os.path.isfile(absPath):
            if os.path.isfile(oldPath):
                if filecmp.cmp(oldPath, absPath):
                    r.put(("unchanged", "%s%s"%(relPath, opt_snapshotEOL)))
                else:
                    r.put(("modified", "%s%s"%(relPath, opt_snapshotEOL)))
                    if opt_sourceType == "SNAPSHOT":
                        cargo = True
            else:    
                r.put(("added", "%s%s"%(relPath, opt_snapshotEOL)))
                if opt_sourceType == "SNAPSHOT":
                    cargo = True
            
            if cargo:
                absPath = absPath.replace("\r","")
                absPath = absPath.replace("\n","")

                hash = md5sum(absPath)
                hash = hash.replace(" ","")

                r.put(("cargo", "%s  %s%s"%(hash, absPath, opt_cargoEOL)))

        else:
            r.put(("error", "invalid path: %s%s"%(relPath, opt_snapshotEOL)))

        q.task_done()


# pathWorker used by threads to process a Full snapshot of the file tree
#
def processFull(i, q, r):
    while not terminateThreads:
        relPath = q.get()

        if relPath == os.path.abspath(relPath):
            # This is an explict entry from the rework file
            absPath = relPath
        else:
            # This is an entry for the current snapshot
            absPath = os.path.abspath(opt_currentSnapshot+"/"+relPath)

        if opt_debug:
            r.put(("log", "Thread %s - %s%s"%(current_thread().getName(), relPath, opt_snapshotEOL)))

        # What have we picked up from the queue
        if os.path.isdir(absPath):
            for childPath in os.listdir(absPath):
		q.put(relPath+"/"+childPath)
            r.put(("directory", "%s%s"%(relPath, opt_snapshotEOL)))

        elif (not opt_followSymlink) and os.path.islink(absPath):
            r.put(("symlink", "%s %s%s"%(relPath, os.path.realpath(absPath), opt_snapshotEOL)))

        elif os.path.isfile(absPath):
            if not (opt_followSymlink and os.path.islink(absPath)):
                r.put(("added", "%s%s"%(relPath, opt_snapshotEOL)))
                if (opt_sourceType == "SNAPSHOT"):
                    absPath = absPath.replace("\r","")
                    absPath = absPath.replace("\n","")

                    hash = md5sum(absPath)
                    hash = hash.replace(" ","")

                    r.put(("cargo", "%s  %s%s"%(hash, absPath, opt_cargoEOL)))

        else:
            r.put(("error", "invalid path: %s%s"%(relPath, opt_snapshotEOL)))
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
        r.put(("error", message))
        sys.stderr.write(message)
    return hashA == hashB;

def logConfig(r):
    r.put(("config", "pathWorkers %s%s"%(opt_threads, opt_snapshotEOL)))
    r.put(("config", "reportWorkers 1%s"%(opt_snapshotEOL)))
    r.put(("config", "snapshotName %s%s"%(opt_snapshotName, opt_snapshotEOL)))
    r.put(("config", "timestamp %s%s"%(opt_snapshotTimestamp, opt_snapshotEOL)))
    r.put(("config", "previousSnapshot %s%s"%(opt_previousSnapshot, opt_snapshotEOL)))
    r.put(("config", "currentSnapshot %s%s"%(opt_currentSnapshot, opt_snapshotEOL)))
    r.put(("config", "previousAbsPath %s%s"%(os.path.abspath(opt_previousSnapshot), opt_snapshotEOL)))
    r.put(("config", "currentAbsPath %s%s"%(os.path.abspath(opt_currentSnapshot), opt_snapshotEOL)))
    r.put(("config", "sourceType %s%s"%(opt_sourceType, opt_snapshotEOL)))
    r.put(("config", "followSymlinks %s%s"%(opt_followSymlink, opt_snapshotEOL)))
    r.put(("config", "explicitFiles %s%s"%(opt_explicitPaths, opt_snapshotEOL)))
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
    opt_sourceType = ""
    opt_explicitPaths =""
    opt_followSymlink = False
    opt_debug = False
    
    args = sys.argv[1:]
    it = iter(args)
    for i in it:
        if i.upper() =='-DEBUG':
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
	    if opt_sourceType == "Live":
                sys.stderr.write("You cann't specify both a snapshot and a live sources!")
		usage()
                sys.exit(-1)
            elif not os.path.isdir(opt_currentSnapshot):
                sys.stderr.write("Cannot find open snapshot source directory %s\n"%opt_currentSnapshot)
                sys.exit(-1)
            else:
                opt_sourceType = "SNAPSHOT"
            continue
        elif i == '-l' or i =='-L':
            opt_currentSnapshot = next(it)
            if opt_sourceType == "SNAPSHOT":
                sys.stderr.write("You can specify both a snapshot and a live sources!")
                sys.exit(-1)
            elif not os.path.isdir(opt_currentSnapshot):
                sys.stderr.write("Cannot find open live source directory %s\n"%opt_currentSnapshot)
                sys.exit(-1)
            else:
                opt_sourceType = "LIVE"
            continue
        elif i == '-s' or i =='-S':
            opt_followSymlink = True
            continue
        elif i.startswith('-j'):
            opt_threads = int(i[2:]) -1
            # we need to save 1 thread for processing the results queue
            continue

    # We'll need somewhere to output this lot
    filebase = opt_outputDirectory+opt_snapshotName+"/"+opt_snapshotTimestamp+"/"
    try:
        os.makedirs(filebase)
    except ValueError:
        sys.stderr.write("Cannot create output directory (error: %s)\n"%ValueError)
        sys.exit(-1)


    terminateThreads = False

    # initialise Queues
    pathQueue = Queue(MaxQueue)
    pathQueue.put(".")
    resultsQueue = Queue(opt_threads*MaxQueue)

    logConfig(resultsQueue)

    # setup the single results worker
    resultsWorker = Thread(target=outputResult, args=(i, resultsQueue))
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

    if opt_explicitPaths:
        try:
            for line in open(opt_explicitPaths):
                explicitFile = line.rstrip('\n').rstrip('\r')
                if explicitFile == os.path.abspath(explicitFile):
                    pathQueue.put(explicitFile)
                else:
                    resultsQueue.put(("error", "must be absolute path: %s%s"%(explicitFile, opt_snapshotEOL)))
        except ValueError:
            sys.stderr.write("Cannot read list of files to ingest from %s (error: %s)\n"%(opt_explicitPaths, ValueError))
            sys.exit(-1)

    try:

        # lets just hang back and wait for the queues to empty
        while not terminateThreads:
             time.sleep(.1)
             if pathQueue.empty():
                 pathQueue.join()
                 resultsQueue.join()
                 exit(1)
    except KeyboardInterrupt:
        # Time to tell all the threads to bail out
        terminateThreads = True
        raise
