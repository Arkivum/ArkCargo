#!/usr/bin/env python
#
# ArkCargo - mkcargo
#
# MIT License, 
# the code to claculate a file MD5 checksum is based on code from 
# https://github.com/joswr1ght/md5deep/blob/master/md5deep.py
# 
# @author Chris Pates <chris.pates@arkivum.com>
# @copyright Arkivum Limited 2015


import os, sys, hashlib, re, multiprocessing
from Queue import Queue, LifoQueue
from threading import Thread,current_thread
import time, datetime, errno
import filecmp
import csv
import argparse
import shutil

# To stop the queue from consuming all the RAM available
queueParams = {'max' : 10000, 'highWater' : 9000, 'lowWater' : 100}


validFiles = ["log", "failed", "added", "modified", "unchanged", "symlink", "directory", "config", "cargo", "removed", "queue.debug"]
preserveFiles = ["log", "failed", "config", "queue.debug"]
appendFiles = ["added", "modified", "unchanged", "symlink", "directory", "removed", "snapshot.csv", "ingest.csv", "cargo.csv"]
includeStats = {'snapshot' : ['added', 'modified', 'unchanged'], 'ingest' : ['added', 'modified'], 'cargo': []}
stats = {}
statsFields = {}

def toBytes(humanBytes):
    # convert human readable to '0's
    byteUnits = {'KB' : '000', 'MB' : '000000', 'GB' : '000000000', 'TB' :'000000000000', 'PB' : '000000000000000'}

    humanBytes = humanBytes.upper()
    humanBytes = humanBytes[:-2] + byteUnits.get(humanBytes[-2:])
    return int(humanBytes);

parser = argparse.ArgumentParser(description='Analysis a filesystem and create a cargo file to drive an ingest job.')

parser.add_argument('-s', dest='followSymlink', action='store_true', help='follow symlinks and ingest their target, defaults to recording symlinks and their targets in the symlink file.')

parser.add_argument('-j', metavar='threads', nargs='?', dest='threads', type=int, default=(multiprocessing.cpu_count()-1), help='Controls multi-threading. By default the program will create one thread per CPU core, one thread is used for writing to the output files, the rest for scanning the file system and calculating MD5 hashes. Multi-threading causes output filenames to be in non-deterministic order, as files that take longer to hash will be delayed while they are hashed.')

parser.add_argument('-n', nargs='?', metavar='name', dest='name', type=str, required=True, help='a meaningful name for the dataset, this should be consistent for all snapshot of a given dataset.')

parser.add_argument('-t', nargs='?', metavar='yyyymmddThhmmss', dest='timestamp', type=str, required=True, help='Where a filesystem snapshot is being processed then it is important to use the timestamp from the newer snapshot.')

parser.add_argument('-o', nargs='?', metavar='output directory', dest='output', type=str, default="output", help='the directory under which to write the output. <output directory>/<name>/<timestamp>/<output files>.')

parser.add_argument('--exists', dest='prepMode', choices=['clean', 'preserve', 'append'], default='preserve', help=argparse.SUPPRESS)
parser.add_argument('--clean', action='store_true', help='if output directory exists delete its contents before process, by default previous output is preserved.')

parser.add_argument('--debug', dest='debug', action='store_true', help=argparse.SUPPRESS)
parser.add_argument('-boundaries', dest='statsBoundaries', default=os.path.join(os.path.dirname( os.path.realpath( __file__ )), 'boundaries.csv'), type=str, help=argparse.SUPPRESS)
parser.add_argument('--stats', dest='cargo', action='store_false', help='calculate stats (does not generate a cargo file)')
parser.add_argument('-cargoEOL', dest='cargoEOL', default='\0', help=argparse.SUPPRESS)
parser.add_argument('-cargoMax', dest='cargoMax', default='5TB', help=argparse.SUPPRESS)
parser.add_argument('-cargoPad', dest='cargoPad', default=6, help=argparse.SUPPRESS)
parser.add_argument('-cargoExt', dest='cargoExt', default='.md5', help=argparse.SUPPRESS)
parser.add_argument('-lastRunPad', default=4, help=argparse.SUPPRESS)
parser.add_argument('-lastRunPrefix', default='run', help=argparse.SUPPRESS)
parser.add_argument('-defaultEOL', dest='snapshotEOL', default='\n', help=argparse.SUPPRESS)

parser.add_argument('--rework', metavar='file', nargs='+', help='a file containing paths for which a cargo file needs to be generated, generally based on a {failed} file from a previous run to be append to the stats and cargo')

group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--full', metavar='snapshots', dest='snapshots', nargs=1, default="", help='generate a cargo file for the current snapshot')
group.add_argument('--incr', metavar='snapshots', dest='snapshots', nargs=2, default="", help='generates a cargo file for the difference between the first snapshot and the second')
group.add_argument('--files', metavar='file', dest='file', nargs='+', default="", help='a file containing explicit paths for which a cargo file needs to be generated')


# convert human readable version of a size in Bytes
#
def toBytes(humanBytes):
    # convert human readable to '0's
    byteUnits = {'KB' : '000', 'MB' : '000000', 'GB' : '000000000', 'TB' :'000000000000', 'PB' : '000000000000000'}

    humanBytes = humanBytes.upper()
    humanBytes = humanBytes[:-2] + byteUnits.get(humanBytes[-2:])
    return int(humanBytes);

# log current config to a file
#
def logConfig(config, r):
    r.put(("config", "", "%s%s"%(config, args.snapshotEOL)))
    return;


# move some files to a new 'run' directory
# copy some others but delete nothing!
#
def prepOutput():
    prefix = "run"
    cargo_ext = ".md5"
    moveList = []
    copyList = []

    # lets figure out what to do if there already files in the output directory
    if args.rework:
        args.prepMode = 'append'
    if args.clean:
        args.prepMode = 'clean'

    try:
        root, dirlist, filelist = next(os.walk(args.filebase))

        if args.prepMode == 'clean':
            for file in filelist:
                os.remove(os.path.join(args.filebase, file))
            for dir in dirlist:
                shutil.rmtree(os.path.join(args.filebase, dir))
        else:
            # figure out how many times this has been run.
            numRuns = len(filter(lambda x: x.startswith(prefix), dirlist))
            lastRun = os.path.join(args.filebase, "%s%s"%(args.lastRunPrefix,str(numRuns+1).zfill(args.lastRunPad)))
            os.makedirs(lastRun)

            if args.prepMode == 'append':
                copyList  = list(set(filelist).intersection(appendFiles))
            else:
                moveList  = list(set(filelist).intersection(appendFiles))
            moveList += list(set(filelist).intersection(preserveFiles))
            moveList += filter(lambda x: x.endswith(args.cargoExt), filelist)

            for file in moveList:
                shutil.move(os.path.join(args.filebase, file), lastRun)
            for file in copyList:
                shutil.copy2(os.path.join(args.filebase, file), lastRun)
    except ValueError:
        sys.stderr.write("Can't archive previous run: %s)\n"%(file, ValueError))
        sys.exit(-1)
    return;


def loadBoundaries(file):
    boundaries = []
    statsFields = []
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

    statsFields = ['Category'] + bytesfields.keys() + countfields.keys()

    return boundaries, statsFields;

def prepStats():
    boundaries = []
    fields =[]

    # load stats boundaries
    boundaries, fields = loadBoundaries( args.statsBoundaries )

    if args.rework:
        stats = loadStats(fields)
    else:
        stats = initStats(fields)
    return boundaries, fields, stats;


def loadStats(fields):
    cargoCount = 0

    # imports stats and boundaries, only relevant for 'rework' mode
    try:
        # First load in the stats for the snapshot (so far)
        file = os.path.join(args.filebase,'snapshot.csv')
        with open(file) as csvfile:
            statsBoundaries = csv.DictReader(csvfile)
            for row in statsBoundaries:
                stats[row['Category']] = {}
                for field in fields:
                    if field == 'Category':
                        stats[row['Category']][field] = row[field]
                    else:
                        stats[row['Category']][field] = int(row[field])

        # First load in the stats for the cargo files (so far)
        cargoCount += 1

        file = os.path.join(args.filebase,'cargo.csv')
        with open(file) as csvfile:
            statsBoundaries = csv.DictReader(csvfile)
            # skip the header row, as this was created above
            next(statsBoundaries)
            for row in statsBoundaries:
                stats[row['Category']] = {}
                cargoCount += 1
                for field in fields:
                    if field == 'Category':
                        includeStats['cargo'].append(row[field])
                        stats[row['Category']][field] = row[field]
                    else:
                        stats[row['Category']][field] = int(row[field])
        # [TO DO]

        # even if we are reworking failed files, we'll start with a new cargo file
        stats['cargo'] = {}
        stats['cargo']['Vol'] = 0
        stats['cargo']['Num'] = cargoCount

    except ValueError:
        sys.stderr.write("Can't import stats to file %s (error: %s)\n"%(file, ValueError))
        exit(-1)

    return stats;

def initStats(fields):
    # imports boundaries and initialise stats

    for category in includeStats['snapshot']:
        stats[category] = {}
        for field in fields:
            if field == 'Category':
                stats[category]['Category'] = category
            else:   
                stats[category][field] = 0
    stats['cargo'] = {}
    stats['cargo']['Vol'] = 0
    stats['cargo']['Num'] = 0

    return stats;


def updateStats(file, size):
    newFile = False
    global stats 
    global includeStats
    filePath = ""
    bytes = int(size)

    if file == "cargo":
        if (stats[file]['Vol'] + bytes) > args.cargoMaxBytes:
            stats[file]['Num'] += 1 
            stats[file]['Vol'] = bytes
            newFile = True
        else:
            stats[file]['Vol'] += bytes

        filename = args.name + "-" +  args.timestamp + "-" + str(stats[file]['Num']).zfill(args.cargoPad) + args.cargoExt
        file = file + str(stats[file]['Num']).zfill(args.cargoPad)
        if file not in includeStats['cargo']:
            #add cargo file to list
            includeStats['cargo'].append(file)

            #initialise new stats line
            stats[file] = {}
            for field in statsFields:
                if field == 'Category':
                    stats[file]['Category'] = file
                else:  
                    stats[file][field] = 0
            filePath = os.path.join(args.filebase, filename)

    else:
        filePath = os.path.join(args.filebase, file)

    for boundary in statsBoundaries:
        name, lower, upper = boundary
        if (bytes >= int(lower) and upper == '') or (bytes >= int(lower) and bytes < int(upper)):
            stats[file]['count '+name] += 1
            stats[file]['bytes '+name] += bytes

    return newFile, filePath;


def exportStats():
    # exports stats based on categories listed in includeStats & includeStats 
    try:
        for statSet in includeStats:
            file = os.path.join(args.filebase,statSet+'.csv')
            with open(file, "wb") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=statsFields)
                #writer.writeheader()
                fields = {}
                for field in statsFields:
                    fields[field] = field
                writer.writerow(fields)
                for category in includeStats[statSet]:
                    writer.writerow(stats[category])
    except ValueError:
        sys.stderr.write("Can't export stats to file %s (error: %s)\n"%(filefull, ValueError))
    return;


# outputWorker used by threads to process output to the various output files. The must never be more
# than of this type of thread running.
#
def outputResult(i, files, q):
    otherFiles = ['cargo']
    while True:
        changeFile = False
        filePath = ""
        file, bytes, message = q.get()

        if file not in validFiles:
            sys.stderr.write("invalid output file'%s'"%file)
            sys.exit(-1)

        # update stats for this file
        if file in list( set(otherFiles) | set(includeStats['snapshot']) | set(includeStats['ingest']) ):
            changeFile, filePath = updateStats(file, bytes)
        else:
            filePath = os.path.join(args.filebase,file)

        if changeFile and (file in files):
           try:
               (files[file]).close()
               files[file] = open(filePath, "a", 0)
           except ValueError:
                sys.stderr.write("can't open %s"%file)
                sys.exit(-1)

        if not file in files:
            try:
                files[file] = open(filePath, "a", 0)
            except ValueError:
                sys.stderr.write("can't open %s"%file)
                sys.exit(-1)


        # write out the message to the end of the file
        try:
           files[file].write(message)
        except:
            sys.stderr.write("Cannot write to %s\n"%file)
            sys.stderr.write("%s: %s\n"%(file, message))
            exit(-1)
        q.task_done()


# Calculate the MD5 sum of the file
#
def cargoEntry(path, queue):
    if args.cargo:        
        hash = hashlib.md5()
        blocksize=65536

        try:
            if args.file:
                absPath = path
            else:
                absPath = os.path.abspath(os.path.join(args.snapshotCurrent, path))

            # the following means of calculating an MD5 checksum for a file
            # is based on code in https://github.com/joswr1ght/md5deep/blob/master/md5deep.py
            # also under MIT license.
            with open(absPath, "rb") as f:
                for block in iter(lambda: f.read(blocksize), ""):
                    hash.update(block)

            hash = hash.hexdigest().replace(" ","")
            queue.put(("cargo", os.path.getsize(absPath), "%s  %s%s"%(hash, absPath, args.cargoEOL)))

        except IOError as e:
            queue.put(("log", "", "%s %s%s"%(e, absPath, args.snapshotEOL)))
            queue.put(("failed", "", "%s%s"%(path, args.snapshotEOL)))
            pass
    return;


# pathWorker used by threads to process a Full snapshot of the file tree
#
def snapshotFull(i, f, d, r):
    lowWater = queueParams['lowWater']
   
    while not terminateThreads:
        if f.qsize() > lowWater:
            fileFull(f, r)
        else:
            if not d.empty():
                dirFull(d, f, r)
            elif f.empty():
	        time.sleep(1)
            else:
                fileFull(f, r)
    return;


# process a single file path 
# 
def fileFull(fileQueue, outQueue):
    relPath = fileQueue.get()
    absPath = os.path.abspath(os.path.join(args.snapshotCurrent, relPath))

    if not os.access(absPath, os.R_OK):
        outQueue.put(("log", "", "Permission Denied; %s%s"%(absPath, args.snapshotEOL)))
        outQueue.put(("failed", "", "%s%s"%(relPath, args.snapshotEOL)))
    else:
        if (not args.followSymlink) and os.path.islink(absPath):
            outQueue.put(("symlink", "", "%s %s%s"%(relPath, os.path.realpath(absPath), args.snapshotEOL)))
        else:
            outQueue.put(("added", os.path.getsize(absPath), "%s%s"%(relPath, args.snapshotEOL)))
            cargoEntry(relPath, outQueue)
    fileQueue.task_done()
    return;

# process a single directory
#
def dirFull(dirQueue, fileQueue, outQueue):
    relPath = dirQueue.get()
    absPath = os.path.abspath(os.path.join(args.snapshotCurrent, relPath))

    if not os.access(absPath, os.R_OK):
        outQueue.put(("log", "", "Permission Denied; %s%s"%(absPath, args.snapshotEOL)))
        outQueue.put(("failed", "", "%s%s"%(relPath, args.snapshotEOL)))
    else:
        directory_name, dirs, files = next(os.walk(absPath))

        if len(dirs) > 0:
            for childPath in dirs:
                dirQueue.put(os.path.join(relPath, childPath))
        else:
            # must be leaf node lets record it
            outQueue.put(("directory", "", "%s%s"%(relPath, args.snapshotEOL)))

        for childPath in files:
            fileQueue.put(os.path.join(relPath, childPath))
    dirQueue.task_done()
    return;


# pathWorker used by threads to process a Full snapshot of the file tree
#
def snapshotIncr(i, f, d, r):
    lowWater = queueParams['lowWater']
  
    while not terminateThreads:
        if f.qsize() > lowWater:
            fileIncr(f, r)
        else:
            if not d.empty():
                dirIncr(d, f, r)
            elif f.empty():
                time.sleep(1)
            else:
                fileIncr(f, r)
    return;


# process a single file path
#
def fileIncr(fileQueue, outQueue):
    relPath = fileQueue.get()
    absPath = os.path.abspath(os.path.join(args.snapshotCurrent, relPath))
    oldPath = os.path.abspath(os.path.join(args.snapshotPrevious, relPath))

    if not os.access(absPath, os.R_OK):
        outQueue.put(("log", "", "Permission Denied; %s%s"%(absPath, args.snapshotEOL)))
        outQueue.put(("failed", "", "%s%s"%(relPath, args.snapshotEOL)))
    elif (not args.followSymlink) and os.path.islink(absPath):
        outQueue.put(("symlink", "", "%s %s%s"%(relPath, os.path.realpath(absPath), args.snapshotEOL)))
    else:
        if os.path.isfile(oldPath):
            if not os.access(oldPath, os.R_OK):
                outQueue.put(("log", "", "Permission Denied; %s%s"%(oldPath, args.snapshotEOL)))
                outQueue.put(("failed", "", "%s%s"%(relPath, args.snapshotEOL)))
            elif filecmp.cmp(oldPath, absPath):
                outQueue.put(("unchanged", os.path.getsize(absPath), "%s%s"%(relPath, args.snapshotEOL)))
            else:
                outQueue.put(("modified", os.path.getsize(absPath), "%s%s"%(relPath, args.snapshotEOL)))
                cargoEntry(relPath, outQueue)
        else:    
            outQueue.put(("added", os.path.getsize(absPath), "%s%s"%(relPath, args.snapshotEOL)))
            cargoEntry(relPath, outQueue)
    fileQueue.task_done()
    return;


# process a single directory
#
def dirIncr(dirQueue, fileQueue, outQueue):
    relPath = dirQueue.get()
    absPath = os.path.abspath(os.path.join(args.snapshotCurrent, relPath))
    oldPath = os.path.abspath(os.path.join(args.snapshotPrevious, relPath))

    if not os.access(absPath, os.R_OK):
        outQueue.put(("log", "", "Permission Denied; %s%s"%(absPath, args.snapshotEOL)))
        outQueue.put(("failed", "", "%s%s"%(relPath, args.snapshotEOL)))
    elif not os.access(oldPath, os.R_OK):
        outQueue.put(("log", "", "Permission Denied; %s%s"%(oldPath, args.snapshotEOL)))
        outQueue.put(("failed", "", "%s%s"%(relPath, args.snapshotEOL)))
    else:    
        if os.path.isdir(oldPath):
            newDir_name, newDirs, newFiles = next(os.walk(absPath))
            oldDir_name, oldDirs, oldFiles = next(os.walk(oldPath))
            
            for removed in (set(oldDirs).difference(newDirs) | set(oldFiles).difference(newFiles)):
                outQueue.put(("removed", os.path.getsize(oldPath), "%s%s"%(os.path.join(relPath, removed), args.snapshotEOL)))

            if len(newDirs) > 0:
                for childPath in newDirs:
                    dirQueue.put(os.path.join(relPath, childPath))
            else:
                # must be leaf node lets record it
                outQueue.put(("directory", "", "%s%s"%(relPath, args.snapshotEOL)))

            for childPath in newFiles:
                fileQueue.put(os.path.join(relPath, childPath))
        else:
            dirFull(d.get(), d, f, r)
    dirQueue.task_done()
    return;


# pathWorker used by threads to process a Full snapshot of the file tree
#
def snapshotExplicit(i, f, d, r):
    lowWater = queueParams['lowWater']

    while not terminateThreads:
        if f.qsize() > lowWater:
            fileExplicit(f, r)
        else:
            if not d.empty():
                dirExplicit(d, f, r)
            elif f.empty():
                time.sleep(1)
            else:
                fileExplicit(f, r)
    return;


# process a single file path
#
def fileExplicit(fileQueue, outQueue):
    absPath = fileQueue.get()

    if not os.path.exists(absPath):
        outQueue.put(("log", "", "Does not exist; %s%s"%(absPath, args.snapshotEOL)))
        outQueue.put(("failed", "", "%s%s"%(absPath, args.snapshotEOL)))
    elif not os.access(absPath, os.R_OK):
        outQueue.put(("log", "", "Permission Denied; %s%s"%(absPath, args.snapshotEOL)))
        outQueue.put(("failed", "", "%s%s"%(absPath, args.snapshotEOL)))
    else:
        if (not args.followSymlink) and os.path.islink(absPath):
            outQueue.put(("symlink", "", "%s %s%s"%(absPath, os.path.realpath(absPath), args.snapshotEOL)))
        else:
            outQueue.put(("added", os.path.getsize(absPath), "%s%s"%(absPath, args.snapshotEOL)))
            cargoEntry(absPath, outQueue)
    fileQueue.task_done()
    return;


# process a single directory
#
def dirExplicit(dirQueue, fileQueue, outQueue):
    absPath = dirQueue.get()

    if not os.path.exists(absPath):
        outQueue.put(("log", "", "Does not exist; %s%s"%(absPath, args.snapshotEOL)))
        outQueue.put(("failed", "", "%s%s"%(absPath, args.snapshotEOL)))
    elif not os.access(absPath, os.R_OK):
        outQueue.put(("log", "", "Permission Denied; %s%s"%(absPath, args.snapshotEOL)))
        outQueue.put(("failed", "", "%s%s"%(absPath, args.snapshotEOL)))
    else:
        directory_name, dirs, files = next(os.walk(absPath))

        if len(dirs) > 0:
            for childPath in dirs:
                dirQueue.put(os.path.join(absPath, childPath))
        else:
            # must be leaf node lets record it
            outQueue.put(("directory", "", "%s%s"%(absPath, args.snapshotEOL)))

        for childPath in files:
            fileQueue.put(os.path.join(absPath, childPath))
    dirQueue.task_done()
    return;


# prime Queues with paths to be processed
#
def primeQueues(fileQueue, dirQueue, outQueue):

    fileList =[]

    if args.rework:
        fileList += args.rework
    if args.file:
        fileList += args.file
    if len(fileList):
        try:
            for pathFile in fileList:
                for line in open(pathFile):
                    relPath = line.rstrip('\n').rstrip('\r')
                    if args.file:
                        absPath = relPath
                    else:
                        absPath = os.path.join(args.snapshotCurrent, relPath)
                    if os.path.isdir(absPath):
                        dirQueue.put(relPath)
                    elif os.path.isfile(absPath):
                        fileQueue.put(relPath)
                    else:
                        outQueue.put(("failed", "", "%s%s"%(relPath, args.snapshotEOL)))
                        outQueue.put(("log", "", "invalid path: %s%s"%(relPath, args.snapshotEOL)))
        except ValueError:
            # Time to tell all the threads to bail out
            terminateThreads = True
            sys.stderr.write("Cannot read list of files to ingest from %s (error: %s)\n"%(args.rework, ValueError))
            sys.exit(-1)
    else:
        dirQueue.put(".")
    return;


if __name__ == '__main__':
    global args
    args = parser.parse_args()
    args.cargoMaxBytes = toBytes(args.cargoMax)

    try:
        if len(args.snapshots) == 0:
            args.mode = 'explicit'
        elif len(args.snapshots) == 1:
            args.snapshotCurrent = args.snapshots[0]
            args.mode = 'full'
        else:
            args.snapshotPrevious, args.snapshotCurrent = args.snapshots
            args.mode = 'incr'

        args.filebase = os.path.join(args.output, args.name, args.timestamp)
        prepOutput()

        statsBoundaries, statsFields, stats = prepStats()
    except ValueError:
        # Time to tell all the threads to bail out
        terminateThreads = True
        sys.stderr.write("Cannot create output directory (error: %s)\n"%ValueError)
        sys.exit(-1)


    terminateThreads = False

    # initialise Queues
    fileQueue = Queue(queueParams['max'])
    dirQueue = LifoQueue(queueParams['max'])
    resultsQueue = Queue(args.threads*queueParams['max'])

    logConfig(args, resultsQueue)

    fileHandles = {}

    try:
        # setup the single results worker
        resultsWorker = Thread(target=outputResult, args=(1, fileHandles, resultsQueue))
        resultsWorker.setDaemon(True)
        resultsWorker.start()
        
        # setup the pool of path workers
        for i in range(args.threads):
            if args.mode == 'full':
                pathWorker = Thread(target=snapshotFull, args=(i, fileQueue, dirQueue, resultsQueue))
            elif args.mode == 'incr':
                pathWorker = Thread(target=snapshotIncr, args=(i, fileQueue, dirQueue, resultsQueue))
            elif args.mode == 'explicit':
                pathWorker = Thread(target=snapshotExplicit, args=(i, fileQueue, dirQueue, resultsQueue))
            else:
                terminateThreads = True
                sys.stderr.write("unrecognised mode! abandon excution.\n")
                sys.exit(-1)

            pathWorker.setDaemon(True)
            pathWorker.start()

        # now the worker threads are processing lets feed the fileQueue, this will block if the 
        # rework file is larger than the queue.
        primeQueues(fileQueue, dirQueue, resultsQueue)


        if args.debug:
            resultsQueue.put(("queue.debug", "", "\"max\", \"file\", \"dir\", \"results\"\n"))
        # lets just hang back and wait for the queues to empty
        while not terminateThreads:
            if args.debug:
                resultsQueue.put(("queue.debug", "", "%s, %s, %s, %s\n"%(queueParams['max'], fileQueue.qsize(), dirQueue.qsize(), resultsQueue.qsize())))
            time.sleep(.1)
            if fileQueue.empty() and dirQueue.empty():
                fileQueue.join()
                resultsQueue.join()
                exportStats()
                for file in fileHandles:
                    fileHandles[file].close()
                exit(1)
    except KeyboardInterrupt:
        # Time to tell all the threads to bail out
        terminateThreads = True
        raise
