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
moveFiles = ["log", "failed", "config", "queue.debug"]
copyFiles = ["added", "modified", "unchanged", "symlink", "directory", "cargo", "removed", "stats_full.csv", "stats_incr.csv"]
includeStats = {'full' : ['added', 'modified', 'unchanged'], 'incr' : ['added', 'modified']}
stats = {}
statsFields = {}

parser = argparse.ArgumentParser(description='Analysis a filesystem and create a cargo file to drive an ingest job.')

parser.add_argument('-s', dest='followSymlink', action='store_true', help='follow symlinks and ingest their target, defaults to recording symlinks and their targets in the symlink file.')

parser.add_argument('-j', metavar='threads', nargs='?', dest='threads', type=int, default=(multiprocessing.cpu_count()-1), help='Controls multi-threading. By default the program will create one thread per CPU core, one thread is used for writing to the output files, the rest for scanning the file system and calculating MD5 hashes. Multi-threading causes output filenames to be in non-deterministic order, as files that take longer to hash will be delayed while they are hashed.')

parser.add_argument('-n', nargs='?', metavar='name', dest='name', type=str, required=True, help='a meaningful name for the dataset, this should be consistent for all snapshot of a given dataset.')

parser.add_argument('-t', nargs='?', metavar='yyyymmddThhmmss', dest='timestamp', type=str, required=True, help='Where a filesystem snapshot is being processed then it is important to use the timestamp from the newer snapshot.')

parser.add_argument('-o', nargs='?', metavar='output directory', dest='output', type=str, default="output", help='the directory under which to write the output. <output directory>/<name>/<timestamp>/<output files>.')

parser.add_argument('--debug', dest='debug', action='store_true', help=argparse.SUPPRESS)
parser.add_argument('-boundaries', dest='statsBoundaries', default=os.path.join(os.path.dirname( os.path.realpath( __file__ )), 'boundaries.csv'), type=str, help=argparse.SUPPRESS)
parser.add_argument('--nocargo', dest='cargo', action='store_false', help=argparse.SUPPRESS)
parser.add_argument('-cargoEOL', dest='cargoEOL', default='\0', help=argparse.SUPPRESS)
parser.add_argument('-defaultEOL', dest='snapshotEOL', default='\n', help=argparse.SUPPRESS)

parser.add_argument('--rework', metavar='file', nargs='+', help='a file containing paths for which a cargo file needs to be generated')
parser.add_argument('--stats', dest='stats', action='store_false', help='calculate stats (does not generate a cargo file)')

group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--full', metavar='snapshots', dest='snapshots', nargs=1, help='generate a cargo file for the current snapshot')
group.add_argument('--incr', metavar='snapshots', dest='snapshots', nargs=2, help='generates a cargo file for the difference between the first snapshot and the second')



def logConfig(config, r):
    r.put(("config", "", "%s%s"%(config, args.snapshotEOL)))
    return;

# incase this job has previously been run lets move/copy things out of the way!
#
def prepOutput():
    prefix = "run"

    try:
        if os.path.isdir(args.filebase):
            root, dirlist, filelist = next(os.walk(args.filebase))

            # figure out how many times this has been run.
            numRuns = len(filter(lambda x: x.startswith(prefix), dirlist))
            lastRun = os.path.join(args.filebase, "run%s"%str(numRuns+1).zfill(4))
            os.makedirs(lastRun)
  
            # if we are reworking then appending to the various metadata files makes sense otherwise
            # move them out of the way and we need to rerun from scratch.
            if args.rework:
                for file in set(filelist).intersection(copyFiles):
                    shutil.copy2(os.path.join(args.filebase, file), lastRun)
            else:
                for file in set(filelist).intersection(copyFiles):
                    shutil.move(os.path.join(args.filebase, file), lastRun)
     
            for file in set(filelist).intersection(moveFiles):
                shutil.move(os.path.join(args.filebase, file), lastRun)
        else:
            os.makedirs(args.filebase)

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

    if os.path.isfile(os.path.join(args.filebase, "stats_full.csv")):
        stats = loadStats(fields)
    else:
        stats = initStats(fields)
    return boundaries, fields, stats;


def loadStats(fields):
    # imports stats and boundaries, only relevant for 'rework' mode
    try:
        file = os.path.join(args.filebase,'stats_full.csv')
        with open(file) as csvfile:
            statsBoundaries = csv.DictReader(csvfile)
            for row in statsBoundaries:
                stats[row['Category']] = {}
                for field in fields:
                    if field == 'Category':
                        stats[row['Category']][field] = row[field]
                    else:
                        stats[row['Category']][field] = int(row[field])
    except ValueError:
        sys.stderr.write("Can't import stats to file %s (error: %s)\n"%(file, ValueError))
        exit(-1)

    return stats;

def initStats(fields):
    # imports boundaries and initialise stats

    for category in includeStats['full']:
        stats[category] = {}
        for field in fields:
            if field == 'Category':
                stats[category]['Category'] = category
            else:   
                stats[category][field] = 0
    return stats;


def updateStats(file, bytes):
    global stats 

    for boundary in statsBoundaries:
        name, lower, upper = boundary
        if (bytes >= int(lower) and upper == '') or (bytes >= int(lower) and bytes < int(upper)):
            stats[file]['count '+name] += 1
            stats[file]['bytes '+name] += bytes


def exportStats():
    # exports stats based on categories listed in includeStats & includeStats 
    try:
        for statSet in includeStats:
            file = os.path.join(args.filebase,'stats_'+statSet+'.csv')
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
    while True:
        file, bytes, message = q.get()

        # update stats for this file
        if file in list( set(includeStats['full']) | set(includeStats['incr']) ):
            updateStats(file, bytes)

        if file not in validFiles:
            sys.stderr.write("invalid output file'%s'"%file)
            sys.exit(-1)
        elif not file in files:
            try:
                files[file] = open(os.path.join(args.filebase,file), "a", 0)
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
            #path = path.replace("\r","")
            #path = path.replace("\n","")
            absPath = os.path.abspath(os.path.join(args.snapshotCurrent, path))

            # the following means of calculating an MD5 checksum for a file
            # is based on code in https://github.com/joswr1ght/md5deep/blob/master/md5deep.py
            # also under MIT license.
            with open(absPath, "rb") as f:
                for block in iter(lambda: f.read(blocksize), ""):
                    hash.update(block)

            hash = hash.hexdigest().replace(" ","")
            queue.put(("cargo", "", "%s  %s%s"%(hash, absPath, args.cargoEOL)))

        except IOError as e:
            queue.put(("log", "", "%s %s%s"%(e, absPath, args.snapshotEOL)))
            queue.put(("failed", "", "%s%s"%(path, args.snapshotEOL)))
            pass
    return;


# pathWorker used by threads to process a Full/Incr snapshot of the file tree
#
def snapshot(i, f, d, r):
    lowWater = queueParams['lowWater']
    
    while not terminateThreads:
        action = 'wait'

        if f.qsize() > lowWater:
            action = 'file'
        else:
            if not d.empty():
                action = 'directory'
            elif f.empty():
                action = 'wait'
	    else:
                action = 'file'

        if args.debug:
            r.put(("log", "", "%s - %s%s"%(current_thread().getName(), action, args.snapshotEOL)))

        if action == 'directory':
            if args.mode == 'incr':
                dirIncr(d.get(), d, f, r)
            else:
                dirFull(d.get(), d, f, r)
            d.task_done()
        elif action == 'file':
            if args.mode == 'incr':
                fileIncr(f.get(), r)
            else:
                fileFull(f.get(), r)
            f.task_done()
        else:
	    time.sleep(1)
    return;

# process a single file path 
# 
def fileFull(relPath, outQueue):
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
    return;


# process a single file path
#
def fileIncr(relPath, outQueue):
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
    return;


# process a single directory
#
def dirFull(relPath, dirQueue, fileQueue, outQueue):
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
    return;


# process a single directory
#
def dirIncr(relPath, dirQueue, fileQueue, outQueue):
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
                outQueue.put(("removed", "", "%s%s"%(os.path.join(relPath, removed), args.snapshotEOL)))

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
    return;


if __name__ == '__main__':
    global args
    args = parser.parse_args()

    try:
        if len(args.snapshots) == 1:
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
        
        args.cargo = getattr(args, 'stats')

        # setup the pool of path workers
        for i in range(args.threads):
            pathWorker = Thread(target=snapshot, args=(i, fileQueue, dirQueue, resultsQueue))

            pathWorker.setDaemon(True)
            pathWorker.start()

        # now the worker threads are processing lets feed the fileQueue, this will block if the 
        # rework file is larger than the queue.
        if args.rework:
            try:
                for reworkFile in args.rework:
                    for line in open(reworkFile):
                        reworkPath = line.rstrip('\n').rstrip('\r')
                        if os.path.isdir(reworkPath):
                            dirQueue.put(reworkPath)
                        else:
                            fileQueue.put(reworkPath)
            except ValueError:
                # Time to tell all the threads to bail out
                terminateThreads = True
                sys.stderr.write("Cannot read list of files to ingest from %s (error: %s)\n"%(args.rework, ValueError))
                sys.exit(-1)
        else:
            dirQueue.put(".")


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
