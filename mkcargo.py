#!/usr/bin/env python
#
# ArkCargo - mkcargo
#
# This file is licensed under the Affero General Public License version 3 or
# later. See the COPYING file.
#
# @author Chris Pates <chris.pates@arkivum.com>
# @copyright Arkivum Limited 2015


import os, sys, hashlib, re, multiprocessing
from Queue import Queue
from threading import Thread,current_thread
import time, datetime, errno
import filecmp
import csv
import argparse

# To stop the queue from consuming all the RAM available
MaxQueue = 1000
validModes = ["SNAPSHOT", "LIVE", "STATS", "REWORK"]
cargoModes = ["SNAPSHOT", "REWORK"]
validFiles = ["log", "failed", "added", "modified", "unchanged", "symlink", "directory", "config", "cargo", "removed"]
includeStats = {'full' : ['added', 'modified', 'unchanged'], 'incr' : ['added', 'modified']}
stats = {}
statsFields = {}

parser = argparse.ArgumentParser(description='Analysis a filesystem and create a cargo file to drive an ingest job.')

parser.add_argument('-s', dest='followSymlink', action='store_true', help='follow symlinks and ingest their target, defaults to recording symlinks and their targets in the symlink file.')

parser.add_argument('-j', metavar='threads', nargs='?', dest='threads', type=int, default=(multiprocessing.cpu_count()-1), help='Controls multi-threading. By default the program will create one thread per CPU core, one thread is used for writing to the output files, the rest for scanning the file system and calculating MD5 hashes. Multi-threading causes output filenames to be in non-deterministic order, as files that take longer to hash will be delayed while they are hashed.')

parser.add_argument('-n', nargs='?', metavar='name', dest='name', type=str, required=True, help='a meaningful name for the dataset, this should be consistent for all snapshot of a given dataset.')

parser.add_argument('-t', nargs='?', metavar='yyyymmddThhmmss', dest='timestamp', type=str, default=datetime.datetime.now().strftime("%Y%m%dT%H%M%S"), help='if not supplied the timestamp will be generated at the start of a run. Where a filesystem snapshot is being processed then it is more meaningful to use the timestamp from the newer snapshot.')

parser.add_argument('-o', nargs='?', metavar='output directory', dest='output', type=str, default="output", help='the directory under which to write the output. <output directory>/<name>/<timestamp>/<output files>.')

parser.add_argument('--debug', dest='debug', action='store_true', help=argparse.SUPPRESS)
parser.add_argument('-boundaries', dest='statsBoundaries', default=os.path.join(os.path.dirname( os.path.realpath( __file__ )), 'boundaries.csv'), type=str, help=argparse.SUPPRESS)
parser.add_argument('--nocargo', dest='cargo', action='store_true', help=argparse.SUPPRESS)
parser.add_argument('-cargoEOL', dest='cargoEOL', default='\0', help=argparse.SUPPRESS)
parser.add_argument('-defaultEOL', dest='snapshotEOL', default='\n', help=argparse.SUPPRESS)

group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--rework', metavar='file', nargs='+', help='a file containing absolute paths for which a cargo file needs to be generated')
group.add_argument('--full', metavar='snapshot', nargs=1, help='generate a cargo file for the current snapshot')
group.add_argument('--incr', metavar='snapshot', nargs=2, help='generates a cargo file for the difference between the first snapshot and the second')


def logConfig(config, r):
    r.put(("config", "", "%s%s"%(config, args.snapshotEOL)))
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
            file = os.path.join(args.filebase,'stats_'+statSet+'.csv')
            with open(file, "wb") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=statsFields)
                writer.writeheader()
                for category in includeStats[statSet]:
                    writer.writerow(stats[category])
    except ValueError:
        sys.stderr.write("Can't export stats to file %s (error: %s)\n"%(filefull, ValueError))
        print stats
 

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
                files[file] = open(os.path.join(args.filebase,file), "a")
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
            path = path.replace("\r","")
            path = path.replace("\n","")

            with open(path, "rb") as f:
                for block in iter(lambda: f.read(blocksize), ""):
                    hash.update(block)

            hash = hash.hexdigest().replace(" ","")
            queue.put(("cargo", "", "%s  %s%s"%(hash, path, args.cargoEOL)))

        except IOError as e:
            queue.put(("failed", "", "%s %s%s"%(e, path, args.snapshotEOL)))

    return;


# pathWorker used by threads to process an incremental snapshot of the file tree
#
def processIncr(i, q, r):
    while not terminateThreads:
        relPath = q.get()
        
        previousSnapshot, currentSnapshot = args.incr

        # This is an entry for the current snapshot
        absPath = os.path.abspath(os.path.join(currentSnapshot, relPath))
        oldPath = os.path.abspath(os.path.join(previousSnapshot, relPath))

        if args.debug:
            r.put(("log", "", "Thread %s - %s%s"%(current_thread().getName(), relPath, args.snapshotEOL)))

        # What have we picked up from the queue
        if os.path.isdir(absPath):
            # output only leaf nodes
            if len(next(os.walk(absPath))[1]) ==0:
                r.put(("directory", "", "%s%s"%(relPath, args.snapshotEOL)))

            dirlistOld = os.listdir(oldPath)
            dirlistNew = os.listdir(absPath)

            if os.path.isdir(oldPath):
                for removed in set(dirlistOld).difference(dirlistNew):
                    r.put(("removed", "", "%s%s"%(os.path.join(relPath, removed), args.snapshotEOL)))
                for common in set(dirlistNew).intersection(dirlistOld):
                    q.put(os.path.join(relPath, common))
                for added in set(dirlistNew).difference(dirlistOld):
                    q.put(os.path.join(relPath, added))
            else:
                for childPath in os.listdir(absPath):
                    q.put(os.path.join(relPath, childPath))

        elif (not args.followSymlink) and os.path.islink(absPath):
            if os.path.realpath(oldPath) == os.path.realpath(absPath):
                r.put(("unchanged", os.path.getsize(absPath), "%s%s"%(relPath, args.snapshotEOL)))
            else:
                r.put(("symlink", "", "%s %s%s"%(relPath, os.path.realpath(absPath), args.snapshotEOL)))

        elif os.path.isfile(absPath):
            if os.path.isfile(oldPath):
                if filecmp.cmp(oldPath, absPath):
                    r.put(("unchanged", os.path.getsize(absPath), "%s%s"%(relPath, args.snapshotEOL)))
                else:
                    r.put(("modified", os.path.getsize(absPath), "%s%s"%(relPath, args.snapshotEOL)))
                    cargoEntry(absPath, r)
            else:    
                r.put(("added", os.path.getsize(absPath), "%s%s"%(relPath, args.snapshotEOL)))
                cargoEntry(absPath, r)
            
        else:
            r.put(("failed", "", "invalid path: %s%s"%(relPath, args.snapshotEOL)))
        q.task_done()


# pathWorker used by threads to process a Full snapshot of the file tree
#
def processFull(i, q, r):
    while not terminateThreads:
        relPath = q.get()

        if args.rework:
            # This is an explict entry from the rework file
            absPath = relPath
        else:
            # This is an entry for the current snapshot
            snapshot = args.full[0]
            absPath = os.path.abspath(os.path.join(snapshot, relPath))

        if args.debug:
            r.put(("log", "", "Thread %s - %s%s"%(current_thread().getName(), relPath, args.snapshotEOL)))

        # What have we picked up from the queue
        if os.path.isdir(absPath):
            for childPath in os.listdir(absPath):
		q.put(os.path.join(relPath, childPath))

            # output only leaf nodes
            if len(next(os.walk(absPath))[1]) ==0:
                r.put(("directory", "", "%s%s"%(relPath, args.snapshotEOL)))

        elif (not args.followSymlink) and os.path.islink(absPath):
            r.put(("symlink", "", "%s %s%s"%(relPath, os.path.realpath(absPath), args.snapshotEOL)))

        elif os.path.isfile(absPath):
            if not (args.followSymlink and os.path.islink(absPath)):
                r.put(("added", os.path.getsize(absPath), "%s%s"%(relPath, args.snapshotEOL)))
                cargoEntry(absPath, r)
        else:
            r.put(("failed", "", "invalid path: %s%s"%(relPath, args.snapshotEOL)))
        q.task_done()

# pathWorker used by threads to process a rework file
#
def processRework(i, q, r):
    while not terminateThreads:
        relPath = q.get()

        # This is an explict entry from the rework file
        absPath = relPath

        if args.debug:
            r.put(("log", "", "Thread %s - %s%s"%(current_thread().getName(), relPath, args.snapshotEOL)))

        # What have we picked up from the queue
        if (not args.followSymlink) and os.path.islink(absPath):
            r.put(("symlink", "", "%s %s%s"%(relPath, os.path.realpath(absPath), args.snapshotEOL)))

        elif os.path.isfile(absPath):
            if not (args.followSymlink and os.path.islink(absPath)):
                r.put(("added", os.path.getsize(absPath), "%s%s"%(relPath, args.snapshotEOL)))
                cargoEntry(absPath, r)
        else:
            r.put(("failed", "", "invalid path: %s%s"%(relPath, args.snapshotEOL)))
        q.task_done()


if __name__ == '__main__':
    args = parser.parse_args()

    try:
        args.filebase = os.path.join(args.output, args.name, args.timestamp)
        os.makedirs(args.filebase)
    except ValueError:
        sys.stderr.write("Cannot create output directory (error: %s)\n"%ValueError)
        sys.exit(-1)

    # load stats boundaries
    statsBoundaries = loadBoundaries( args.statsBoundaries )

    terminateThreads = False

    # initialise Queues
    pathQueue = Queue(MaxQueue)
    resultsQueue = Queue(args.threads*MaxQueue)

    logConfig(args, resultsQueue)

    fileHandles = {}

    try:
        # setup the single results worker
        resultsWorker = Thread(target=outputResult, args=(1, fileHandles, resultsQueue))
        resultsWorker.setDaemon(True)
        resultsWorker.start()

        # setup the pool of path workers
        for i in range(args.threads):
            if args.incr:
                pathWorker = Thread(target=processIncr, args=(i, pathQueue, resultsQueue))
            elif args.full:
                pathWorker = Thread(target=processFull, args=(i, pathQueue, resultsQueue))
            elif args.rework:
                pathWorker = Thread(target=processRework, args=(i, pathQueue, resultsQueue))
            pathWorker.setDaemon(True)
            pathWorker.start()


        # now the worker threads are processing lets feed the pathQueue, this will block if the 
        # rework file is larger than the queue.
        if args.rework:
            try:
                for reworkFile in args.rework:
                    for line in open(reworkFile):
                        explicitFile = line.rstrip('\n').rstrip('\r')
                        if explicitFile == os.path.abspath(explicitFile):
                            pathQueue.put(explicitFile)
                        else:
                            resultsQueue.put(("failed", "", "must be absolute path: %s%s"%(explicitFile, args.snapshotEOL)))
            except ValueError:
                sys.stderr.write("Cannot read list of files to ingest from %s (error: %s)\n"%(args.rework, ValueError))
                sys.exit(-1)
        else:
            pathQueue.put(".")


        # lets just hang back and wait for the queues to empty
        while not terminateThreads:
             time.sleep(.1)
             if pathQueue.empty():
                 pathQueue.join()
                 resultsQueue.join()
                 exportStats()
                 for file in fileHandles:
                     fileHandles[file].close()
                 exit(1)
    except KeyboardInterrupt:
        # Time to tell all the threads to bail out
        terminateThreads = True
        raise
