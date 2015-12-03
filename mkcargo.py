#!/usr/bin/env python
#
#

import os, sys, hashlib, re, multiprocessing
from Queue import Queue
from threading import Thread
import time, datetime, errno
from filecmp import dircmp


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
    print "-f <file> - ingest based on a file listing file paths to procress in the cargo file."
    print "-l <dir>  - live mode, to be used when the source filesystem does not support snapshots."
    print "-s <dir>  - snapshot mode, used when the new source filesystem is a snapshot."
    print "-p <dir>  - previous snapshot, if omitted then a full ingest based on the file tree passed with"
    print "            either the -l or -s options."
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
        if (file == "cargo"):
            eolchar = opt_cargoEOL
        else:
	    eolchar = opt_snapshotEOL

        try:
            with open(filebase+file, "a") as outputFile:
                outputFile.write(message+eolchar)
        except:
            sys.stderr.write("Cannot write to %s\n"%file)
            sys.stderr.write("%s: %s\n"%file%message)
        q.task_done()


# pathWorker used by threads to process the new snapshot file tree
#
def processPath(i, q):
    while True:
        path = q.get()
        formatOutput(md5sum(path, md5blocklen),  path)
        q.task_done()

# for files that can't be processed by dircmp we have to expend some extra energy and use
# MD5 checksums
#
def compareFunnyFile(fileA, fileB):
    if not os.path.isfile(fileA):
        sys.stderr.write("%s is not a file!\n"%fileA)
        sys.exit(-1)
    elif not os.path.isfile(fileA):
        sys.stderr.write("%s is not a file!\n"%fileA)
        sys.exit(-1)
    else:
        hashA = md5sum(fileA)
        hashB = md5sum(fileB)
    return hashA == hashB;


def fullSnapshot():
    # Lets hoover up everything
    sys.stdout.write("Should really implement this!")
    sys.exit(1)



def incrSnapshot():
    if (opt_sourceLive):
	opt_source = opt_sourceLive
    else:
	opt_source = opt_sourceSnapshot

    deltaDir = dircmp(opt_previousSnapshot, opt_source)
    for dir in deltaDir.common_dirs:
	output(dir, "same")

    return;


if __name__ == '__main__':

    opt_snapshotEOL = "\n"
    opt_cargoEOL = "\0"
    opt_threads = multiprocessing.cpu_count()-1
    opt_snapshotName =""
    opt_filebase ="./"
    opt_snapshotTimestamp = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
    opt_previousSnapshot = ""
    opt_sourceSnapshot = ""
    opt_outputDirectory = ""
    opt_sourceLive = ""
    opt_allNew = True

    args = sys.argv[1:]
    it = iter(args)
    for i in it:
        if i == '-n' or i =='-N':
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
            else:
                opt_allNew = False
            continue
        elif i == '-p' or i =='-P':
            opt_previousSnapshot = next(it)
            if not os.path.isdir(opt_previousSnapshot):
                sys.stderr.write("Can not find open previous snapshot directory %s\n"%opt_previousSnapshot)
                sys.exit(-1)
            else:
                opt_allNew = False
            continue
        elif i == '-s' or i =='-S':
            opt_sourceSnapshot = next(it)
	    if opt_sourceLive:
                sys.stderr.write("You cann't specify both a snapshot and a live sources!")
		usage()
                sys.exit(-1)
            elif not os.path.isdir(opt_sourceSnapshot):
                sys.stderr.write("Cannot find open snapshot source directory %s\n"%opt_sourceSnapshot)
                sys.exit(-1)
            continue
        elif i == '-l' or i =='-L':
            opt_sourceLive = next(it)
            if opt_sourceSnapshot:
                sys.stderr.write("You can specify both a snapshot and a live sources!")
                sys.exit(-1)
            elif not os.path.isdir(opt_sourceLive):
                sys.stderr.write("Cannot find open live source directory %s\n"%opt_sourceLive)
                sys.exit(-1)
            continue
        elif i.startswith('-j'):
            opt_threads = int(i[2:]) -1
            # we need to save 1 thread for processing the results queue
            continue

    if (opt_sourceSnapshot and opt_sourceLive):
        sys.stderr.write("Nothing to do?\n")
        usage()
        sys.exit(0)

    # We'll need somewhere to output this lot
    filebase = opt_outputDirectory+opt_snapshotName+"/"+opt_snapshotTimestamp+"/"
    os.makedirs(filebase)

    # initialise Queues
    pathQueue = Queue(MaxQueue)
    resultsQueue = Queue(opt_threads*MaxQueue)

    # setup the single results worker
    resultsWorker = Thread(target=outputResult, args=(i, resultsQueue))
    resultsWorker.setDaemon(True)
    resultsWorker.start()

    # setup the pool of path workers
    for i in range(opt_threads):
        pathWorker = Thread(target=processPath, args=(i, pathQueue))
        pathWorker.setDaemon(True)
        pathWorker.start()

    # lets just hang back and wait for the queues to empty
    pathQueue.join()
    resultsQueue.join()
