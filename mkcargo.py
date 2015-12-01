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
    print "            <name>-<iso timestamp>.deleted  - files delete since previous snapshot"
    print "            <name>-<iso timestamp>.common   - files unchanged since previous snapshot"
    print "            <name>-<iso timestamp>.created  - files created since previous snapshot"
    print "            <name>-<iso timestamp>.modified - files modified since previous snapshot"
    print "            <name>-<iso timestamp>.errors   - files it wasn't possible to compare"
    print "            <name>-<iso timestamp>.folders  - a list of the leaf nodes in the folder tree"
    print "-jnn      - Controls multi-threading. By default the program will create one producer thread to scan the file system and one hashing thread per CPU core. Multi-threading causes output filenames to be in non-deterministic order, as files that take longer to hash will be delayed while they are hashed. If a deterministic order is required, specify -j0 to disable multi-threading."
    outputClose(outputFiles)
    return;

def outputOpen(files):
    for i in files
        if os.path.isfile(files.values(i)):
            sys.stderr.write("%s already exists!\n"%files.values(i))
            outputClose()
            exit(1)
        open(files.values(i), 'w')
     return ;

def outputClose(fileshandles):
    for i in files
        close(files.values(i), 'w')
     return;


def md5Worker(i, q):
    while True:
        path = q.get()
        formatOutput(md5sum(path, md5blocklen),  path)
        q.task_done()

def md5sum(filename, blocksize=65536):
    hash = hashlib.md5()
    with open(filename, "rb") as f:
        for block in iter(lambda: f.read(blocksize), ""):
            hash.update(block)
    return hash.hexdigest();


def compareFunnyFile(fileA, fileB):
    if not os.path.isdir(fileA):
        sys.stderr.write("%s is not a file!\n"%fileA)
        sys.exit(-1)
    elif not os.path.isdir(fileA):
        sys.stderr.write("%s is not a file!\n"%fileA)
        sys.exit(-1)
    else:
        hashA = md5sum(fileA)
        hashB = md5sum(fileB)
    return hashA == hashB;


def fullSnapshot():
    # Lets hoover up everything
    sys.stdout.write("Should really implement this!")
    sys.extit(1)
    return;

def incrSnapshot():
    if (opt_sourceLive):
	opt_source = opt_sourceLive
    else:
	opt_source = opt_sourceSnapshot

    deltaDir = dircmp(opt_previousSnapshot, opt_source)
    deltaDir.report_partial_closure()

    return;

if __name__ == '__main__':

    opt_endofline = "\0\n"
    opt_threads = multiprocessing.cpu_count()
    opt_snapshotName =""
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
            opt_outputdirectory = next(it)
            if not os.path.isdir(opt_poutputDirectory):
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
            opt_threads = int(i[2:])
            continue

    # We'll need somewhere to output this lot
    filebase = opt_outputdirectory+opt_snapshotName+"-"+opt_snapshotTimestamp
    outputFiles['deleted'] = filebase+".deleted"
    outputFiles['common'] = filebase+".common"
    outputFiles['created'] = filebase+".created"
    outputFiles['modified'] = filebase+".modified"
    outputFiles['errors'] = filebase+".errors"
    outputFiles['folders'] = filebase+".folders"

    outputOpen(outputFiles)

    if (opt_sourceSnapshot and opt_sourceLive):
	sys.stderr.write("Nothing to do?\n")
        usage()
	sys.exit(0) 
    elif opt_previousSnapshot:
        incrSnapshot()
    else:
        fullSnapshot()

    outputClose(outputFiles)
