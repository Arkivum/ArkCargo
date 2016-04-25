#!/usr/bin/env python
#
# ArkCargo - mkcargo
#
# MIT License, 
# the code to calculate a file MD5 checksum is based on code from 
# https://github.com/joswr1ght/md5deep/blob/master/md5deep.py
# 
# @author Chris Pates <chris.pates@arkivum.com>
# @copyright Arkivum Limited 2015


import os, sys, hashlib, re, multiprocessing, platform
from Queue import Queue, LifoQueue
from threading import Thread,current_thread
import time, datetime, errno
import filecmp
import csv
import argparse
import shutil

stats = {}
statsFields = {}

parser = argparse.ArgumentParser(prog='mkcargo.py', description='Analysis a filesystem and create a cargo file to drive an ingest job.')
# Set consts that we want to know about but the user can not (at this time specify)
parser.set_defaults(queueParams = {'max' : 100000, 'highWater' : 9000, 'lowWater' : 100})
parser.set_defaults(outFiles = {'valid' : ['processed.files', 'processed.dirs', 'savedstate.files', 'savedstate.dirs', 'resumestate.files', 'resumestate.dirs', 'error.log', 'debug.log', 'failed', 'added', 'modified', 'unchanged', 'symlink', 'directory', 'config', 'cargo', 'removed', 'queue.csv']})
parser.set_defaults(rootDir = ['processed', 'savedstate.files', 'savedstate.dirs', 'resumestate.files', 'resumestate.dirs', 'error.log', 'debug.log', 'config'])
parser.set_defaults(statsDir = ['snapshot.csv', 'ingest.csv', 'cargo.csv'])
parser.set_defaults(snapshotDir = ['failed', 'added', 'modified', 'unchanged', 'symlink', 'directory', 'removed'])
parser.set_defaults(cleanupFiles = ['.running', 'processed.files', 'processed.dirs', 'savedstate.files', 'savedstate.dirs', 'resumestate.files', 'resumestate.dirs'])
parser.set_defaults(savedState = False)
parser.set_defaults(includeStats = {'snapshot' : ['added', 'modified', 'unchanged', 'symlink', 'directory', 'removed', 'failed'], 'ingest' : ['added', 'modified'], 'cargo': []})
parser.set_defaults(statsBoundaries = os.path.join(os.path.dirname( os.path.realpath( __file__ )), 'boundaries.csv'))
parser.set_defaults(executablePath = os.path.dirname( os.path.realpath( __file__ )))
parser.set_defaults(terminalPath = os.path.realpath(os.getcwd()))
parser.set_defaults(cargoEOL = '\0')
parser.set_defaults(cargoPad = 6)
parser.set_defaults(cargoExt = '.md5')
parser.set_defaults(lastRunPad = 4)
parser.set_defaults(lastRunPrefix = 'run')
parser.set_defaults(snapshotEOL = '\n')
parser.set_defaults(sys_uname = platform.uname())

parser.set_defaults(startTime = datetime.datetime.now())
parser.set_defaults(processedFile = "")

parser.add_argument('--version', action='version', version='%(prog)s 0.3.3')

parser.add_argument('-s', dest='followSymlink', action='store_true', help='follow symlinks and ingest their target, defaults to recording symlinks and their targets in the symlink file.')

parser.add_argument('-j', metavar='threads', nargs='?', dest='threads', type=int, default=(max(multiprocessing.cpu_count(),1)), help='Controls multi-threading. By default the program will create one thread per CPU core, one thread is used for writing to the output files, the rest for scanning the file system and calculating MD5 hashes. Multi-threading causes output filenames to be in non-deterministic order, as files that take longer to hash will be delayed while they are hashed.')

parser.add_argument('-r', dest='relPathInCargos', action='store_true', help='use relative paths in cargos for rebasing of structures on ingest.')
parser.add_argument('-n', nargs='?', metavar='name', dest='name', type=str, required=True, help='a meaningful name for the dataset, this should be consistent for all snapshot of a given dataset.')

parser.add_argument('-t', nargs='?', metavar='yyyymmddThhmmss', dest='timestamp', type=str, required=True, help='Where a filesystem snapshot is being processed then it is important to use the timestamp from the newer snapshot.')

parser.add_argument('-o', nargs='?', metavar='output directory', dest='output', type=str, default="output", help='the directory under which to write the output. <output directory>/<name>/<timestamp>/<output files>.')

parser.add_argument('-cargoMax', dest='cargoMax', default='100GB', help=argparse.SUPPRESS)

parser.add_argument('--exists', dest='prepMode', choices=['clean', 'preserve', 'append'], default='preserve', help=argparse.SUPPRESS)
parser.add_argument('--clean', action='store_true', help='if output directory exists delete its contents before process, by default previous output is preserved.')
parser.add_argument('--resume', action='store_true', help='if files.queue and dirs.queue restore the queue states and set append mode.')
parser.add_argument('--debug', dest='debug', action='store_true', help=argparse.SUPPRESS)

parser.add_argument('--stats', dest='cargo', action='store_false', help='calculate stats (does not generate a cargo file)')

parser.add_argument('--rework', metavar='file', nargs='+', help='a file containing paths for which a cargo file needs to be generated, generally based on a {failed} file from a previous run to be append to the stats and cargo')

group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--full', metavar='snapshots', dest='snapshots', nargs=1, default="", help='generate a cargo file for the current snapshot')
group.add_argument('--incr', metavar='snapshots', dest='snapshots', nargs=2, default="", help='generates a cargo file for the difference between the first snapshot and the second')
group.add_argument('--files', metavar='file', dest='file', nargs='+', default="", help='a file containing explicit paths for which a cargo file needs to be generated')

def touch(path):
    with open(path, 'a'):
        os.utime(path, None)


# convert human readable version of a size in Bytes
#
def toBytes(humanBytes):
    # convert human readable to '0's
    byteUnits = {'KB' : '000', 'MB' : '000000', 'GB' : '000000000', 'TB' :'000000000000', 'PB' : '000000000000000'}

    humanBytes = humanBytes.upper()
    humanUnits = humanBytes[-2:]
    validUnits = byteUnits.keys()
    if humanUnits not in validUnits:
        sys.stderr.write("Cargo units (%s) not recognised, must have units of: %s\n"%(humanUnits, validUnits))
        sys.exit(-1) 
    else:
        justBytes = humanBytes[:-2] + byteUnits.get(humanUnits)
    return int(justBytes);

def debugMsg(message):
    if args.debug:
        resultsQueue.put(("debug.log", "", message + args.snapshotEOL))
    return;

def errorMsg(message):
    resultsQueue.put(("error.log", "", message + args.snapshotEOL))
    return;

def queueMsg(message):
    if args.debug:
        resultsQueue.put(("queue.csv", "", message + args.snapshotEOL))
    return;

def isDirectory(path):
    resultsQueue.put(("directory", 0, path + args.snapshotEOL))
    return;

def isFailed(path):
    resultsQueue.put(("failed", 0, path + args.snapshotEOL))
    return;

def isAdded(path, bytes):
    resultsQueue.put(("added", bytes, path + args.snapshotEOL))
    return;

def isModified(path, bytes):
    resultsQueue.put(("modified", bytes, path + args.snapshotEOL))
    return;

def isUnchanged(path, bytes):
    resultsQueue.put(("unchanged", bytes, path + args.snapshotEOL))
    return;

def isRemoved(path, bytes):
    resultsQueue.put(("removed", bytes, path + args.snapshotEOL))
    return;

def isSymlink(path, target):
    resultsQueue.put(("symlink", 0, "%s %s%s"%(path, target, args.snapshotEOL)))
    return;

def queueFiles(type, path):
    resultsQueue.put(("%s.files"%type, "", "%s%s"%(path, args.snapshotEOL)))
    return;

def queueDirs(type, path):
    resultsQueue.put(("%s.dirs"%type, "", "%s%s"%(path, args.snapshotEOL)))
    return;

def promptUser_yes_no(question):
    reply = str(raw_input(question+' (y/n): ')).lower().strip()
    if reply[0] == 'y':
        return True
    if reply[0] == 'n':
        return False
    else:
        return yes_or_no(question)

def listDir(path):
    fileList = []
    dirList = []

    debugMsg("listDir (%s) - %s"%(current_thread().getName(), path))
   
    # This is necessary because when used with a VFS, just listing a path
    # without stepping into the file system can have interesting and 
    # unpredictable results, like an empty listing. 
    os.chdir(path)
    listing = os.listdir('path')
    debugMsg("listDir (%s) - %s items"%(current_thread().getName(), len(listing)))

    for child in listing:
        childPath = os.path.join(path, child)
        if os.path.isdir(childPath):
            dirList.append(child)
        elif os.path.isfile(childPath):
            fileList.append(child)
        else:
            isFailed(childPath)
            errorMsg("is not file, dir or symlink? - %s"%childPath)

    debugMsg("listDir (%s) dirs - %s"%(current_thread().getName(), dirList))
    debugMsg("listDir (%s) files - %s"%(current_thread().getName(), fileList))
    return dirList, fileList;


# log current config to a file
#
def logConfig():
    resultsQueue.put(("config", "", "%s%s"%(args, args.snapshotEOL)))
    return;


# move some files to a new 'run' directory
# copy some others but delete nothing!
#
def prepOutput():
    flag_running = ".running"
    flag_pause = ".pause"
    flag_complete = ".complete"
    cargo_ext = ".md5"
    subdirs = ["stats", "cargos", "snapshot"]
    dirlist = []
    filelist = []
    global args 

    try:
        if os.path.isdir(args.filebase):
            listing = os.listdir(args.filebase)

            for child in listing:
                childPath = os.path.abspath(os.path.join(args.filebase, child))
                if os.path.isdir(childPath):
                    dirlist.append(child)
                elif os.path.isfile(childPath):
                    filelist.append(child)

            if flag_pause in filelist:
                os.remove(os.path.join(args.filebase, flag_pause))

            if flag_complete in filelist:
                print("This job has previously completed, clear output directory to re-run from scratch.")
                exit(1)

            if flag_running in filelist:
                print("This job is either still running or has die prematurely, check that it is not still running.")
                if not promptUser_yes_no("reconstructure state of incomplete job and continue?"):
                    exit(1)
            else:
                touch(os.path.join(args.filebase, flag_running))
            
            if os.path.isfile(os.path.join(args.filebase, 'processed')):
                args.processedFile = os.path.join(args.filebase, 'processed')

            if len(set(['processed.files', 'processed.dirs', 'savedstate.files', 'savedstate.dirs']) | set(filelist)) > 0:
                args.savedState = True

            if len(set(subdirs) & set(dirlist)) < len(subdirs):
                #we need to migrate from a pre 0.3.0 version of mkcargo
                print "making 0.3.x subdirs"
                statsDir = os.path.join(args.filebase, 'stats')
                snapDir = os.path.join(args.filebase, 'snapshot')
                cargoDir = os.path.join(args.filebase, 'cargos')

                for dir in subdirs:
                    subdirPath =os.path.join(args.filebase, dir)
                    if not os.path.isdir(subdirPath):
                        os.makedirs(subdirPath)

                #Now we move the files to their post 0.3.0 home
                for file in filelist:
                    if file.endswith(".md5"):
                        shutil.move(os.path.join(args.filebase, file), os.path.join(args.filebase, cargoDir, file))
                    elif file.endswith(".csv"):
                        shutil.move(os.path.join(args.filebase, file), os.path.join(args.filebase, statsDir, file))
                    elif file in args.snapshotDir:
                        shutil.move(os.path.join(args.filebase, file), os.path.join(args.filebase, snapDir, file))

            # lets figure out what to do if there already files in the output directory
            if args.rework or args.resume:
                args.prepMode = 'append'
            elif args.clean:
                if savedState:
                    if not promptUser_yes_no("This job has previously be run and its stated store, are you sure you wish to delete it?"):
                        if promptUser_yes_no("Would you like to override the --clean and resume the previous run?"):
                            args.prepMode = 'append'
                            args.resume = True
                        else:
                            exit(1)        
                    else:
                        args.prepMode = 'clean'
                else:
                    args.prepMode = 'clean'

            if args.prepMode == 'clean':
                for file in filelist:
                    os.remove(os.path.join(args.filebase, file))
                for dir in dirlist:
                    shutil.rmtree(os.path.join(args.filebase, dir))
                for dir in subdirs:
                    os.makedirs(os.path.join(args.filebase, dir))
            else:
                args.prepMode = 'append'
                if args.savedState:
                    print "resuming state from previous run"
                    dirsResume = os.path.join(args.filebase, 'savedstate.dirs')
                    filesResume = os.path.join(args.filebase, 'savedstate.files')
                    args.resumeQueues = [ dirsResume, filesResume ]
                elif resumeState:    
                    print "resuming from check point"
                    dirsResume = os.path.join(args.filebase, 'resumestate.dirs')
                    filesResume = os.path.join(args.filebase, 'resumestate.files')
                    args.resumeQueues = [ dirsResume, filesResume ]
        else:
            os.makedirs(args.filebase)
            for dir in subdirs:
                os.makedirs(os.path.join(args.filebase, dir))
            touch(os.path.join(args.filebase, flag_running))

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

def cleanup():
    for file in args.cleanupFiles:
        filepath = os.path.join(args.filebase, file)
        if os.path.isfile(filepath):
            os.remove(filepath)
    touch(os.path.join(args.filebase, '.complete'))
    return;

def prepStats():
    boundaries = []
    fields =[]

    # load stats boundaries
    boundaries, fields = loadBoundaries( args.statsBoundaries )

    if args.prepMode == 'append':
        stats = loadStats(fields)
    else:
        stats = initStats(fields)
    return boundaries, fields, stats;


def loadStats(fields):
    cargoCount = 0

    # imports stats and boundaries, only relevant for 'rework' mode
    try:
        # First load in the stats for the snapshot (so far)
        file = os.path.join(args.filebase, 'stats', 'snapshot.csv')
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

        file = os.path.join(args.filebase, 'stats', 'cargo.csv')
        with open(file) as csvfile:
            statsBoundaries = csv.DictReader(csvfile)
            # skip the header row, as this was created above
            next(statsBoundaries)
            for row in statsBoundaries:
                stats[row['Category']] = {}
                cargoCount += 1
                for field in fields:
                    if field == 'Category':
                        args.includeStats['cargo'].append(row[field])
                        stats[row['Category']][field] = row[field]
                    else:
                        stats[row['Category']][field] = int(row[field])
        # [TO DO]

        # even if we are reworking failed files, we'll start with a new cargo file
        stats['chunk'] = {}
        stats['chunk']['Vol'] = 0
        stats['chunk']['Num'] = cargoCount

    except ValueError:
        sys.stderr.write("Can't import stats to file %s (error: %s)\n"%(file, ValueError))
        exit(-1)

    return stats;

def initStats(fields):
    # imports boundaries and initialise stats

    for category in args.includeStats['snapshot']:
        stats[category] = {}
        for field in fields:
            if field == 'Category':
                stats[category]['Category'] = category
            else:   
                stats[category][field] = 0
    stats['chunk'] = {}
    stats['chunk']['Vol'] = 0
    stats['chunk']['Num'] = 0

    return stats;


def updateStats(file, size):
    newFile = False
    global stats 
    filePath = ""
    bytes = int(size)

    if (stats['chunk']['Vol'] + bytes) > args.cargoMaxBytes:
        stats['chunk']['Num'] += 1 
        newFile = True
    else:
        stats['chunk']['Vol'] += bytes

    if file == "cargo":
        filename = args.name + "-" +  args.timestamp + "-" + str(stats['chunk']['Num']).zfill(args.cargoPad) + args.cargoExt
        filePath = os.path.join(args.filebase, 'cargos', filename)
        file = file + str(stats['chunk']['Num']).zfill(args.cargoPad)
        if file not in args.includeStats['cargo']:
            #add cargo file to list
            args.includeStats['cargo'].append(file)

            #initialise new stats line
            stats[file] = {}
            for field in statsFields:
                if field == 'Category':
                    stats[file]['Category'] = file
                else:  
                    stats[file][field] = 0
    else:
        if file in args.statsDir:
            filePath = os.path.join(args.filebase, 'stats', file)
        elif file in args.snapshotDir:
            filename = str(stats['chunk']['Num']).zfill(args.cargoPad) + "." + file
            filePath = os.path.join(args.filebase, 'snapshot', filename)
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
        for statSet in args.includeStats:
            file = os.path.join(args.filebase,'stats',statSet+'.csv')
            with open(file, "wb") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=statsFields)
                #writer.writeheader()
                fields = {}
                for field in statsFields:
                    fields[field] = field
                writer.writerow(fields)
                for category in args.includeStats[statSet]:
                    writer.writerow(stats[category])
    except ValueError:
        sys.stderr.write("Can't export stats to file %s (error: %s)\n"%(filefull, ValueError))
    return;


# outputWorker used by threads to process output to the various output files. The must never be more
# than of this type of thread running.
#
def outputResult(i, files, q):
    otherFiles = ['cargo']
    global terminateThreads

    while True:
        changeFile = False
        filePath = ""
        file, bytes, message = q.get()

        if file not in args.outFiles['valid']:
            sys.stderr.write("invalid output file'%s'"%file)
            sys.exit(-1)

        # update stats for this file
        if file in list( set(otherFiles) | set(args.includeStats['snapshot']) | set(args.includeStats['ingest']) ):
            changeFile, filePath = updateStats(file, bytes)
        elif file in args.snapshotDir:
            filePath = os.path.join(args.filebase, 'snapshot', file)
        else:
            filePath = os.path.join(args.filebase, file)

        if changeFile and (file in files):
           try:
               (files[file]).close()
               files[file] = open(filePath, "w", 0)
           except ValueError:
                sys.stderr.write("can't open %s"%file)
                sys.exit(-1)

        if not file in files:
            try:
                # This is necessary for REALLY early 2.6.1 builds
                # where append mode doesn't create a file if it doesn't
                # already exist.
                mode = 'a' if os.path.exists(filePath) else 'w'
                files[file] = open(filePath, mode, 0)
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
        if changeFile:
            exportStats()
        q.task_done()


# Calculate the MD5 sum of the file
#
def cargoEntry(path):
    global resultsQueue
    if args.cargo:        
        hash = hashlib.md5()
        blocksize=65536

        try:
            if args.file:
                absPath = path
            else:
                absPath = os.path.abspath(os.path.join(args.snapshotCurrent, path))
            debugMsg("cargoEntry (%s) - %s"%(current_thread().getName(), absPath))

            # the following means of calculating an MD5 checksum for a file
            # is based on code in https://github.com/joswr1ght/md5deep/blob/master/md5deep.py
            # also under MIT license.
            with open(absPath, "rb") as f:
                for block in iter(lambda: f.read(blocksize), ""):
                    hash.update(block)

            hash = hash.hexdigest().replace(" ","")
            if args.relPathInCargos:
                filePath = path
            else:
                filePath = absPath
            resultsQueue.put(("cargo", os.path.getsize(absPath), "%s  %s%s"%(hash, filePath, args.cargoEOL)))

        except (IOError, OSError) as e:
            errorMsg("%s %s"%(e, absPath))
            isFailed(path)
            pass
    return;

def saveState(type):
    global terminateThreads

    terminateThreads = True
    print "The job is exiting and saving the current queue to disk."
    print "Please be patient."
    print "saving file queue state..."
    while not fileQueue.empty():
        queueFiles(type, fileQueue.get())
    print "file queue state saved."
    print "saving directory queue state..."
    while not dirQueue.empty():
        queueDirs(type, dirQueue.get())
    print "directory queue state saved."
    print "waiting for threads to complete."
    for pathWorker in pathWorkers:
        pathWorker.join()
    print "all threads have completed."
    resultsQueue.join()
    print "exporting stats."
    exportStats()
    print "stats saved."
    for file in fileHandles:
        fileHandles[file].close()
    if os.path.isfile(os.path.join(args.filebase, '.running')):
        os.remove(os.path.join(args.filebase, '.running'))
    return();


# pathWorker used by threads to process a Full snapshot of the file tree
#
def snapshotFull(i, f, d):
    threadName =current_thread().getName()
    fileOnly = False if (i % 2) == 0 else True
    debugMsg("thread ident %s - fileOnly: %s"%(i, fileOnly))  
    processedFiles = None
    processedDirs = None
 
    if os.path.isdir(args.snapshotCurrent):
        # This is necessary because when used with a VFS, just listing a path
        # without stepping into the file system can have interesting and
        # unpredictable results, like an empty listing.
        os.chdir(args.snapshotCurrent)
    else:
        errorMsg("bad snapshot: %s"%args.snapshotCurrent)

    # if savedState then we need to open filehandles to check whether they are
    # have previously been processed
    try:
        if args.savedState:
            processedfilePath = os.path.join(args.filebase, 'processed.files')
            processeddirPath = os.path.join(args.filebase, 'processed.dirs')
            if os.path.exists(processedfilePath):
                processedFiles = open(processedfilePath, 'r')
            if os.path.exists(processeddirPath):
                processedDirs = open(processeddirPath, 'r')

    except (IOError, OSError) as e:
        errorMsg("%s %s"%(e, processedfilePath))
        exit(-1)

    while not terminateThreads:
        if os.path.exists(os.path.join(args.filebase, '.pause')):
            saveState("savedstate")
        elif not f.empty():
            debugMsg("f (%s) d (%s) (%s) calling fileFull"%(f.qsize(), d.qsize(), threadName))
            if (args.savedState and processedFiles):
                processedFiles.seek(0)
            fileFull(f, processedFiles)
        elif d.empty() or fileOnly:
            debugMsg("f (%s) d (%s) (%s) Idle"%(f.qsize(), d.qsize(), threadName))
            time.sleep(i)
        else:
            debugMsg("f (%s) d (%s) (%s) calling dirFull"%(f.qsize(), d.qsize(), threadName))
            dirFull(d, f, processedDirs)
    if args.savedState: 
        if processedFiles:
            processedFiles.close()
        if processedDirs:
            processedDirs.close()
    return;


# process a single file path 
# 
def fileFull(fileQ, processedFiles):
    relPath = fileQ.get()
    try:
        absPath = os.path.abspath(os.path.join(args.snapshotCurrent, relPath))

        debugMsg("fileFull (%s)- %s"%(current_thread().getName(), relPath))
        if args.savedState and processedFiles:
            for line in processedFiles:
                if relPath in line:
                    fileQ.task_done()
                    return;
        if not os.access(absPath, os.R_OK):
            errorMsg("Permission Denied: %s"%absPath)
            isFailed(relPath)
        else:
            if (not args.followSymlink) and os.path.islink(absPath):
                isSymlink(relPath, os.path.realpath(absPath))
            else:
                isAdded(relPath, os.path.getsize(absPath))
                cargoEntry(relPath)
    except (IOError, OSError) as e:
        errorMsg("%s %s"%(e, absPath))
        isFailed(absPpath)
        pass
    fileQ.task_done()
    return;

# process a single directory
#
def dirFull(dirQ, fileQ, processedDirs):
    leafNode = True
    relPath = dirQ.get()
    try:
        absPath = os.path.abspath(os.path.join(args.snapshotCurrent, relPath))

        debugMsg("dirfull (%s)- %s"%(current_thread().getName(), relPath))
        if not os.path.exists(absPath):
            errorMsg("Path does not exist: %s"%absPath)
            isFailed(relPath)
        elif not os.access(absPath, os.R_OK):
            errorMsg("Permission Denied: %s"%absPath)
            isFailed(relPath)
        elif os.path.islink(absPath) and os.path.isdir(absPath):
            isSymlink(relPath, os.path.realpath(absPath))	
        elif os.path.isdir(absPath):
            debugMsg("dirfull (%s) isDir-%s"%(current_thread().getName(), absPath))

            tries = 6
            attempt = 0
        
            while (attempt < tries):
                attempt += 1
                listing = os.listdir(absPath)
                if len(listing) > 0:
                    debugMsg("dirFull (%s) attempt %s - %s items"%(current_thread().getName(), attempt, len(listing)))
                    break;
                else: 
                    debugMsg("dirFull (%s) - empty directory?"%current_thread().getName())
                    time.sleep(1)       

            for childItem in listing:
                childAbs = os.path.abspath(os.path.join(absPath, childItem))
                childRel = os.path.join(relPath, childItem)
                debugMsg("dirFull (%s)- %s"%(current_thread().getName(), childRel))
                if os.path.isdir(childAbs):
                    leafNode = False
                    dirQ.put(childRel)
                    debugMsg("dirQueue.put (%s)- %s"%(current_thread().getName(), childRel))
                elif os.path.isfile(childAbs):
                    fileQ.put(childRel)
                    debugMsg("fileQueue.put (%s)- %s"%(current_thread().getName(), childRel))
                else:
                    isFailed(childRel)
                    errorMsg("is not file, dir or symlink? - %s"%childRel)

            if leafNode:
                # must be leaf node lets record it
                if args.savedState and processedDirs:
                    dirProcessed = False
                    for line in processedDirs:
                        if relPath in line:
                           dirProcessed = True
                           break
                    if dirProcessed:
                        isDirectory(relPath)
                else:
                    isDirectory(relPath) 
    except (IOError, OSError) as e:
        errorMsg("%s %s"%(e, absPath))
        isFailed(relPath)
        pass
    dirQ.task_done()
    return;


# pathWorker used by threads to process a Full snapshot of the file tree
#
def snapshotIncr(i, f, d):
    threadName =current_thread().getName()
    fileOnly = False if (i % 2) == 0 else True
    processedFiles = None
    processedDirs = None

    debugMsg("thread ident %s - fileOnly: %s"%(i, fileOnly))
    if os.path.isdir(args.snapshotCurrent):
        # This is necessary because when used with a VFS, just listing a path
        # without stepping into the file system can have interesting and
        # unpredictable results, like an empty listing.
        os.chdir(args.snapshotCurrent)
    else:
        errorMsg("bad snapshot: %s"%args.snapshotCurrent)

    try:
        if args.savedState:
            processedfilePath = os.path.join(args.filebase, 'processed.files')
            processeddirPath = os.path.join(args.filebase, 'processed.dirs')
            if os.path.exists(processedfilePath):
                processedFiles = open(processedfilePath, 'r')
            if os.path.exists(processeddirPath):
                processedDirs = open(processeddirPath, 'r')
    except (IOError, OSError) as e:
        errorMsg("%s %s"%(e, processedfilePath))
        exit(-1)

    while not terminateThreads:
        if os.path.exists(os.path.join(args.filebase, '.pause')):
            saveState('savedstate')
        elif not f.empty():
            debugMsg("f (%s) d (%s) (%s) calling fileIncr"%(f.qsize(), d.qsize(), threadName))
            if (args.savedState and processedFiles):
                processedFiles.seek(0)
            fileIncr(f, processedFiles)
        elif d.empty() or fileOnly:
            debugMsg("f (%s) d (%s) (%s) Idle"%(f.qsize(), d.qsize(), threadName))
            time.sleep(i)
        else:
            debugMsg("f (%s) d (%s) (%s) calling dirIncr"%(f.qsize(), d.qsize(), threadName))
            if (args.savedState and processedFiles):
               processedFiles.seek(0)
            if (args.savedState and processedDirs):
               processedDirs.seek(0)
            dirIncr(d, f, processedDirs, processedFiles)

    if args.savedState:
        if processedFiles:
            processedFiles.close()
        if processedDirs:
            processedDirs.close()
    return;


# process a single file path
#
def fileIncr(fileQ, processedFiles):
    relPath = fileQ.get()
    debugMsg("fileIncr (%s)- %s - start"%(current_thread().getName(), relPath))
    try:
        absPath = os.path.abspath(os.path.join(args.snapshotCurrent, relPath))
        oldPath = os.path.abspath(os.path.join(args.snapshotPrevious, relPath))

        debugMsg("fileIncr (%s)- %s"%(current_thread().getName(), absPath))
        if args.savedState and processedFiles:
            for line in processedFiles:
                if relPath in line:
                    fileQ.task_done()
                    return;

        if not os.access(absPath, os.R_OK):
            errorMsg("Permission Denied; %s"%absPath)
            isFailed(relPath)
        elif (not args.followSymlink) and os.path.islink(absPath):
            debugMsg("fileIncr (%s) isSymlink- %s"%(current_thread().getName(), relPath))
            isSymlink(relPath, os.path.realpath(absPath))
        else:
            if os.path.isfile(oldPath):
                if not os.access(oldPath, os.R_OK):
                    errorMsg("Permission Denied (assuming Modified); %s"%oldPath)
                    isAdded(relPath, os.path.getsize(absPath))
                    cargoEntry(relPath)
                elif filecmp.cmp(oldPath, absPath):
                    debugMsg("fileIncr (%s) isUnchanged- %s"%(current_thread().getName(), relPath))
                    isUnchanged(relPath, os.path.getsize(absPath))
                else:
                    debugMsg("fileIncr (%s) isModified- %s"%(current_thread().getName(), relPath))
                    isModified(relPath, os.path.getsize(absPath))
                    cargoEntry(relPath)
            else:    
                debugMsg("fileIncr (%s) isAdded- %s"%(current_thread().getName(), relPath))
                isAdded(relPath, os.path.getsize(absPath))
                cargoEntry(relPath)
    except (IOError, OSError) as e:
        errorMsg("%s %s"%(e, absPath))
        isFailed(relPath)
        pass
    debugMsg("fileIncr (%s)- %s - complete"%(current_thread().getName(), relPath))
    fileQ.task_done()
    return;


# process a single directory
#
def dirIncr(dirQ, fileQ, processedDirs, processedFiles):
    leafNode = True
    relPath = dirQ.get()
    try:
        absPath = os.path.abspath(os.path.join(args.snapshotCurrent, relPath))
        oldPath = os.path.abspath(os.path.join(args.snapshotPrevious, relPath))
        debugMsg("dirIncr (%s)- %s"%(current_thread().getName(), relPath))
        if not os.access(absPath, os.R_OK):
            errorMsg("Permission Denied; %s"%absPath)
            isFailed(relPath)
        elif (not args.followSymlink) and os.path.islink(absPath):
            isSymlink(relPath, os.path.realpath(absPath))
        elif os.path.isdir(absPath):
            debugMsg("dirIncr (%s)- %s"%(current_thread().getName(), absPath))

            tries = 6
            attempt = 0

            while (attempt < tries):
                attempt += 1
                listingNew = os.listdir(absPath)
                if len(listingNew) > 0:
                    debugMsg("dirIncr (%s) attempt %s - %s items"%(current_thread().getName(), attempt, len(listingNew)))
                    break;
                else: 
                    debugMsg("dirIncr (%s) - empty directory?"%current_thread().getName())

            if os.path.isdir(oldPath):
                if not os.access(oldPath, os.R_OK):
                    errorMsg("Permission Denied (assuming Modified); %s"%oldPath)
                else: 
                    listingOld = os.listdir(oldPath)
                    debugMsg("dirIncr (%s) old - %s items"%(current_thread().getName(), len(listingOld)))
                    for removed in list(set(listingOld).difference(listingNew)):
                        if args.savedState and processedFiles:
                            processedFiles.seek(0)
                            fileProcessed = False
                            for line in processedFiles:
                                if relPath in line:
                                    fileProcessed = True
                                    break
                            if not fileProcessed:
                                isRemoved(os.path.join(relPath, removed), os.path.getsize(oldPath))
                        else:
                            isRemoved(os.path.join(relPath, removed), os.path.getsize(oldPath))

            for childItem in listingNew:
                childAbs = os.path.abspath(os.path.join(absPath, childItem))
                childRel = os.path.join(relPath, childItem)
                debugMsg("dirIncr (%s)- %s"%(current_thread().getName(), childItem))
                if os.path.isdir(childAbs):
                    leafNode = False
                    dirQ.put(childRel)
                    debugMsg("dirQueue.put (%s)- %s"%(current_thread().getName(), childRel))
                elif os.path.isfile(childAbs):
                    fileQ.put(childRel)
                    debugMsg("fileQueue.put (%s)- %s"%(current_thread().getName(), childRel))
                else:
                    isFailed(childRel)
                    errorMsg("is not file, dir or symlink? - %s"%childRel)

            if leafNode:
                # must be leaf node lets record it
                if args.savedState and processedDirs:
                    dirProcessed = False
                    for line in processedDirs:
                        if relPath in line:
                           dirProcessed = True
                           break
                    if not dirProcessed:
                        isDirectory(relPath)
                else:
                    isDirectory(relPath)
        else:
            debugMsg("fileQueue.put (%s)- file in dirQueue %s"%(current_thread().getName(), relPath))
            fileQ.put(relPath)
    except (IOError, OSError) as e:
        errorMsg("%s %s"%(e, absPath))
        isFailed(relPath)
        pass
    dirQueue.task_done()
    return;


# pathWorker used by threads to process a Full snapshot of the file tree
#
def snapshotExplicit(i, f, d):
    threadName =current_thread().getName()
    fileOnly = False if (i % 2) == 0 else True
    debugMsg("thread ident %s - fileOnly: %s"%(i, fileOnly))

    # if savedState then we need to open filehandles to check whether they are
    # have previously been processed
    try:
        if args.savedState:
            processedfilePath = os.path.join(args.filebase, 'processed.files')
            processeddirPath = os.path.join(args.filebase, 'processed.dirs')
            if os.path.exists(processedfilePath):
                processedFiles = open(processedfilePath, 'r')
            if os.path.exists(processeddirPath):
                processedDirs = open(processeddirPath, 'r')
    except (IOError, OSError) as e:
        errorMsg("%s %s"%(e, processedfilePath))
        exit(-1)

    while not terminateThreads:
        if os.path.exists(os.path.join(args.filebase, '.pause')):
            saveState('savedstate')
        elif not f.empty():
            debugMsg("f (%s) d (%s) (%s) calling fileFull"%(f.qsize(), d.qsize(), threadName))
            if (args.savedState and processedFiles):
                processedFiles.seek(0)
                fileExplicit(f, processedFiles)
        elif d.empty() or fileOnly:
            debugMsg("f (%s) d (%s) (%s) Idle"%(f.qsize(), d.qsize(), threadName))
            time.sleep(i)
        else:
            debugMsg("f (%s) d (%s) (%s) calling dirFull"%(f.qsize(), d.qsize(), threadName))
            dirExplicit(d, f, processedDirs)
    if args.savedState:
        if processedFiles:
            processedFiles.close()
        if processedDirs:
            processedDirs.close()
    return;


# process a single file path
#
def fileExplicit(fileQ, processedFiles):
    absPath = fileQ.get()
    debugMsg("fileExplicit (%s)- %s"%(current_thread().getName(), absPath))
    try:
        if args.savedState and processedFiles:
            for line in processedFiles:
                if relPath in line:
                    fileQ.task_done()
                    return;

        if not os.path.exists(absPath):
            errorMsg("Does not exist; %s"%absPath)
            isFailed(absPath)
        elif not os.access(absPath, os.R_OK):
            errorMsg("Permission Denied; %s"%absPath)
            isFailed(absPath)
        else:
            if (not args.followSymlink) and os.path.islink(absPath):
                isSymlink(absPath, os.path.realpath(absPath))
            else:
                isAdded(absPath, os.path.getsize(absPath))
                cargoEntry(absPath)
    except (IOError, OSError) as e:
        errorMsg("%s %s"%(e, absPath))
        isFailed(path)
        pass
    fileQ.task_done()
    return;


# process a single directory
#
def dirExplicit(dirQ, fileQ):
    absPath = dirQ.get()
    debugMsg("dirExplicit (%s)- %s"%(current_thread().getName(), absPath))

    try:
        if not os.path.exists(absPath):
            errorMsg("Does not exist; %s"%absPath)
            isFailed(absPath)
        elif not os.access(absPath, os.R_OK):
            errorMsg("Permission Denied: %s"%absPath)
            isFailed(absPath)
        elif os.path.isdir(absPath):
            dirs, files = listDir(absPath)
            debugMsg("dirs %s %s"%(absPath, dirs))
            debugMsg("files %s %s"%(absPath, files))

            if len(dirs) > 0:
                for childPath in dirs:
                    dirQ.put(os.path.join(absPath, childPath))
                    debugMsg("dirQueue.put (%s)- %s"%(current_thread().getName(), os.path.join(absPath, childPath)))
            else:
                # must be leaf node lets record it
                isDirectory(absPath)

            for childPath in files:
                fileQ.put(os.path.join(absPath, childPath))
                debugMsg("fileQueue.put (%s)- %s"%(current_thread().getName(), os.path.join(absPath, childPath)))
        else:
            debugMsg("fileQueue.put (%s)- file in dirQueue %s"%(current_thread().getName(), absPath))
            fileQ.put(absPath)
    except (IOError, OSError) as e:
        errorMsg("%s %s"%(e, absPath))
        isFailed(relPath)
        pass
    dirQ.task_done()
    return;


# prime Queues with paths to be processed
#
def primeQueues(fileQueue, dirQueue):

    fileList =[]

    if args.rework:
        fileList += args.rework
    if args.file:
        fileList += args.file
    if args.resume:
       fileList = args.resumeQueues    
    if len(fileList):
        try:
            for pathFile in fileList:
                for line in open(pathFile):
                    relPath = line.rstrip('\n').rstrip('\r')
                    if args.file:
                        absPath = relPath
                    else:
                        absPath = os.path.join(args.snapshotCurrent, relPath)

                    debugMsg("primeQueues - %s"%absPath)

                    if os.path.isdir(absPath):
                        dirQueue.put(relPath)
                    elif os.path.isfile(absPath):
                        fileQueue.put(relPath)
                    else:
                        isFailed(relPath)
                        errorMsg("invalid path: %s"%relPath)

            print "creating resume state from saved state..."
            if os.path.exists(os.path.join(args.filebase, 'savedstate.files')):
                os.rename(os.path.join(args.filebase, 'savedstate.files'), os.path.join(args.filebase, 'resumestate.files'))
            if os.path.exists(os.path.join(args.filebase, 'savedstate.dirs')):
                os.rename(os.path.join(args.filebase, 'savedstate.dirs'), os.path.join(args.filebase, 'resumestate.dirs'))

        except ValueError:
            # Time to tell all the threads to bail out
            terminateThreads = True
            sys.stderr.write("Cannot read list of files to ingest from %s (error: %s)\n"%(args.rework, ValueError))
            sys.exit(-1)

    else:
        relPath = "."
        absPath = os.path.join(args.snapshotCurrent, relPath)

        debugMsg("primeQueues - %s"%absPath)

        if os.path.isdir(absPath):
            dirQueue.put(relPath)
        else:
            isFailed(relPath)
            errorMsg("invalid path: %s"%relPath)
    return;


if __name__ == '__main__':
    global args
    args = parser.parse_args()
    args.cargoMaxBytes = toBytes(args.cargoMax)
    terminateThreads = False

    # initialise Queues
    fileQueue = Queue(args.queueParams['max'])
    dirQueue = LifoQueue(args.queueParams['max'])
    resultsQueue = Queue(args.threads*args.queueParams['max'])

    try:
        if len(args.file) > 0:
            args.mode = 'explicit'
        elif len(args.snapshots) == 1:
            args.snapshotCurrent = args.snapshots[0]
            args.mode = 'full'
        else:
            args.snapshotPrevious, args.snapshotCurrent = args.snapshots
            args.mode = 'incr'

        if args.mode in ['full', 'incr'] and os.path.isdir(args.snapshotCurrent):
            os.chdir(args.snapshotCurrent)
        else:
            sys.stderr.write("Current not a directory!\n")
            sys.exit(-1)

        args.filebase = os.path.join(args.output, args.name, args.timestamp)
        prepOutput()

        statsBoundaries, statsFields, stats = prepStats()
    except ValueError:
        # Time to tell all the threads to bail out
        terminateThreads = True
        sys.stderr.write("Cannot create output directory (error: %s)\n"%ValueError)
        sys.exit(-1)
 
    debugMsg("Running in %s mode"%args.mode)  
    logConfig()

    fileHandles = {}

    try:
        # setup the single results worker
        resultsWorker = Thread(target=outputResult, args=(1, fileHandles, resultsQueue))
        resultsWorker.setDaemon(True)
        resultsWorker.start()
        
        pathWorkers = []
        # setup the pool of path workers
        for i in range(args.threads):
            if args.mode == 'full':
                pathWorker = Thread(target=snapshotFull, args=(i, fileQueue, dirQueue))
            elif args.mode == 'incr':
                pathWorker = Thread(target=snapshotIncr, args=(i, fileQueue, dirQueue))
            elif args.mode == 'explicit':
                pathWorker = Thread(target=snapshotExplicit, args=(i, fileQueue, dirQueue))
            else:
                terminateThreads = True
                sys.stderr.write("unrecognised mode! abandon excution.\n")
                sys.exit(-1)
            pathWorkers.append(pathWorker)

        for pathWorker in pathWorkers:
            pathWorker.setDaemon(True)
            pathWorker.start()

        # now the worker threads are processing lets feed the fileQueue, this will block if the 
        # rework file is larger than the queue.
        primeQueues(fileQueue, dirQueue)


        if args.debug:
            queueMsg("\"max\", \"file\", \"dir\", \"results\"")
        # lets just hang back and wait for the queues to empty
        print "If you need to pause this job, press Ctrl-C once"
        while not terminateThreads:
            if args.debug:
                queueMsg("\"%s\", \"%s\", \"%s\", \"%s\"\n"%(args.queueParams['max'], fileQueue.qsize(), dirQueue.qsize(), resultsQueue.qsize()))
            time.sleep(.1)
            
            if fileQueue.empty() and dirQueue.empty():
                queueMsg("\"%s\", \"%s\", \"%s\", \"%s\"\n"%(args.queueParams['max'], fileQueue.qsize(), dirQueue.qsize(), resultsQueue.qsize()))
                print "waiting for directory queue to clear..."
                dirQueue.join()
                print "waiting for file queue to clear..."
                fileQueue.join()
                print "waiting for worker processes to complete..."
                terminateThreads = True
                print "waiting for results queue to clear..."
                resultsQueue.join()
                print "exporting statistics..."
                exportStats()
                print "closing files..."
                for file in fileHandles:
                    fileHandles[file].close()
                print "cleaning up process files..."
                cleanup()
                exit(1)
    except KeyboardInterrupt:
        # Time to tell all the threads to bail out
        terminateThreads = True
        saveState('savedstate')
        exit(1)
