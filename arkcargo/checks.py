#!/usr/bin/env python

import os, sys, re, argparse
import fs

def countlines(filename):
    linecount=0
    for line in open(filename, 'rb'):
        linecount += 1
    return linecount

def readConfig(path):
    regexDict = {
        'dataset' : re.compile("name='([a-zA-Z0-9_\-]*)['|,]"),
        'job' : re.compile("timestamp='(([0-9]{6,8})(T[0-9]{4,8})?)['|,]"),
        'mode' : re.compile("mode='(full|incr)',"),
        'snapshotCurrentPath' : re.compile(" snapshotCurrent='([a-zA-Z0-9_\/\-.]*)',")
        }

    if os.path.isfile(path):
        for i, line in enumerate(open(path)):
            configParams={}
            for parameter in regexDict.keys():
                param = re.search(regexDict[parameter], line)
                if param:
                    configParams[parameter] = param.group(1)
                else:
                    print "problem reading %s from %s"%(parmeter, path)
                    exit(1)
    return configParams;

def childofIgnorePath(filepath, ignore):
    for category in ignore.keys():
        for path in ignore[category]:
            if filepath.startswith(path):
                return category;
    return None;

def checkPath(candidate, snapshotDir):
    absPath = os.path.abspath(os.path.join(snapshotDir, candidate))
    relPath = os.path.relpath(absPath, snapshotDir)
    parent = os.path.dirname(relPath)
    returnedCat = ""
    returnedPath = ""
    returnCat = ""
    returnPath = ""

    if snapshotDir == parent:
        print "hit bottom -  something is seriously wrong!"
        exit (1)

    elif absPath != os.path.realpath(absPath):
        returnCat = 'symlink'
        returnPath = relPath
        returnedCat, returnedPath = checkPath(parent, snapshotDir)

    elif fs.isDirReg(absPath):
        if os.access(absPath, os.R_OK):
            returnCat = 'directory'
            returnPath = relPath
        else:
            returnCat = 'failed.permissions'
            returnPath = relPath

    elif fs.isFileReg(absPath):
        if os.access(absPath, os.R_OK):
            returnCat = 'rework'
            returnPath = relPath
        else:
            returnCat = 'failed.permissions'
            returnPath = relPath
        returnedCat, returnedPath = checkPath(parent, snapshotDir)

    elif not fs.exists(absPath):
        returnCat = 'failed.missing'
        returnPath = relPath
        returnedCat, returnedPath = checkPath(parent, snapshotDir)

    elif not os.access(absPath, os.R_OK):
        returnCat = 'failed.permissions'
        returnPath = relPath
        returnedCat, returnedPath = checkPath(parent, snapshotDir)

    if returnedCat not in ['', 'directory']:
        returnCat = returnedCat
        returnPath = returnedPath

    return (returnCat, returnPath);


def writeOutput(outPath, file, message, files):
    filePath = os.path.join(outPath, file)

    if not file in files.keys():
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
        files[file].write("%s\n"%message)
    except:
        sys.stderr.write("Cannot write to %s\n"%file)
        sys.stderr.write("%s: %s\n"%(file, message))
        exit(-1)
    return;

def processListofPaths(outputDir, snapshotDir, listFile):
    files = {}
    ignore = {}
    ignore['failed.missing'] = []
    ignore['symlink'] = []

    errorTotal = countlines(listFile)
    errorNu = 1

    print "snapshot dir: %s"%snapshotDir
    for line in open(listFile, 'rb'):
        testPath = line.strip()
        category, path = checkPath(testPath, snapshotDir)
        if category == 'rework':
            writeOutput(outputDir, category, path, files)
        elif (testPath != path):
            if path not in ignore[category]:
                ignore[category].append(path)
                if category == 'symlink':
                    targetPath =os.path.relpath(os.path.realpath(os.path.join(snapshotDir, path)), snapshotDir)
	            writeOutput(outputDir, category, "%s %s"%(path, targetPath), files)
                else:
                    writeOutput(outputDir, category, path, files)
            writeOutput(outputDir, "ignored.%s"%category, testPath, files)
        print '%d of %d - [%s] %s\r'%(errorNu, errorTotal, category, path)
        errorNu += 1

def processCargoDiff(outputDir, snapshotDir, cargodiffFile):
    files = {}
    ignore = {}
    ignore['failed.missing'] = []
    ignore['symlink'] = []

    errorTotal = countlines(cargodiffFile)
    errorNu = 1

    print "snapshot dir: %s"%snapshotDir
    for line in open(cargodiffFile, 'rb'):
        regexCargoDiff = re.compile('^([0-9a-fA-F]{32})(?:  )(.*)$')

        match = regexCargoDiff.match(line.strip())
        md5checksum, testPath = match.groups()

        category, path = checkPath(testPath, snapshotDir)
        if category == 'rework':
            writeOutput(outputDir, category, path, files)
        elif (testPath != path):
            if path not in ignore[category]:
                ignore[category].append(path)
                if category == 'symlink':
                    targetPath =os.path.relpath(os.path.realpath(os.path.join(snapshotDir, path)), snapshotDir)
	            writeOutput(outputDir, category, "%s %s"%(path, targetPath), files)
                else:
                    writeOutput(outputDir, category, path, files)
            writeOutput(outputDir, "ignored.%s"%category, testPath, files)
        print '%d of %d - [%s] %s\r'%(errorNu, errorTotal, category, path)
        errorNu += 1

