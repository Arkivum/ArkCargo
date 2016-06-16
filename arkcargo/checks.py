#!/usr/bin/env python

import os, sys, re, argparse
import fs

def countlines(filename):
    linecount=0
    for line in open(filename, 'rb'):
        linecount = linecount + 1
    return linecount

def readConfig(path):
    regexDict = {
        'dataset' : re.compile("name='([a-zA-Z0-9_\-]*)['|,]"),
        'job' : re.compile("timestamp='(([0-9]{6,8})(T[0-9]{4,8})?)['|,]"),
        'mode' : re.compile("mode='(full|incr)',"),
        'snapshotCurrentPath' : re.compile(" snapshotCurrent='([a-zA-Z0-9_\/\ \-.]*)',")
        }

    if os.path.isfile(path):
        for i, line in enumerate(open(path)):
            configParams={}
            for parameter in regexDict.keys():
                param = re.search(regexDict[parameter], line)
                if param:
                    configParams[parameter] = param.group(1)
                else:
                    print "problem reading %s from %s"%(parameter, path)
                    exit(1)
    return configParams;

def childofIgnorePath(filepath, ignore):
    for category in ignore.keys():
        for path in ignore[category]:
            if filepath.startswith(path):
                return category;
    return None;

def checkPath(charMap, candidate, snapshotDir):
    absPath = os.path.abspath(os.path.join(snapshotDir, candidate))
    relPath = os.path.relpath(absPath, snapshotDir)
    parent = os.path.dirname(relPath)

    returnedCat = ""
    returnedPath = ""
    returnCat = ""
    returnPath = ""

    if len(candidate) == 0:
        returnCat = ''
    elif absPath != os.path.realpath(absPath):
        returnCat = 'symlink'
        returnPath = relPath
        returnedCat, returnedPath = checkPath(charMap, parent, snapshotDir)

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
        returnedCat, returnedPath = checkPath(charMap, parent, snapshotDir)

    elif not fs.exists(absPath):
        if not fs.hasSpecialChars(charMap, relPath):
            returnCat = 'failed.missing'
            returnPath = relPath
        else:
            returnCat = 'charset'
            returnPath = fs.subSpecialChars(charMap, relPath)
        returnedCat, returnedPath = checkPath(charMap, parent, snapshotDir)

    elif not os.access(absPath, os.R_OK):
        returnCat = 'failed.permissions'
        returnPath = relPath
        returnedCat, returnedPath = checkPath(charMap, parent, snapshotDir)

    if returnedCat not in ['', 'directory']:
        return(returnedCat, returnedPath)

    return (returnCat, returnPath);


def writeOutput(outPath, ignore, file, message, files):
    if ignore:
        outPath = os.path.join(outPath,"ignored")
        filename = "ignored.%s"%file
    else:
	filename = file
    filePath = os.path.join(outPath, file)

    if not os.path.exists(outPath):
        os.mkdir(outPath)

    if not filename in files.keys():
        try:
            # This is necessary for REALLY early 2.6.1 builds
            # where append mode doesn't create a file if it doesn't
            # already exist.
            mode = 'a' if os.path.exists(filePath) else 'w'
            files[filename] = open(filePath, mode, 0)
        except ValueError:
            sys.stderr.write("can't open %s"%file)
            sys.exit(-1)

    # write out the message to the end of the file
    try:
        files[filename].write("%s\n"%message)
    except:
        sys.stderr.write("Cannot write to %s\n"%filePath)
        sys.stderr.write("%s: %s\n"%(filename, message))
        exit(-1)
    return;

def processFailedFile(outputDir, snapshotDir, failedFile, specialChars, ignore):
    files = {}
    errorTotal = countlines(failedFile)
    
    for line in open(failedFile, 'rb'):
        processPath(outputDir, snapshotDir, line, specialChars, files, ignore)
    return;

def processCargodiffFile(outputDir, snapshotDir, cargodiffFile, specialChars, ignore):
    regexCargoDiff = re.compile('^([0-9a-fA-F]{32})(?:  )(.*)$')
    files = {}

    errorTotal = countlines(cargodiffFile)
    if errorTotal == 0:
        return;

    for line in open(cargodiffFile, 'rb'):
        match = regexCargoDiff.match(line.strip())
        if not match:
            return;

        md5checksum, testPath = match.groups()
        processPath(outputDir, snapshotDir, testPath, specialChars, files, ignore)
    return;


def processPath(outputDir, snapshotDir, line, specialChars, files, ignore):
    trailingChars = len(line) - len(line.strip())
    testPath = line.strip()
    for iteration in range(1, trailingChars):
        testPath += u"\xee"

    category = childofIgnorePath(testPath, ignore)
    if category:
       writeOutput(outputDir, True, category, testPath, files)
       return;
        
    category, path = checkPath(specialChars, testPath, snapshotDir)
    if category == 'rework':
        writeOutput(outputDir, False, category, path, files)
    elif (testPath != path):
        if path not in ignore[category]:
            ignore[category].append(path)
            if category == 'symlink':
                targetPath =os.path.relpath(os.path.realpath(os.path.join(snapshotDir, path)), snapshotDir)
                writeOutput(outputDir, False, category, "%s %s"%(path, targetPath), files)
            elif category in ['charset','directory']:
	        writeOutput(outputDir, False, 'rework', path, files)
            else:
                writeOutput(outputDir, False, category, path, files)
    else:
        writeOutput(outputDir, False, category, testPath, files)

def processCargoJob(outputDir, snapMetadataDir, cargodiffDir):
    files = {}
    ignore = {}
    ignore['failed.missing'] = []
    ignore['failed.permissions'] = []
    ignore['charset'] = []
    ignore['symlink'] = []
    ignore['directory'] = []

    specialChars = {}
    specialChars[':'] = '\xee'
    specialChars['?'] = '\xee'
    specialChars['*'] = '\xee'

    configPath = os.path.join(snapMetadataDir, 'config')
    if not os.path.isfile(configPath):
        print("%s - snapshot metadata not found, skipping"%configPath)
        return
    config = readConfig(configPath)
    dataset = config['dataset']
    job = config['job']

    regexPattern = "^(%s)-(%s)(?:-(\d{6}))?(?:\.md5)"%(dataset, job)
    regexCargo = re.compile(regexPattern)

    snapshotDir = config['snapshotCurrentPath']

    if os.path.isdir(os.path.join(snapMetadataDir, 'snapshot')):
        failedFile = os.path.join(snapMetadataDir, 'snapshot', 'failed')
    else: 
        failedFile = os.path.join(snapMetadataDir, 'failed')

    if os.path.isfile(failedFile):
        processedFile = os.path.join(outputDir, '.processed', 'failed')
        if not os.path.isfile(processedFile):
            print "processing - %s"%failedFile
            processFailedFile(outputDir, snapshotDir, failedFile, specialChars, ignore)
            fs.touch(processedFile)
        else:
            print "skipping - %s"%failedFile

    cargodiffList = [f for f in os.listdir(cargodiffDir) if re.match(regexCargo, f)]
    for file in cargodiffList:
        processedFile = os.path.join(outputDir, '.processed', file)
        cargodiffFile = os.path.join(cargodiffDir, file)
        if not os.path.isfile(processedFile):
            print "processing - %s"%cargodiffFile
            processCargodiffFile(outputDir, snapshotDir, cargodiffFile, specialChars, ignore)
            fs.touch(processedFile)
        else:
            print "skipping - %s"%cargodiffFile
    return;
