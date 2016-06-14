#!/usr/bin/env python
#
# ArkCargo - fs
#	- fsMetadata
#	- fs
#
# @author Chris Pates <chris.pates@arkivum.com>
# @copyright Arkivum Limited 2015


import os 

def touch(path):
    parent = os.path.dirname(path)
    if not os.path.isdir(parent):
       os.makedirs(parent)
    with open(path, 'a'):
        os.utime(path, None)

def exists(path):
    if os.path.lexists(path) or os.path.exists(path):
        status = True
    else:
        status = False
    return status;

def isLink(path):
    if os.path.lexists(path) and (not os.path.exists(path)):
        #invalid link
        status = True
    elif os.path.isdir(path) and os.path.islink(path):
        #valid link to directory
        status = True
    elif os.path.isfile(path) and os.path.islink(path):
        #valid link to file
        status = True
    else: 
        status = False
    return status;

def isLinkValid(path):
    if os.path.isdir(path) and os.path.islink(path):
        #valid link to directory
        status = True
    elif os.path.isfile(path) and os.path.islink(path):
        #valid link to file
        status = True
    else:
        status = False
    return status;    

def isLinkInvalid(path):
    if os.path.lexists(path) and (not os.path.exists(path)):
        #invalid link
        status = True
    else:
        status = False
    return status;

def isLink2dir(path):
    if os.path.isdir(path) and os.path.islink(path):
        #valid link to directory
        status = True
    else:
        status = False
    return status;
       
def isLink2file(path):
    if os.path.isfile(path) and os.path.islink(path):
        #is a link that points to a file
        status = True
    else:
        status = False
    return status;

def isDir(path):
    if os.path.isdir(path):
        #the path or link points to a directory
        status = True
    else:
        status = False
    return status;

def isDirReg(path):
    if os.path.isdir(path) and not os.path.islink(path):
        #is a directory and not a link to
        status = True
    else:
        status = False
    return status;

def isLeafDir(path):
    if os.path.isdir(path) and not os.path.islink(path):
        childList = os.listdir(path)
        for child in childList:
            if isDirReg(os.path.join(path, child)):
                return False;
        return True;
    return False;

def isFileReg(path):
    if os.path.isfile(path) and not os.path.islink(path):
        #is a file and not a link to a file
        status = True
    else:
        status = False
    return status;

def isFile(path):
    #the path or link points to a file
    return os.path.isfile(path);

def hasSpecialChars(charMap, path):
    return any(specialChar in path for specialChar in charMap.keys());
        
def subSpecialChars(charMap, path):
    parent, file = os.path.split(path)
    if any(specialChar in parent for specialChar in charMap.keys()):
        return subSpecialChars(charMap, parent)
    else:
        
        for specialChar in charMap.keys():
             file = file.replace(specialChar, charMap[specialChar])
        return os.path.join(parent, file);

def classify(path, followSymlinks):
    classification = ''
    if not exists(path):
        classification = 'broken'
    elif not followSymlinks:
        if isLink(path):
            classification = 'symlink'
        elif isDirReg(path):
            classification = 'directory'
        elif isFileReg(path):
            classification = 'file'
    else:
        if not exists(path):
            classification = 'broken'
        elif isDir(path):
            classification = 'directory'
        elif isFile(path):
            classification = 'file'
        elif isLinkInvalid(path):
            classification = 'broken'
    return classification; 

class fsMetadata:

    def __init__(self):
        self.dirPath = ""
        self.basePath = ""
        self.EOLchar = '\n'
        self.categories = ['added', 'modified', 'unchanged', 'symlink', 'directory', 'removed', 'failed']
        self.files = []
        return;

    @classmethod
    def initWithDir(cls, name, basePath):
        initMetadata.setName(name)
        initMetadata.setDir(basePath)
        initMetadata.init()
        return initMetadata;

    def setName(name):
        self.name = name
        return;

    def setDir(path):
        if os.path.isdir(path):
           self.basePath = path
        return;
    
    def setEOLchar(self, char):
        self.EOLchar = char
        return;

    def addEntry(category, entry):
        if not category in self.files:
            filePath = os.path.join(self.basePath, self.name + category)
            try:
                # This is necessary for REALLY early 2.6.1 builds
                # where append mode doesn't create a file if it doesn't
                # already exist.
                mode = 'a' if os.path.exists(filePath) else 'w'
                self.files[category] = open(filePath, mode, 0)
            except ValueError:
                sys.stderr.write("can't open %s"%filePath)
                sys.exit(-1)
                # write out the message to the end of the file
        try:
            (self.files[category]).write(entry + self.EOLchar)
        except:
            sys.stderr.write("Cannot write to %s\n"%filePath)
            sys.stderr.write("%s: %s\n"%(category, entry))
            exit(-1)
        return;

    def close(self):
        try:
            for file in self.files:
               (self.files[file]).close()
        except ValueError:
            sys.stderr.write("can't close %s.%s"%(self.chunkNu, file))
            sys.exit(-1)
        return;

    def init(self):
        return;
