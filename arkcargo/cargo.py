#!/usr/bin/env python
#
# ArkCargo - MD5 deep format cargo
#
# @author Chris Pates <chris.pates@arkivum.com>
# @copyright Arkivum Limited 2015


import os 
import hashlib

def filename(dataset, timestamp, chunkId):
    filename = dataset + "-" +  timestamp + "-" + str(chunkId).zfill(cargoPad) + ".md5"
    return filename;


def getMD5(path):
    hash = hashlib.md5()
    blocksize=65536

    # the following means of calculating an MD5 checksum for a file
    # is based on code in https://github.com/joswr1ght/md5deep/blob/master/md5deep.py
    # also under MIT license.
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(blocksize), ""):
            hash.update(block)
        hash = hash.hexdigest().replace(" ","")

    return hash;


class cargoWriter:

    def __init__(self):
        self.filePath = ""
        self.basePath = ""
        self.EOLchar = '\0'
        self.file = None
        return;

    @classmethod
    def initWithPath(cls, cargoFile):
        initCargo.create(cargoFile)
        initCargo.basePath =""
        initCargo.init()
        return initCargo;
    
    def setAbsolutePaths(self, state):
        self.AbsolutePaths = state
        return;

    def setEOLchar(self, char):
        self.EOLchar = char
        return;

    def createAbsolute(self, cargoFile):
        try:
            self.filePath = cargoFile
            self.file = open(cargoFile, "w", 0)
        except ValueError:
            sys.stderr.write("can't open %s"%filePath)
            sys.exit(-1)
        return;

    def createRelative(self, cargoFile, basePath):
        self.basePath = basePath

        try:
            self.filePath = cargoFile
            self.file = open(cargoFile, "w", 0)
        except ValueError:
            sys.stderr.write("can't open %s"%filePath)
            sys.exit(-1)
        return;

    def close(self):
        try:
            if self.file:
               (self.file).close()
        except ValueError:
            sys.stderr.write("can't close %s"%self.filePath)
            sys.exit(-1)
        return;

    def addEntry(self, filePath):
        if self.basePath == "":
            abspath = filePath
        else:
            abspath = os.path.abspath(os.path.join(self.basePath, filePath))
     
        (self.file).write("%s  %s%s"%(getMD5(abspath), filePath, self.EOLchar))
        return;

    def init(self):
        return;
