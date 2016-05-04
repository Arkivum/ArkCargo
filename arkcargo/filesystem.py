#!/usr/bin/env python
#
# ArkCargo - fsMetadata
#
# @author Chris Pates <chris.pates@arkivum.com>
# @copyright Arkivum Limited 2015


import os 


class fsMetadata:

    def __init__(self):
        self.dirPath = ""
        self.basePath = ""
        self.EOLchar = '\n'
        self.files = []
        return;

    @classmethod
    def initWithDir(cls, cargoFile):
        initMetadata.create(cargoFile)
        initMetadata.basePath =""
        initMetadata.init()
        return initMetadata;
    
    def setEOLchar(self, char):
        self.EOLchar = char
        return;

    def create(self, dirPath):
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

    def init(self):
        return;
