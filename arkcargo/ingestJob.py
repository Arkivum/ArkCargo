#!/usr/bin/env python
#
# ArkCargo - chunkset
#            describes the files, directories and links 
#            within a filesystem in preperation for
#            ingest
#            - chunk (encapsulates output relating to a 
#              predetermined amount of filesystem
#            - chunkId (for consistent numbering of chunks)
#
# @author Chris Pates <chris.pates@arkivum.com>
# @copyright Arkivum Limited 2015


from multiprocessing import Process, Value, Lock

class chunkId(object):
    def __init__(self, initval=0):
        self.val = Value('i', initval)
        self.lock = Lock()

    def increment(self):
        with self.lock:
            self.val.value += 1

    def value(self):
        with self.lock:
            return self.val.value

    def getNext(self):
        with self.lock:
            self.val.value += 1
            return self.val.value


class ingestChunk:

    def __init__(self):
        self.boundaries = boundaries()
        self.metadata = fsMetadata()
        self.cargoFile = cargoWriter()
        self.wipPath = u""
        self.dataset = u""
        self.timestamp = u""
        return;

    @classmethod
    def initIngestChunk(cls, path, name, timestamp, boundaryFile):
        initIngestChunk = ingestChunk()
        initIngestChunk.wipPath = path
        initIngestChunk.dataset = name
        initIngestChunk.timestamp = timestamp
        initIngestChunk.init()
        return initIngestChunk;
        

