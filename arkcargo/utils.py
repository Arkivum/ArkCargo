#!/usr/bin/env python
#
# ArkCargo - Utils
#
# @author Chris Pates <chris.pates@arkivum.com>
# @copyright Arkivum Limited 2016
#
# provides ancillary services to all arkcargo 

import os 
import datetime
from multiprocessing import Process, Value, Lock

# provides tools to create and clean-up a work-in-progress (WIP) area
# used by all tools to provide consistency
#
class wipSpace:
    
    def __init__(self):
        self.parser = None 
        self.name = u''
        self.paths = []
        return;

    @classmethod
    def createWIP(cls, name, configFiles):
        createWIP = wipSpace()

        return;

    def loadConfigs(self, name, configFiles):
        self.name = name
        # Load configs
        self.config = argparse.ArgumentParser()
        return;

    def prepWIP(self, config):
        # Create working Directory
        

        return;

    def cleanupWIP(self):

        return;


# Read config files for the system, and job to be run
# separate contexts:
# config
# ======
# This loads the following configs, in sequence allowing for overloading
#      arkcargo.conf (shipped within the arkcargo package)
#      /etc/arkcargo.conf (system specific customisations)
#      ~/arkcargo.conf (user specific customisations)
# job
# ===
# This is populated can be populated either from:
#      separate <dataset>.<dataset> & <snapshot>.snapshot files
# or:
#      a single <dataset>-<snapshot>.job config. 
# The form suites where a snapshot covers multiple datasets, such as for
# for large scale rolling snapshots 
import configargparse

class config:

    def __init___(self):
        self.arkcargo = None
        self.job = None
        return;

    @classmethod
    def initJob(cls, jobConfig):
        initJob = config()

        return initJob;

    @classmethod
    def initDatasetSnapshot(cls, datasetConfig, snapshotConfig):
        initDatasetSnapshot = config()
        initDatasetSnapshot.loadConfig()
        initDatasetSnapshot.loadJob(jobDataset)
        initDatasetSnapshot.loadJob(jobSnapshot)

        return initDatasetSnapshot;

    def loadArkCargoConfig(self):
         conf = configargparse.ArgParser( default_config_files=[ os.path.join(os.path.dirname( os.path.realpath( __file__ )), 'arkcargo.conf'),'~/arkcargo.conf'])
         self.arkcargo = conf.parse_args()

# creates and maintains a logfile
#
class logFile:
    
    def __init__(self):
        self.levels = [u'NOTSET', u'DEBUG', u'INFO', u'WARNING', u'ERROR', u'CRITICAL']
        self.lock = Lock()
        self.level = 0
        self.name = u''
        self.path = u''
        self.timestamp = False
        self.fileHandle = None 
        return;

    @classmethod
    def createLog(cls, name, path, level):
        createLog = logFile()
        createLog.setLevel(level)
        createLog.open(name, path)
        return createLog;

    def setTimestamp(state):
        self.timestamp = state

    def setLevel(self, level):
        level = level.upper()
        self.level = self.levels.index(level);

    def log(self, level, msg):
        # Might not always be necessary but lets not chance it in a 
        # multithreading environment.
        with self.lock:
            if self.isEnabledFor(level):
                timestamp = datetime.datetime.today().strftime('%Y%m%dT%H%M%S')
                if self.timestamp:
                    self.fileHandle.write("%s [%s] %s\n"%(timestamp, level, msg))
                else:
                    self.fileHandle.write("[%s] %s\n"%(level, msg))
        return;

    def isEnabledFor(self, level):
        level = level.upper()
        return (self.levels.index(level) >= self.level);
        
    def getEffectiveLevel(self):
        return self.levels[self.level]; 

    def debug(self, msg):
        self.log('DEBUG', msg)
        return;
 
    def info(self, msg):
        self.log('INFO', msg)
        return;

    def warning(self, msg):
        self.log('WARNING', msg)
        return;

    def error(self, msg):
        self.log('ERROR', msg)
        return;

    def critical(self, msg):
        self.log('ERROR', msg)
        return;

    def open(self, name, path):
        if not self.fileHandle:
            self.name = name
            self.path = path
            try:
                # This is necessary for REALLY early 2.6.1 builds
                # where append mode doesn't create a file if it doesn't
                # already exist.
                mode = 'a' if os.path.exists(self.path) else 'w'
                self.fileHandle = open(self.path, mode, 0)
                self.info("Opening %s"%self.name)
            except ValueError:
                sys.stderr.write("can't open %s"%path)
                sys.exit(-1)
            return True;
        return False;

    def close(self):
        # Might not always be necessary but lets not chance it in a multithreading environment
        if self.fileHandle:
            self.info("Closing %s"%self.name)
            with self.lock:
                try:
                    self.fileHandle.close()
                except (IOError, OSError) as e:
                    sys.stderr.write("%s - %s"%(e, self.path))
                    return False;
                return True;
        return;

