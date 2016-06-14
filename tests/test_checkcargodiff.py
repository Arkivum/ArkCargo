#!/usr/bin/env python
#
#  stats  Identifying information about tests here.
#
#===============
#  This is based on a skeleton test file, more information at:
#
#     https://github.com/linsomniac/python-unittest-skeleton


import unittest2 as unittest

import os, sys, filecmp, shutil
sys.path.append('../arkcargo')
from checks import checkPath, processFailedFile, processCargoJob

class test_checkCargoDiff(unittest.TestCase):
    def setUp(self):
        self.testname=""

        self.specialChars = {}
        self.specialChars[':'] = '\xee'   

        self.basePath = os.path.join(os.getcwd(), 'datasets/snapshots/test-20160101T0400/')

        configFileContent = "Namespace(cargo=True, cargoEOL='', cargoExt='.md5', cargoMax='10GB', cargoMaxBytes=10000000000, cargoPad=6, clean=False, cleanupFiles=['.running', 'processed.files', 'processed.dirs', 'savedstate.files', 'savedstate.dirs', 'resumestate.files', 'resumestate.dirs'], debug=False, executablePath='%s', file='', filebase='%s/test_checkcargodiff/snapshot-metadata/test/20160101T0400', followSymlink=False, includeStats={'cargo': [], 'snapshot': ['added', 'modified', 'unchanged', 'symlink', 'directory', 'removed', 'failed'], 'ingest': ['added', 'modified']}, lastRunPad=4, lastRunPrefix='run', mode='full', name='test', outFiles={'valid': ['processed.files', 'processed.dirs', 'savedstate.files', 'savedstate.dirs', 'resumestate.files', 'resumestate.dirs', 'error.log', 'debug.log', 'failed', 'added', 'modified', 'unchanged', 'symlink', 'directory', 'config', 'cargo', 'removed', 'queue.csv']}, output='%s/test_checkcargodiff/snapshot-metadata', prepMode='preserve', processedFile='', queueParams={'highWater': 9000, 'max': 100000, 'lowWater': 100}, relPathInCargos=False, resume=False, rework=None, rootDir=['processed', 'savedstate.files', 'savedstate.dirs', 'resumestate.files', 'resumestate.dirs', 'error.log', 'debug.log', 'config'], savedState=False, snapshotCurrent='%s/datasets/snapshots/test-20160101T0400/', snapshotDir=['failed', 'added', 'modified', 'unchanged', 'symlink', 'directory', 'removed'], snapshotEOL='', snapshots=['%s/datasets/snapshots/test-20160101T0400/'], startTime=datetime.datetime(2016, 5, 24, 23, 50, 32, 438800), statsBoundaries='%s/boundaries.csv', statsDir=['snapshot.csv', 'ingest.csv', 'cargo.csv'], sys_uname=('Darwin', 'Banoffee.local', '15.5.0', 'Darwin Kernel Version 15.5.0: Tue Apr 19 18:36:36 PDT 2016; root:xnu-3248.50.21~8/RELEASE_X86_64', 'x86_64', 'i386'), terminalPath='/Users/chrispates/GitHub/checkfailed/test', threads=8, timestamp='20160101T0400')"%(os.path.abspath('../arkcargo'), os.getcwd(), os.getcwd(), os.getcwd(), os.getcwd(), os.path.abspath('../arkcargo'))
        
        configFile = os.path.join(os.getcwd(), 'test_checkcargodiff/snapshot-metadata/test/20160101T0400/config')
        try:
            file = open(configFile, 'w', 0)
            file.write(configFileContent)
            file.close
        except ValueError:
            sys.stderr.write("can't open %s"%file)
            sys.exit(-1)
        pass

    def tearDown(self):
        testname = self.testname.split(self.__class__.__name__+'_',1)[1]
        outputDir = './test_checkcargodiff/%s/output'%testname
        if os.path.exists(outputDir):
           shutil.rmtree(outputDir)
           os.mkdir(outputDir)
        pass

    def test_checkCargoDiff_childofsymlink(self):
        self.testname = sys._getframe().f_code.co_name

        testPath ='sym2dir/sameinboth.txt'
        category, path = checkPath(self.specialChars, testPath, self.basePath)
        self.assertTrue(category == 'symlink' and path == 'sym2dir', "category = %s, %s, %s"%(category,testPath, path))
        pass

    def test_checkCargoDiff_symlinktofile(self):
        self.testname = sys._getframe().f_code.co_name

        testPath ='sameinboth/onlyinA/sym2file'
        category, path = checkPath(self.specialChars, testPath, self.basePath)
        self.assertTrue(category == 'symlink' and path == 'sameinboth/onlyinA/sym2file', "category = %s, %s, %s"%(category,testPath, path))
        pass

    def test_checkCargoDiff_missingdirtree(self):
        self.testname = sys._getframe().f_code.co_name

        testPath = 'missing/sameinboth/onlyinA.txt'
        category, path = checkPath(self.specialChars, testPath, self.basePath)
        self.assertTrue(category == 'failed.missing' and path == 'missing', "category = %s, %s, %s"%(category,testPath, path))
        pass

    def test_checkCargoDiff_regular_file(self):
        self.testname = sys._getframe().f_code.co_name
        testPath = 'sameinboth/onlyinA.txt'

        category, path = checkPath(self.specialChars, testPath, self.basePath)
        self.assertTrue(category == 'rework' and path == testPath, "category = %s, %s, %s"%(category,testPath, path))
        pass

    def test_checkCargoDiff_missing_file(self):
        self.testname = sys._getframe().f_code.co_name
        testPath = 'sameinboth/missing'

        category, path = checkPath(self.specialChars, testPath, self.basePath)
        self.assertTrue(category == 'failed.missing' and path == testPath, "category = %s, %s, %s"%(category,testPath, path))
        pass

    def test_checkCargoDiff_permission_file(self):
        self.testname = sys._getframe().f_code.co_name
        testPath = 'root.txt'

        category, path = checkPath(self.specialChars, testPath, self.basePath)
        self.assertTrue(category == 'failed.permissions' and path == testPath, "category = %s, %s, %s"%(category,testPath, path))
        pass

    def test_checkCargoDiff_permission_dirtree(self):
        self.testname = sys._getframe().f_code.co_name
        testPath = 'root/onlyinA.txt'

        category, path = checkPath(self.specialChars, testPath, self.basePath)
        self.assertTrue(category == 'failed.permissions' and path == 'root', "category = %s, %s, %s"%(category,testPath, path))
        pass

    def test_checkCargoDiff_processFailedFile(self):
        self.testname = sys._getframe().f_code.co_name

        ignore = {}
        ignore['failed.missing'] = []
        ignore['failed.permissions'] = []
        ignore['charset'] = []
        ignore['symlink'] = []

        failedFile = "test_checkcargodiff/snapshot-metadata/test/20160101T0400/snapshot/failed"
        snapshotDir = "datasets/snapshots/test-20160101T0400/"
        outputDir = "test_checkcargodiff/%s/output"%self.testname
        expectedDir = "test_checkcargodiff/%s/expected"%self.testname
        processFailedFile(outputDir, snapshotDir, failedFile, self.specialChars, ignore)
        pass

    def test_checkCargoDiff_processCargoJobs(self):
        self.testname = sys._getframe().f_code.co_name
        snapMetadata = "test_checkcargodiff/snapshot-metadata/test/20160101T0400/"
	cargodiffs = "test_checkcargodiff/%s/cargo-diff/"%self.testname
        outputDir = "test_checkcargodiff/%s/output"%self.testname
        expectedDir = "test_checkcargodiff/%s/expected"%self.testname

        processCargoJob(outputDir, snapMetadata, cargodiffs)
        outputList = os.listdir(outputDir)
        expectedList = os.listdir(expectedDir)
        self.assertTrue(outputList == expectedList, "listed of output files does not match expected")
        for file in expectedList:
             outputFile = os.path.join(outputDir, file)
             expectedFile = os.path.join(expectedDir, file)
             self.assertTrue(filecmp.dircmp(outputDir, expectedDir), "output does not match expected - %s"%file)
        pass

unittest.main()
     

