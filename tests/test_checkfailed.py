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
from checks import checkPath, processListofPaths

class test_checkFailed(unittest.TestCase):
    def setUp(self):
        self.testname=""
        self.specialChars = {}
        self.specialChars[':'] = '\xee'

        pass

    def tearDown(self):
        testname = self.testname.split(self.__class__.__name__+'_',1)[1]
        outputDir = './test_checkfailed/output'
        if os.path.exists(outputDir):
           shutil.rmtree(outputDir)
           os.mkdir(outputDir)
        pass

    def test_checkFailed_childofsymlink(self):
        self.testname = sys._getframe().f_code.co_name
        basePath = './test_checkfailed/snapshot_tree/'

        testPath ='dir_0/dir_1/sym2dir/emptyfile'
        category, path = checkPath(self.specialChars, testPath, basePath)
        self.assertTrue(category == 'symlink' and path == 'dir_0/dir_1/sym2dir', "category = %s, %s, %s"%(category,testPath, path))
        pass

    def test_checkFailed_symlinktofile(self):
        self.testname = sys._getframe().f_code.co_name
        basePath = './test_checkfailed/snapshot_tree/'

        testPath ='dir_0/sym2file'
        category, path = checkPath(self.specialChars, testPath, basePath)
        self.assertTrue(category == 'symlink' and path == 'dir_0/sym2file', "category = %s, %s, %s"%(category,testPath, path))
        pass

    def test_checkFailed_missingdirtree(self):
        self.testname = sys._getframe().f_code.co_name
        basePath = './test_checkfailed/snapshot_tree/'

        testPath = 'dir_a/dir_b/extfile'
        category, path = checkPath(self.specialChars, testPath, basePath)
        self.assertTrue(category == 'failed.missing' and path == 'dir_a', "category = %s, %s, %s"%(category,testPath, path))
        pass

    def test_checkFailed_regularfile(self):
        self.testname = sys._getframe().f_code.co_name
        basePath = './test_checkfailed/snapshot_tree/'
        testPath = 'dir_0/dir_1/readme.txt'

        category, path = checkPath(self.specialChars, testPath, basePath)
        self.assertTrue(category == 'rework' and path == testPath, "category = %s, %s, %s"%(category,testPath, path))
        pass

    def test_checkFailed_missingfile(self):
        self.testname = sys._getframe().f_code.co_name
        basePath = './test_checkfailed/snapshot_tree/'
        testPath = 'dir_0/broken'

        category, path = checkPath(self.specialChars, testPath, basePath)
        self.assertTrue(category == 'failed.missing' and path == testPath, "category = %s, %s, %s"%(category,testPath, path))
        pass

    def test_checkFailed_processListofPaths(self):
        self.testname = sys._getframe().f_code.co_name
        snapshotDir = './test_checkfailed/snapshot_tree/'
	failedFile = './test_checkfailed/test/20160101T0000/snapshot/failed'
        outputDir = './test_checkfailed/output'
        expectedDir = './test_checkfailed/expected'

        processListofPaths(outputDir, snapshotDir, failedFile)
        outputList = os.listdir(outputDir)
        expectedList = os.listdir(expectedDir)
        self.assertTrue(outputList == expectedList, "listed of output files does not match expected")
        for file in expectedList:
             outputFile = os.path.join(outputDir, file)
             expectedFile = os.path.join(expectedDir, file)
             self.assertTrue(filecmp.cmp(outputFile, expectedFile), "output instance of %s does not match expected"%file)
        
        pass

unittest.main()
     

