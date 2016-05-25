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
from checks import checkPath, processCargoDiff

class test_checkCargoDiff(unittest.TestCase):
    def setUp(self):
        self.testname=""
        
        pass

    def tearDown(self):
        testname = self.testname.split(self.__class__.__name__+'_',1)[1]
        outputDir = './test_checkcargodiff/output'
        if os.path.exists(outputDir):
           shutil.rmtree(outputDir)
           os.mkdir(outputDir)
        pass

    def test_checkCargoDiff_childofsymlink(self):
        self.testname = sys._getframe().f_code.co_name
        basePath = './test_checkcargodiff/snapshot_tree/'

        testPath ='dir_0/dir_1/sym2dir/emptyfile'
        category, path = checkPath(testPath, basePath)
        self.assertTrue(category == 'symlink' and path == 'dir_0/dir_1/sym2dir', "category = %s, %s, %s"%(category,testPath, path))
        pass

    def test_checkCargoDiff_symlinktofile(self):
        self.testname = sys._getframe().f_code.co_name
        basePath = './test_checkcargodiff/snapshot_tree/'

        testPath ='dir_0/sym2file'
        category, path = checkPath(testPath, basePath)
        self.assertTrue(category == 'symlink' and path == 'dir_0/sym2file', "category = %s, %s, %s"%(category,testPath, path))
        pass

    def test_checkCargoDiff_missingdirtree(self):
        self.testname = sys._getframe().f_code.co_name
        basePath = './test_checkcargodiff/snapshot_tree/'

        testPath = 'dir_a/dir_b/extfile'
        category, path = checkPath(testPath, basePath)
        self.assertTrue(category == 'failed.missing' and path == 'dir_a', "category = %s, %s, %s"%(category,testPath, path))
        pass

    def test_checkCargoDiff_regularfile(self):
        self.testname = sys._getframe().f_code.co_name
        basePath = './test_checkcargodiff/snapshot_tree/'
        testPath = 'dir_0/dir_1/readme.txt'

        category, path = checkPath(testPath, basePath)
        self.assertTrue(category == 'rework' and path == testPath, "category = %s, %s, %s"%(category,testPath, path))
        pass

    def test_checkCargoDiff_missingfile(self):
        self.testname = sys._getframe().f_code.co_name
        basePath = './test_checkcargodiff/snapshot_tree/'
        testPath = 'dir_0/broken'

        category, path = checkPath(testPath, basePath)
        self.assertTrue(category == 'failed.missing' and path == testPath, "category = %s, %s, %s"%(category,testPath, path))
        pass

    def test_checkCargoDiff_processListofPaths(self):
        self.testname = sys._getframe().f_code.co_name
        snapshotDir = './datasets/snapshots/test-20160101T0400/'
	cargodiffs = './test_checkcargodiff/cargdiffs/'
        outputDir = './test_checkcargodiff/output'
        expectedDir = './test_checkcargodiff/expected'

        processCargoDiff(outputDir, snapshotDir, cargoDiffFile)
        outputList = os.listdir(outputDir)
        expectedList = os.listdir(expectedDir)
        self.assertTrue(outputList == expectedList, "listed of output files does not match expected")
        for file in expectedList:
             outputFile = os.path.join(outputDir, file)
             expectedFile = os.path.join(expectedDir, file)
             self.assertTrue(filecmp.cmp(outputFile, expectedFile), "output instance of %s does not match expected"%file)
        
        pass

unittest.main()
     

