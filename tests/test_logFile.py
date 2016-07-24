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
from utils import logFile

class test_logFile(unittest.TestCase):
    def setUp(self):
        self.testname = ""
        self.logFile = logFile
        pass

    def tearDown(self):
        outputDir = os.path.join(os.getcwd(),'test_logFile/', self.testname, 'output/')
        if os.path.exists(outputDir):
           shutil.rmtree(outputDir)
           os.mkdir(outputDir)
        pass

    def test_logFile_create_bad(self):
        self.testname = sys._getframe().f_code.co_name
        self.assertRaises(IOError, self.logFile.createLog, 'information level', os.path.join(os.getcwd(),'test_logFile/', self.testname, 'output-broken/', 'info.log'), 'info')
        pass

    def test_logFile_setLevel(self):
        self.testname = sys._getframe().f_code.co_name
        expectedLevel = 'INFO'
        testLog = logFile()
        testLog.setLevel('Info')
        outputLevel = testLog.getEffectiveLevel()
        self.assertTrue(expectedLevel == expectedLevel, "response not expected")
        pass

    def test_logFile_isEnabledFor(self):
        self.testname = sys._getframe().f_code.co_name
        testLog = logFile()
        testLog.setLevel('Warning')
        self.assertTrue(testLog.isEnabledFor('Error') == True, "incorrect response")
        self.assertTrue(testLog.isEnabledFor('Warning') == True, "incorrect response")
        self.assertTrue(testLog.isEnabledFor('Debug') == False, "incorrect response")
        pass

    def test_logFile_create_info(self):
        self.testname = sys._getframe().f_code.co_name
        outputLog = os.path.join(os.getcwd(),'test_logFile/', self.testname, 'output', 'info.log')
        expectedLog = os.path.join(os.getcwd(),'test_logFile/', self.testname, 'expected', 'info.log')
        testLog = logFile()
        testLog.setLevel('Info')
        testLog.open('information level', outputLog)
        testLog.critical("Hello world?")
        testLog.close()
        self.assertTrue(filecmp.cmp(outputLog, expectedLog), "output and expected file, do not match")
        pass

unittest.main()
