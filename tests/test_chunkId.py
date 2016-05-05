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
from ingestJob import chunkId


class test_chunkId(unittest.TestCase):
    def setUp(self):
        self.testname =""
        self.chunkId = chunkId()
        pass

    def tearDown(self):
        testname = self.testname.split(self.__class__.__name__+'_',1)[1]
        outputDir = os.path.join(os.getcwd(),'test_chunkId/', testname, 'output/')
        if os.path.exists(outputDir):
           shutil.rmtree(outputDir)
           os.mkdir(outputDir)
        pass

    def test_chunkId_create(self):
        self.testname = sys._getframe().f_code.co_name

        pass


unittest.main()
