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
from fs import fsMetadata


class test_fsMetadata(unittest.TestCase):
    def setUp(self):
        self.testname =""
        self.fsMetadata = fsMetadata()
        pass

    def tearDown(self):
        testname = self.testname.split(self.__class__.__name__+'_',1)[1]
        outputDir = os.path.join(os.getcwd(),'test_fsMetadata/', testname, 'output/')
        if os.path.exists(outputDir):
           shutil.rmtree(outputDir)
           os.mkdir(outputDir)
        pass

    def test_fsMetadata_create_bad(self):
        self.testname = sys._getframe().f_code.co_name
        self.assertRaises(IOError, self.fsMetadata.create, os.path.join(os.getcwd(),'test_fsMetadata/', self.testname, 'output-broken/', 'cargo.md5'))
        pass


unittest.main()
