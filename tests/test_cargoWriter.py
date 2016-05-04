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
from cargo import cargoWriter


class test_cargoWriter(unittest.TestCase):
    def setUp(self):
        self.testname =""
        self.cargoWriter = cargoWriter()
        pass

    def tearDown(self):
        testname = self.testname.split(self.__class__.__name__+'_',1)[1]
        outputDir = os.path.join(os.getcwd(),'test_cargoWriter/', testname, 'output/')
        if os.path.exists(outputDir):
           shutil.rmtree(outputDir)
           os.mkdir(outputDir)
        pass

    def test_cargoWriter_create_bad(self):
        self.testname = sys._getframe().f_code.co_name
        self.assertRaises(IOError, self.cargoWriter.createAbsolute, os.path.join(os.getcwd(),'test_cargoWriter/', self.testname, 'output-broken/', 'cargo.md5'))
        pass

    def test_cargoWriter_addEntry(self):
        self.testname = sys._getframe().f_code.co_name
        assertCargoFile = os.path.join(os.getcwd(), './test_cargoWriter/', self.testname, 'expected/cargo.md5')
        outputCargoFile = os.path.join(os.getcwd(), './test_cargoWriter/', self.testname, 'output/cargo.md5')
   
        self.cargoWriter.createRelative(outputCargoFile, os.getcwd())
        self.cargoWriter.addEntry(os.path.join('./datasets/snapshots/test-20160101T0400/onlyinA.txt'))
        self.cargoWriter.close()

        self.assertTrue(filecmp.cmp(outputCargoFile, assertCargoFile), "output and expected file, do not match")
        pass

    def test_cargoWriter_addEntries(self):
        self.testname = sys._getframe().f_code.co_name
        assertCargoFile = os.path.join(os.getcwd(), 'test_cargoWriter/', self.testname, 'expected/cargo.md5')
        outputCargoFile = os.path.join(os.getcwd(),'test_cargoWriter/', self.testname, 'output/cargo.md5')

        self.cargoWriter.createRelative(outputCargoFile, os.getcwd())
        self.cargoWriter.addEntry(os.path.join('datasets/snapshots/test-20160101T0400/onlyinA.txt'))
        self.cargoWriter.addEntry(os.path.join('datasets/snapshots/test-20160101T0400/sameinboth.txt'))
        self.cargoWriter.addEntry(os.path.join('datasets/snapshots/test-20160101T0400/changedfroma2b.txt'))
        self.cargoWriter.close()

        self.assertTrue(filecmp.cmp(outputCargoFile, assertCargoFile), "output and expected file, do not match")
        pass

unittest.main()
