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
import fs


class test_fs(unittest.TestCase):
    def setUp(self):
        self.testname =""
        pass

    def tearDown(self):
        testname = self.testname.split(self.__class__.__name__+'_',1)[1]
        pass

    def test_fs_exists(self):
        self.testname = sys._getframe().f_code.co_name
        self.assertTrue(fs.exists(os.path.join(os.getcwd(), 'test_fs', 'file-valid')), "is not a link")
        self.assertTrue(fs.exists(os.path.join(os.getcwd(), 'test_fs', 'file-invalid')), "is not a link")
        self.assertTrue(fs.exists(os.path.join(os.getcwd(), 'test_fs', 'dir-valid')), "is not a link")
        self.assertTrue(fs.exists(os.path.join(os.getcwd(), 'test_fs', 'dir-invalid')), "is not a link")
        self.assertTrue(fs.exists(os.path.join(os.getcwd(), 'test_fs', 'readme.txt')), "is not a link")
        self.assertTrue(fs.exists(os.path.join(os.getcwd(), 'test_fs', 'directory')), "is not a link")
        self.assertFalse(fs.exists(os.path.join(os.getcwd(), 'test_fs', 'missing')), "is not a link")
        pass

    def test_fs_isLink(self):
        self.testname = sys._getframe().f_code.co_name
        self.assertTrue(fs.isLink(os.path.join(os.getcwd(), 'test_fs', 'file-valid')), "is not a link")
        self.assertTrue(fs.isLink(os.path.join(os.getcwd(), 'test_fs', 'file-invalid')), "is not a link")
        self.assertTrue(fs.isLink(os.path.join(os.getcwd(), 'test_fs', 'dir-valid')), "is not a link")
        self.assertTrue(fs.isLink(os.path.join(os.getcwd(), 'test_fs', 'dir-invalid')), "is not a link")
        self.assertFalse(fs.isLink(os.path.join(os.getcwd(), 'test_fs', 'readme.txt')), "is not a link")
        self.assertFalse(fs.isLink(os.path.join(os.getcwd(), 'test_fs', 'directory')), "is not a link")
        self.assertFalse(fs.isLink(os.path.join(os.getcwd(), 'test_fs', 'missing')), "is not a link")
        pass

    def test_fs_isLinkInvalid(self):
        self.testname = sys._getframe().f_code.co_name
        self.assertFalse(fs.isLinkInvalid(os.path.join(os.getcwd(), 'test_fs', 'file-valid')), "is not a link")
        self.assertTrue(fs.isLinkInvalid(os.path.join(os.getcwd(), 'test_fs', 'file-invalid')), "is not a link")
        self.assertFalse(fs.isLinkInvalid(os.path.join(os.getcwd(), 'test_fs', 'dir-valid')), "is not a link")
        self.assertTrue(fs.isLinkInvalid(os.path.join(os.getcwd(), 'test_fs', 'dir-invalid')), "is not a link")
        self.assertFalse(fs.isLinkInvalid(os.path.join(os.getcwd(), 'test_fs', 'readme.txt')), "is not a link")
        self.assertFalse(fs.isLinkInvalid(os.path.join(os.getcwd(), 'test_fs', 'directory')), "is not a link")
        self.assertFalse(fs.isLinkInvalid(os.path.join(os.getcwd(), 'test_fs', 'missing')), "is not a link")
        pass

    def test_fs_isLinkValid(self):
        self.testname = sys._getframe().f_code.co_name
        self.assertTrue(fs.isLinkValid(os.path.join(os.getcwd(), 'test_fs', 'file-valid')), "is not a link")
        self.assertFalse(fs.isLinkValid(os.path.join(os.getcwd(), 'test_fs', 'file-invalid')), "is not a link")
        self.assertTrue(fs.isLinkValid(os.path.join(os.getcwd(), 'test_fs', 'dir-valid')), "is not a link")
        self.assertFalse(fs.isLinkValid(os.path.join(os.getcwd(), 'test_fs', 'dir-invalid')), "is not a link")
        self.assertFalse(fs.isLinkValid(os.path.join(os.getcwd(), 'test_fs', 'readme.txt')), "is not a link")
        self.assertFalse(fs.isLinkValid(os.path.join(os.getcwd(), 'test_fs', 'directory')), "is not a link")
        self.assertFalse(fs.isLinkValid(os.path.join(os.getcwd(), 'test_fs', 'missing')), "is not a link")
        pass

    def test_fs_isLink2dir(self):
        self.testname = sys._getframe().f_code.co_name
        self.assertFalse(fs.isLink2dir(os.path.join(os.getcwd(), 'test_fs', 'file-valid')), "is not a link")
        self.assertFalse(fs.isLink2dir(os.path.join(os.getcwd(), 'test_fs', 'file-invalid')), "is not a link")
        self.assertTrue(fs.isLink2dir(os.path.join(os.getcwd(), 'test_fs', 'dir-valid')), "is not a link")
        self.assertFalse(fs.isLink2dir(os.path.join(os.getcwd(), 'test_fs', 'dir-invalid')), "is not a link")
        self.assertFalse(fs.isLink2dir(os.path.join(os.getcwd(), 'test_fs', 'readme.txt')), "is not a link")
        self.assertFalse(fs.isLink2dir(os.path.join(os.getcwd(), 'test_fs', 'directory')), "is not a link")
        self.assertFalse(fs.isLink2dir(os.path.join(os.getcwd(), 'test_fs', 'missing')), "is not a link")
        pass

    def test_fs_isLink2file(self):
        self.testname = sys._getframe().f_code.co_name
        self.assertTrue(fs.isLink2file(os.path.join(os.getcwd(), 'test_fs', 'file-valid')), "is not a link")
        self.assertFalse(fs.isLink2file(os.path.join(os.getcwd(), 'test_fs', 'file-invalid')), "is not a link")
        self.assertFalse(fs.isLink2file(os.path.join(os.getcwd(), 'test_fs', 'dir-valid')), "is not a link")
        self.assertFalse(fs.isLink2file(os.path.join(os.getcwd(), 'test_fs', 'dir-invalid')), "is not a link")
        self.assertFalse(fs.isLink2file(os.path.join(os.getcwd(), 'test_fs', 'readme.txt')), "is not a link")
        self.assertFalse(fs.isLink2file(os.path.join(os.getcwd(), 'test_fs', 'directory')), "is not a link")
        self.assertFalse(fs.isLink2file(os.path.join(os.getcwd(), 'test_fs', 'missing')), "is not a link")
        pass

    def test_fs_isDirReg(self):
        self.testname = sys._getframe().f_code.co_name
        self.assertFalse(fs.isDirReg(os.path.join(os.getcwd(), 'test_fs', 'file-valid')), "is not a link")
        self.assertFalse(fs.isDirReg(os.path.join(os.getcwd(), 'test_fs', 'file-invalid')), "is not a link")
        self.assertFalse(fs.isDirReg(os.path.join(os.getcwd(), 'test_fs', 'dir-valid')), "is not a link")
        self.assertFalse(fs.isDirReg(os.path.join(os.getcwd(), 'test_fs', 'dir-invalid')), "is not a link")
        self.assertFalse(fs.isDirReg(os.path.join(os.getcwd(), 'test_fs', 'readme.txt')), "is not a link")
        self.assertTrue(fs.isDirReg(os.path.join(os.getcwd(), 'test_fs', 'directory')), "is not a link")
        self.assertFalse(fs.isDirReg(os.path.join(os.getcwd(), 'test_fs', 'missing')), "is not a link")
        pass

    def test_fs_isDir(self):
        self.testname = sys._getframe().f_code.co_name
        self.assertFalse(fs.isDir(os.path.join(os.getcwd(), 'test_fs', 'file-valid')), "is not a link")
        self.assertFalse(fs.isDir(os.path.join(os.getcwd(), 'test_fs', 'file-invalid')), "is not a link")
        self.assertTrue(fs.isDir(os.path.join(os.getcwd(), 'test_fs', 'dir-valid')), "is not a link")
        self.assertFalse(fs.isDir(os.path.join(os.getcwd(), 'test_fs', 'dir-invalid')), "is not a link")
        self.assertFalse(fs.isDir(os.path.join(os.getcwd(), 'test_fs', 'readme.txt')), "is not a link")
        self.assertTrue(fs.isDir(os.path.join(os.getcwd(), 'test_fs', 'directory')), "is not a link")
        self.assertFalse(fs.isDir(os.path.join(os.getcwd(), 'test_fs', 'missing')), "is not a link")
        pass

    def test_fs_isLeafDir(self):
        self.testname = sys._getframe().f_code.co_name
        self.assertFalse(fs.isLeafDir(os.path.join(os.getcwd(), 'test_fs', 'file-valid')), "is not a link")
        self.assertFalse(fs.isLeafDir(os.path.join(os.getcwd(), 'test_fs', 'file-invalid')), "is not a link")
        self.assertFalse(fs.isLeafDir(os.path.join(os.getcwd(), 'test_fs', 'dir-valid')), "is not a link")
        self.assertFalse(fs.isLeafDir(os.path.join(os.getcwd(), 'test_fs', 'dir-invalid')), "is not a link")
        self.assertFalse(fs.isLeafDir(os.path.join(os.getcwd(), 'test_fs', 'readme.txt')), "is not a link")
        self.assertFalse(fs.isLeafDir(os.path.join(os.getcwd(), 'test_fs', 'directory')), "is not a link")
        self.assertTrue(fs.isLeafDir(os.path.join(os.getcwd(), 'test_fs', 'directory/leaf')), "is not a link")
        self.assertFalse(fs.isLeafDir(os.path.join(os.getcwd(), 'test_fs', 'missing')), "is not a link")
        pass

    def test_fs_isFileReg(self):
        self.testname = sys._getframe().f_code.co_name
        self.assertFalse(fs.isFileReg(os.path.join(os.getcwd(), 'test_fs', 'file-valid')), "is not a link")
        self.assertFalse(fs.isFileReg(os.path.join(os.getcwd(), 'test_fs', 'file-invalid')), "is not a link")
        self.assertFalse(fs.isFileReg(os.path.join(os.getcwd(), 'test_fs', 'dir-valid')), "is not a link")
        self.assertFalse(fs.isFileReg(os.path.join(os.getcwd(), 'test_fs', 'dir-invalid')), "is not a link")
        self.assertTrue(fs.isFileReg(os.path.join(os.getcwd(), 'test_fs', 'readme.txt')), "is not a link")
        self.assertFalse(fs.isFileReg(os.path.join(os.getcwd(), 'test_fs', 'directory')), "is not a link")
        self.assertFalse(fs.isFileReg(os.path.join(os.getcwd(), 'test_fs', 'missing')), "is not a link")
        pass

    def test_fs_isFile(self):
        self.testname = sys._getframe().f_code.co_name
        self.assertTrue(fs.isFile(os.path.join(os.getcwd(), 'test_fs', 'file-valid')), "is not a link")
        self.assertFalse(fs.isFile(os.path.join(os.getcwd(), 'test_fs', 'file-invalid')), "is not a link")
        self.assertFalse(fs.isFile(os.path.join(os.getcwd(), 'test_fs', 'dir-valid')), "is not a link")
        self.assertFalse(fs.isFile(os.path.join(os.getcwd(), 'test_fs', 'dir-invalid')), "is not a link")
        self.assertTrue(fs.isFile(os.path.join(os.getcwd(), 'test_fs', 'readme.txt')), "is not a link")
        self.assertFalse(fs.isFile(os.path.join(os.getcwd(), 'test_fs', 'directory')), "is not a link")
        self.assertFalse(fs.isFile(os.path.join(os.getcwd(), 'test_fs', 'missing')), "is not a link")
        pass


    def test_fs_classify(self):
        self.testname = sys._getframe().f_code.co_name
        # Following Symlinks
        self.assertTrue(fs.classify(os.path.join(os.getcwd(), 'test_fs', 'file-valid'), True) == 'file', "is not a link")
        self.assertTrue(fs.classify(os.path.join(os.getcwd(), 'test_fs', 'file-invalid'), True) == 'broken', "is not a link")
        self.assertTrue(fs.classify(os.path.join(os.getcwd(), 'test_fs', 'dir-valid'), True) == 'directory', "is not a link")
        self.assertTrue(fs.classify(os.path.join(os.getcwd(), 'test_fs', 'dir-invalid'), True) == 'broken', "is not a link")
        self.assertTrue(fs.classify(os.path.join(os.getcwd(), 'test_fs', 'readme.txt'), True) == 'file', "is not a link")
        self.assertTrue(fs.classify(os.path.join(os.getcwd(), 'test_fs', 'directory'), True) == 'directory', "is not a link")
        self.assertTrue(fs.classify(os.path.join(os.getcwd(), 'test_fs', 'missing'), True) == 'broken', "is not a link")
        # Do not follow symlinks
        self.assertTrue(fs.classify(os.path.join(os.getcwd(), 'test_fs', 'file-valid'), False) == 'symlink', "is not a link")
        self.assertTrue(fs.classify(os.path.join(os.getcwd(), 'test_fs', 'file-invalid'), False) == 'symlink', "is not a link")
        self.assertTrue(fs.classify(os.path.join(os.getcwd(), 'test_fs', 'dir-valid'), False) == 'symlink', "is not a link")
        self.assertTrue(fs.classify(os.path.join(os.getcwd(), 'test_fs', 'dir-invalid'), False) == 'symlink', "is not a link")
        self.assertTrue(fs.classify(os.path.join(os.getcwd(), 'test_fs', 'readme.txt'), False) == 'file', "is not a link")
        self.assertTrue(fs.classify(os.path.join(os.getcwd(), 'test_fs', 'directory'), False) == 'directory', "is not a link")
        self.assertTrue(fs.classify(os.path.join(os.getcwd(), 'test_fs', 'missing'), False) == 'broken', "is not a link")
        pass

unittest.main()
