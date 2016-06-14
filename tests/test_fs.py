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
        files = ['file-valid', 'file-invalid', 'dir-valid', 'dir-invalid', 'readme.txt', 'directory', 'missing']

        self.paths = {}
        for file in files:
            self.paths[file] = os.path.join(os.getcwd(), 'test_fs', file)

        self.specialChars = {}
        self.specialChars[':'] = u"\xee"
        pass

    def tearDown(self):
        pass

    def test_fs_exists_ln2file_valid(self):
        self.assertTrue(fs.exists(self.paths['file-valid']))
        pass

    def test_fs_exists_ln2file_invalid(self):
        self.assertTrue(fs.exists(self.paths['file-invalid']))
        pass

    def test_fs_exists_ln2dir_valid(self):
        self.assertTrue(fs.exists(self.paths['dir-valid']))
        pass

    def test_fs_exists_ln2dir_invalid(self):
        self.assertTrue(fs.exists(self.paths['dir-invalid']))
        pass

    def test_fs_exists_file(self):
        self.assertTrue(fs.exists(self.paths['readme.txt']))
        pass

    def test_fs_exists_directory(self):
        self.assertTrue(fs.exists(self.paths['directory']))
        pass

    def test_fs_exists_missing(self):
        self.assertFalse(fs.exists(self.paths['missing']))
        pass

    def test_fs_isLink_file_valid(self):
        self.assertTrue(fs.isLink(self.paths['file-valid']))
        pass

    def test_fs_isLink_file_invalid(self):
        self.assertTrue(fs.isLink(self.paths['file-invalid']))
        pass

    def test_fs_isLink_dir_valid(self):
        self.assertTrue(fs.isLink(self.paths['dir-valid']))
        pass

    def test_fs_isLink_dir_invalid(self):
        self.assertTrue(fs.isLink(self.paths['dir-invalid']))
        pass

    def test_fs_isLink_file(self):
        self.assertFalse(fs.isLink(self.paths['readme.txt']))
        pass

    def test_fs_isLink_directory(self):
        self.assertFalse(fs.isLink(self.paths['directory']))
        pass

    def test_fs_isLink_missing(self):
        self.assertFalse(fs.isLink(self.paths['missing']))
        pass

    def test_fs_isLinkInvalid_ln2file_valid(self):
        self.assertFalse(fs.isLinkInvalid(self.paths['file-valid']))
        pass

    def test_fs_isLinkInvalid_ln2file_invalid(self):
        self.assertTrue(fs.isLinkInvalid(self.paths['file-invalid']))
        pass

    def test_fs_isLinkInvalid_ln2dir_valid(self):
        self.assertFalse(fs.isLinkInvalid(self.paths['dir-valid']))
        pass

    def test_fs_isLinkInvalid_ln2dir_invalid(self):
        self.assertTrue(fs.isLinkInvalid(self.paths['dir-invalid']))
        pass

    def test_fs_isLinkInvalid_file(self):
        self.assertFalse(fs.isLinkInvalid(self.paths['readme.txt']))
        pass

    def test_fs_isLinkInvalid_directory(self):
        self.assertFalse(fs.isLinkInvalid(self.paths['directory']))
        pass

    def test_fs_isLinkInvalid_missing(self):
        self.assertFalse(fs.isLinkInvalid(self.paths['missing']))
        pass

    def test_fs_isLinkValid(self):
        self.assertTrue(fs.isLinkValid(os.path.join(os.getcwd(), 'test_fs', 'file-valid')), "is not a link")
        self.assertFalse(fs.isLinkValid(os.path.join(os.getcwd(), 'test_fs', 'file-invalid')), "is not a link")
        self.assertTrue(fs.isLinkValid(os.path.join(os.getcwd(), 'test_fs', 'dir-valid')), "is not a link")
        self.assertFalse(fs.isLinkValid(os.path.join(os.getcwd(), 'test_fs', 'dir-invalid')), "is not a link")
        self.assertFalse(fs.isLinkValid(os.path.join(os.getcwd(), 'test_fs', 'readme.txt')), "is not a link")
        self.assertFalse(fs.isLinkValid(os.path.join(os.getcwd(), 'test_fs', 'directory')), "is not a link")
        self.assertFalse(fs.isLinkValid(os.path.join(os.getcwd(), 'test_fs', 'missing')), "is not a link")
        pass

    def test_fs_isLink2dir(self):
        self.assertFalse(fs.isLink2dir(os.path.join(os.getcwd(), 'test_fs', 'file-valid')), "is not a link")
        self.assertFalse(fs.isLink2dir(os.path.join(os.getcwd(), 'test_fs', 'file-invalid')), "is not a link")
        self.assertTrue(fs.isLink2dir(os.path.join(os.getcwd(), 'test_fs', 'dir-valid')), "is not a link")
        self.assertFalse(fs.isLink2dir(os.path.join(os.getcwd(), 'test_fs', 'dir-invalid')), "is not a link")
        self.assertFalse(fs.isLink2dir(os.path.join(os.getcwd(), 'test_fs', 'readme.txt')), "is not a link")
        self.assertFalse(fs.isLink2dir(os.path.join(os.getcwd(), 'test_fs', 'directory')), "is not a link")
        self.assertFalse(fs.isLink2dir(os.path.join(os.getcwd(), 'test_fs', 'missing')), "is not a link")
        pass

    def test_fs_isLink2file(self):
        self.assertTrue(fs.isLink2file(os.path.join(os.getcwd(), 'test_fs', 'file-valid')), "is not a link")
        self.assertFalse(fs.isLink2file(os.path.join(os.getcwd(), 'test_fs', 'file-invalid')), "is not a link")
        self.assertFalse(fs.isLink2file(os.path.join(os.getcwd(), 'test_fs', 'dir-valid')), "is not a link")
        self.assertFalse(fs.isLink2file(os.path.join(os.getcwd(), 'test_fs', 'dir-invalid')), "is not a link")
        self.assertFalse(fs.isLink2file(os.path.join(os.getcwd(), 'test_fs', 'readme.txt')), "is not a link")
        self.assertFalse(fs.isLink2file(os.path.join(os.getcwd(), 'test_fs', 'directory')), "is not a link")
        self.assertFalse(fs.isLink2file(os.path.join(os.getcwd(), 'test_fs', 'missing')), "is not a link")
        pass

    def test_fs_isDirReg(self):
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


    def test_fs_classify_follow_ln2file_valid(self):
        response = fs.classify(os.path.join(os.getcwd(), 'test_fs', 'file-valid'), True)
        expected = 'file'
        self.assertTrue(response == expected, "%s is not %s"%(response, expected))
        pass

    def test_fs_classify_follow_ln2file_invalid(self):
        response = fs.classify(os.path.join(os.getcwd(), 'test_fs', 'file-invalid'), True)
        expected = 'broken'
        self.assertTrue(response == expected, "%s is not %s"%(response, expected))
        pass

    def test_fs_classify_follow_ln2dir_valid(self):
        response = fs.classify(os.path.join(os.getcwd(), 'test_fs', 'dir-valid'), True)
        expected = 'directory'
        self.assertTrue(response == expected, "%s is not %s"%(response, expected))
        pass

    def test_fs_classify_follow_ln2dir_invalid(self):
        response = fs.classify(os.path.join(os.getcwd(), 'test_fs', 'dir-invalid'), True)
        expected = 'broken'
        self.assertTrue(response == expected, "%s is not %s"%(response, expected))
        pass

    def test_fs_classify_follow_file(self):
        response = fs.classify(os.path.join(os.getcwd(), 'test_fs', 'readme.txt'), True) 
        expected = 'file'
        self.assertTrue(response == expected, "%s is not %s"%(response, expected))
        pass

    def test_fs_classify_follow_directory(self):
        response = fs.classify(os.path.join(os.getcwd(), 'test_fs', 'directory'), True) 
        expected = 'directory'
        self.assertTrue(response == expected, "%s is not %s"%(response, expected))
        pass

    def test_fs_classify_follow_missing(self):
        response = fs.classify(os.path.join(os.getcwd(), 'test_fs', 'missing'), True)
        expected = 'broken'
        self.assertTrue(response == expected, "%s is not %s"%(response, expected))
        pass

    def test_fs_classify_nofollow_ln2file_valid(self):
        response = fs.classify(os.path.join(os.getcwd(), 'test_fs', 'file-valid'), False) 
        expected = 'symlink'
        self.assertTrue(response == expected, "%s is not %s"%(response, expected))
        pass

    def test_fs_classify_nofollow_ln2file_invalid(self):
        response = fs.classify(os.path.join(os.getcwd(), 'test_fs', 'file-invalid'), False) 
        expected = 'symlink'
        self.assertTrue(response == expected, "%s is not %s"%(response, expected))
        pass

    def test_fs_classify_nofollow_ln2dir_valid(self):
        response = fs.classify(os.path.join(os.getcwd(), 'test_fs', 'dir-valid'), False) 
        expected = 'symlink'
        self.assertTrue(response == expected, "%s is not %s"%(response, expected))
        pass

    def test_fs_classify_nofollow_ln2dir_invalid(self):
        response = fs.classify(os.path.join(os.getcwd(), 'test_fs', 'dir-invalid'), False) 
        expected = 'symlink'
        self.assertTrue(response == expected, "%s is not %s"%(response, expected))
        pass

    def test_fs_classify_nofollow_file(self):
        response = fs.classify(os.path.join(os.getcwd(), 'test_fs', 'readme.txt'), False)           
        expected = 'file'
        self.assertTrue(response == expected, "%s is not %s"%(response, expected))
        pass

    def test_fs_classify_nofollow_directory(self):
        response = fs.classify(os.path.join(os.getcwd(), 'test_fs', 'directory'), False)         
        expected = 'directory'
        self.assertTrue(response == expected, "%s is not %s"%(response, expected))
        pass

    def test_fs_classify_nofollow_missing(self):
        response = fs.classify(os.path.join(os.getcwd(), 'test_fs', 'missing'), False)
        expected = 'broken'
        self.assertTrue(response == expected, "%s is not %s"%(response, expected))
        pass

    def test_fs_hasSpecialChars_with(self):
        pathWithSpecials = os.path.join(os.getcwd(), 'test_fs', u"file:valid")

        self.assertTrue(fs.hasSpecialChars(self.specialChars, pathWithSpecials), pathWithSpecials)
        pass

    def test_fs_hasSpecialChars_without(self):
        pathWithoutSpecials = os.path.join(os.getcwd(), 'test_fs', 'file-valid')

        self.assertFalse(fs.hasSpecialChars(self.specialChars, pathWithoutSpecials), pathWithoutSpecials)
        pass

    def test_fs_subSpecialChars_filename(self):
        path = os.path.join(os.getcwd(), 'test_fs', u"file:valid")
        expected = os.path.join(os.getcwd(), 'test_fs', "file"+u"\xee"+"valid")

        result = fs.subSpecialChars(self.specialChars, path)
        self.assertTrue(result == expected, "%s != %s"%(result, expected))
        pass

    def test_fs_subSpecialChars_directory(self):
        path = os.path.join(os.getcwd(), 'test_fs', u"dir:valid", "file")
        expected = os.path.join(os.getcwd(), 'test_fs', "dir"+u"\xee"+"valid")
        
        result = fs.subSpecialChars(self.specialChars, path)
        self.assertTrue(result == expected, "%s != %s"%(result, expected))
        pass

    def test_fs_subSpecialChars_dir_file(self):
        path = os.path.join(os.getcwd(), 'test_fs', u"dir:valid", u"file:valid")
        expected = os.path.join(os.getcwd(), 'test_fs', "dir"+u"\xee"+"valid")
        
        result = fs.subSpecialChars(self.specialChars, path)
        self.assertTrue(result == expected, "%s != %s"%(result, expected))
        pass

unittest.main()
