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
from stats import boundaries, stats


class test_stats(unittest.TestCase):
    def setUp(self):
        self.testname =""
        self.boundaries = boundaries()
        self.stats = stats()
        pass

    def tearDown(self):
        testname = self.testname.split(self.__class__.__name__+'_',1)[1]
        outputDir = os.path.join(os.getcwd(),'test_stats/', testname, 'output/')
        if os.path.exists(outputDir):
           shutil.rmtree(outputDir)
           os.mkdir(outputDir)
        pass

    def test_stats_loadboundaries_bad(self):
        self.testname = sys._getframe().f_code.co_name
        self.assertRaises(IOError, self.boundaries.load, 'non-sense.csv')
        pass

    def test_stats_boundaries_load_good(self):
        self.testname = sys._getframe().f_code.co_name
        assertBoundaries = [('<50KB', '0', '50000'), ('50-100KB', '50000', '100000'), ('100-250KB', '100000', '250000'), ('250-500KB', '250000', '500000'), ('500-750KB', '500000', '750000'), ('750KB-1MB', '750000', '1000000'), ('1-10MB', '1000000', '10000000'), ('10-50MB', '10000000', '50000000'), ('50-100MB', '50000000', '100000000'), ('100-250MB', '100000000', '250000000'), ('250>', '250000000', '')]
        assertFields = ['Category', 'bytes <50KB', 'bytes 250-500KB', 'bytes 500-750KB', 'bytes 50-100MB', 'bytes 10-50MB', 'bytes 750KB-1MB', 'bytes 250>', 'bytes 50-100KB', 'bytes 100-250KB', 'bytes 100-250MB', 'bytes 1-10MB', 'count 750KB-1MB', 'count 1-10MB', 'count 250>', 'count 250-500KB', 'count <50KB', 'count 50-100KB', 'count 100-250KB', 'count 100-250MB', 'count 500-750KB', 'count 50-100MB', 'count 10-50MB']
        boundarypath = os.path.join(os.getcwd(), 'test_stats/boundaries.csv')

        self.boundaries.load(boundarypath)
        self.assertEqual(self.boundaries.getBoundaries(), assertBoundaries)
        self.assertEqual(self.boundaries.getFields(), assertFields)
        pass

    def test_stats_loadcategory(self):
        self.testname = sys._getframe().f_code.co_name
        assertCategories = ['added', 'modified', 'unchanged', 'removed']
        for category in assertCategories:
            self.stats.addCategory(category)
        self.assertEqual(self.stats.getCategories(), assertCategories)
        pass

    def test_stats_loadcategory(self):
        self.testname = sys._getframe().f_code.co_name
        assertCategories = ['added', 'modified', 'unchanged', 'removed']
        self.stats.addCategories(assertCategories)
        self.assertEqual(self.stats.getCategories(), assertCategories)
        pass

    def test_stats_init_stats_boundaries(self):
        self.testname = sys._getframe().f_code.co_name
        assertStats = {'unchanged': {'Category': 'unchanged', 'count 750KB-1MB': 0, 'bytes 250-500KB': 0, 'count 500-750KB': 0, 'bytes 1-10MB': 0, 'bytes <50KB': 0, 'count 1-10MB': 0, 'bytes 50-100KB': 0, 'bytes 50-100MB': 0, 'count 10-50MB': 0, 'count 250-500KB': 0, 'bytes 750KB-1MB': 0, 'count 50-100KB': 0, 'count <50KB': 0, 'bytes 250>': 0, 'count 50-100MB': 0, 'bytes 10-50MB': 0, 'count 250>': 0, 'bytes 100-250KB': 0, 'count 100-250KB': 0, 'bytes 500-750KB': 0, 'count 100-250MB': 0, 'bytes 100-250MB': 0}, 'added': {'Category': 'added', 'count 750KB-1MB': 0, 'bytes 250-500KB': 0, 'count 500-750KB': 0, 'bytes 1-10MB': 0, 'bytes <50KB': 0, 'count 1-10MB': 0, 'bytes 50-100KB': 0, 'bytes 50-100MB': 0, 'count 10-50MB': 0, 'count 250-500KB': 0, 'bytes 750KB-1MB': 0, 'count 50-100KB': 0, 'count <50KB': 0, 'bytes 250>': 0, 'count 50-100MB': 0, 'bytes 10-50MB': 0, 'count 250>': 0, 'bytes 100-250KB': 0, 'count 100-250KB': 0, 'bytes 500-750KB': 0, 'count 100-250MB': 0, 'bytes 100-250MB': 0}, 'removed': {'Category': 'removed', 'count 750KB-1MB': 0, 'bytes 250-500KB': 0, 'count 500-750KB': 0, 'bytes 1-10MB': 0, 'bytes <50KB': 0, 'count 1-10MB': 0, 'bytes 50-100KB': 0, 'bytes 50-100MB': 0, 'count 10-50MB': 0, 'count 250-500KB': 0, 'bytes 750KB-1MB': 0, 'count 50-100KB': 0, 'count <50KB': 0, 'bytes 250>': 0, 'count 50-100MB': 0, 'bytes 10-50MB': 0, 'count 250>': 0, 'bytes 100-250KB': 0, 'count 100-250KB': 0, 'bytes 500-750KB': 0, 'count 100-250MB': 0, 'bytes 100-250MB': 0}, 'modified': {'Category': 'modified', 'count 750KB-1MB': 0, 'bytes 250-500KB': 0, 'count 500-750KB': 0, 'bytes 1-10MB': 0, 'bytes <50KB': 0, 'count 1-10MB': 0, 'bytes 50-100KB': 0, 'bytes 50-100MB': 0, 'count 10-50MB': 0, 'count 250-500KB': 0, 'bytes 750KB-1MB': 0, 'count 50-100KB': 0, 'count <50KB': 0, 'bytes 250>': 0, 'count 50-100MB': 0, 'bytes 10-50MB': 0, 'count 250>': 0, 'bytes 100-250KB': 0, 'count 100-250KB': 0, 'bytes 500-750KB': 0, 'count 100-250MB': 0, 'bytes 100-250MB': 0}}
        assertCategories = ['added', 'modified', 'unchanged', 'removed']
        boundarypath = os.path.join(os.getcwd(), 'test_stats/boundaries.csv')
        self.boundaries = boundaries.initWithBoundaries(boundarypath)
        self.stats = stats.initWithBoundaries(self.boundaries)
        self.stats.addCategories(assertCategories)
        self.stats.init()
        self.assertEqual(self.stats.stats, assertStats)
        pass

    def test_stats_init_stats_Categories(self):
        self.testname = sys._getframe().f_code.co_name
        assertStats = {'unchanged': {'Category': 'unchanged', 'count 750KB-1MB': 0, 'bytes 250-500KB': 0, 'count 500-750KB': 0, 'bytes 1-10MB': 0, 'bytes <50KB': 0, 'count 1-10MB': 0, 'bytes 50-100KB': 0, 'bytes 50-100MB': 0, 'count 10-50MB': 0, 'count 250-500KB': 0, 'bytes 750KB-1MB': 0, 'count 50-100KB': 0, 'count <50KB': 0, 'bytes 250>': 0, 'count 50-100MB': 0, 'bytes 10-50MB': 0, 'count 250>': 0, 'bytes 100-250KB': 0, 'count 100-250KB': 0, 'bytes 500-750KB': 0, 'count 100-250MB': 0, 'bytes 100-250MB': 0}, 'added': {'Category': 'added', 'count 750KB-1MB': 0, 'bytes 250-500KB': 0, 'count 500-750KB': 0, 'bytes 1-10MB': 0, 'bytes <50KB': 0, 'count 1-10MB': 0, 'bytes 50-100KB': 0, 'bytes 50-100MB': 0, 'count 10-50MB': 0, 'count 250-500KB': 0, 'bytes 750KB-1MB': 0, 'count 50-100KB': 0, 'count <50KB': 0, 'bytes 250>': 0, 'count 50-100MB': 0, 'bytes 10-50MB': 0, 'count 250>': 0, 'bytes 100-250KB': 0, 'count 100-250KB': 0, 'bytes 500-750KB': 0, 'count 100-250MB': 0, 'bytes 100-250MB': 0}, 'removed': {'Category': 'removed', 'count 750KB-1MB': 0, 'bytes 250-500KB': 0, 'count 500-750KB': 0, 'bytes 1-10MB': 0, 'bytes <50KB': 0, 'count 1-10MB': 0, 'bytes 50-100KB': 0, 'bytes 50-100MB': 0, 'count 10-50MB': 0, 'count 250-500KB': 0, 'bytes 750KB-1MB': 0, 'count 50-100KB': 0, 'count <50KB': 0, 'bytes 250>': 0, 'count 50-100MB': 0, 'bytes 10-50MB': 0, 'count 250>': 0, 'bytes 100-250KB': 0, 'count 100-250KB': 0, 'bytes 500-750KB': 0, 'count 100-250MB': 0, 'bytes 100-250MB': 0}, 'modified': {'Category': 'modified', 'count 750KB-1MB': 0, 'bytes 250-500KB': 0, 'count 500-750KB': 0, 'bytes 1-10MB': 0, 'bytes <50KB': 0, 'count 1-10MB': 0, 'bytes 50-100KB': 0, 'bytes 50-100MB': 0, 'count 10-50MB': 0, 'count 250-500KB': 0, 'bytes 750KB-1MB': 0, 'count 50-100KB': 0, 'count <50KB': 0, 'bytes 250>': 0, 'count 50-100MB': 0, 'bytes 10-50MB': 0, 'count 250>': 0, 'bytes 100-250KB': 0, 'count 100-250KB': 0, 'bytes 500-750KB': 0, 'count 100-250MB': 0, 'bytes 100-250MB': 0}}
        boundarypath = os.path.join(os.getcwd(), 'test_stats/boundaries.csv')
        self.boundaries = boundaries.initWithBoundaries(boundarypath)
        assertCategories = ['added', 'modified', 'unchanged', 'removed']
        self.stats = stats.initWithCategories(self.boundaries, assertCategories)
        self.assertEqual(self.stats.stats, assertStats)
        pass

    def test_stats_write_stats(self):
        self.testname = sys._getframe().f_code.co_name
        writepath = os.path.join(os.getcwd(), 'test_stats/write_stats/output/')
        assertStatsFile = os.path.join(os.getcwd(), 'test_stats/write_stats/expected/snapshot.csv')
        boundarypath = os.path.join(os.getcwd(), 'test_stats/boundaries.csv')
        self.boundaries = boundaries.initWithBoundaries(boundarypath)
        assertCategories = ['added', 'modified', 'unchanged', 'removed']
        self.stats = stats.initWithCategories(self.boundaries, assertCategories)
        self.stats.export('snapshot', writepath)
        self.assertTrue(filecmp.cmp(os.path.join(writepath,"snapshot.csv"),  assertStatsFile), "output and expected file, do not match")
        pass

    def test_stats_update_stats(self):
        self.testname = sys._getframe().f_code.co_name
        writepath = os.path.join(os.getcwd(), 'test_stats/update_stats/output/')
        assertStatsFile = os.path.join(os.getcwd(), 'test_stats/update_stats/expected/snapshot.csv')
        boundarypath = os.path.join(os.getcwd(), 'test_stats/boundaries.csv')
        self.boundaries = boundaries.initWithBoundaries(boundarypath)
        assertCategories = ['added', 'modified', 'unchanged', 'removed']

        self.stats = stats.initWithCategories(self.boundaries, assertCategories)

        self.stats.update('added', 1)
        self.stats.update('added', 1)
        self.stats.update('added', 1)
        self.stats.update('added', 1)
        self.stats.update('added', 1)
        self.stats.update('added', 50001)
        self.stats.update('added', 50001)
        self.stats.update('added', 50001)
        self.stats.update('added', 50001)
        self.stats.update('added', 50001)
        self.stats.update('added', 100001)
        self.stats.update('added', 100001)
        self.stats.update('added', 100001)
        self.stats.update('added', 100001)
        self.stats.update('added', 100001)
        self.stats.update('added', 250001)
        self.stats.update('added', 250001)
        self.stats.update('added', 250001)
        self.stats.update('added', 250001)
        self.stats.update('added', 500001)
        self.stats.update('added', 500001)
        self.stats.update('added', 500001)
        self.stats.update('added', 500001)
        self.stats.update('added', 750001)
        self.stats.update('added', 750001)
        self.stats.update('added', 750001)
        self.stats.update('added', 750001)
        self.stats.update('added', 1000001)
        self.stats.update('added', 1000001)
        self.stats.update('added', 1000001)
        self.stats.update('added', 1000001)
        self.stats.update('added', 10000001)
        self.stats.update('added', 10000001)
        self.stats.update('added', 10000001)
        self.stats.update('added', 10000001)
        self.stats.update('added', 50000001)
        self.stats.update('added', 50000001)
        self.stats.update('added', 50000001)
        self.stats.update('added', 50000001)
        self.stats.update('added', 100000001)
        self.stats.update('added', 100000001)
        self.stats.update('added', 100000001)
        self.stats.update('added', 100000001)
        self.stats.update('added', 250000001)
        self.stats.update('added', 250000001)
        self.stats.update('added', 250000001)
        self.stats.update('added', 250000001)

        self.stats.export('snapshot', writepath)
        self.assertTrue(filecmp.cmp(os.path.join(writepath,"snapshot.csv"),  assertStatsFile), "output and expected file, do not match")
        pass

    def test_stats_getBytesFor(self):
        self.testname = sys._getframe().f_code.co_name
        boundarypath = os.path.join(os.getcwd(), 'test_stats/boundaries.csv')
        self.boundaries = boundaries.initWithBoundaries(boundarypath)
        assertCategories = ['added', 'modified', 'unchanged', 'removed']
        self.stats = stats.initWithCategories(self.boundaries, assertCategories)
        self.stats.update('added', 250)
        self.stats.update('added', 150)
        self.stats.update('added', 500)
        self.stats.update('added', 300)
        addedBytes = self.stats.getBytesFor('added')
        assertBytes = 1200
        addedCounts = self.stats.getCountFor('added')
        assertCounts = 4
        self.assertEqual(addedBytes, assertBytes)
        self.assertEqual(addedCounts, assertCounts)
        pass

    def test_stats_getBytesAll(self):
        self.testname = sys._getframe().f_code.co_name
        boundarypath = os.path.join(os.getcwd(), 'test_stats/boundaries.csv')
        self.boundaries = boundaries.initWithBoundaries(boundarypath)
        assertCategories = ['added', 'modified', 'unchanged', 'removed']
        self.stats = stats.initWithCategories(self.boundaries, assertCategories)
        self.stats.update('added', 250)
        self.stats.update('modified', 150)
        self.stats.update('unchanged', 500)
        self.stats.update('removed', 300)
        totalBytes = self.stats.getBytesAll()
        assertBytes = 1200
        totalCounts = self.stats.getCountAll()
        assertCounts = 4
        self.assertEqual(totalBytes, assertBytes)
        self.assertEqual(totalCounts, assertCounts)
        pass

    def test_stats_reinit(self):
        self.testname = sys._getframe().f_code.co_name
        boundarypath = os.path.join(os.getcwd(), 'test_stats/boundaries.csv')
        self.boundaries = boundaries.initWithBoundaries(boundarypath)
        assertCategories = ['added', 'modified', 'unchanged', 'removed']
        self.stats = stats.initWithCategories(self.boundaries, assertCategories)
        self.stats.update('added', 250)
        self.stats.update('modified', 150)
        self.stats.update('unchanged', 500)
        self.stats.update('removed', 300)
        totalBytes = self.stats.getBytesAll()
        assertBytes = 1200
        self.assertEqual(totalBytes, assertBytes)
        self.stats.init()
        assertBytes = 0
        totalBytes = self.stats.getBytesAll()
        self.assertEqual(totalBytes, assertBytes)
        pass

unittest.main()
