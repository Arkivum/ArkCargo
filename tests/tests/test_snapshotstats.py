#!/usr/bin/env python
#
#  snapshotstats  Identifying information about tests here.
#
#===============
#  This is based on a skeleton test file, more information at:
#
#     https://github.com/linsomniac/python-unittest-skeleton


import unittest2 as unittest

import os, sys, filecmp, shutil
sys.path.append('../arkcargo')      
from snapshotstats import snapshotstats


class test_snapshotstats(unittest.TestCase):
    def setUp(self):
        self.testname =""
        self.snapshotstats = snapshotstats()
        pass

    def tearDown(self):
        testname = self.testname.split(self.__class__.__name__+'_',1)[1]
        outputDir = os.path.join(os.getcwd(),'test_snapshotstats/', testname, 'output/')
        if os.path.exists(outputDir):
           shutil.rmtree(outputDir)
           os.mkdir(outputDir)
        pass

    def test_snapshotstats_loadboundaries_bad(self):
        self.testname = sys._getframe().f_code.co_name
        self.assertRaises(IOError, self.snapshotstats.load_boundaries, 'non-sense.csv')
        pass

    def test_snapshotstats_loadboundaries_good(self):
        self.testname = sys._getframe().f_code.co_name
        assertBoundaries = [('<50KB', '0', '50000'), ('50-100KB', '50000', '100000'), ('100-250KB', '100000', '250000'), ('250-500KB', '250000', '500000'), ('500-750KB', '500000', '750000'), ('750KB-1MB', '750000', '1000000'), ('1-10MB', '1000000', '10000000'), ('10-50MB', '10000000', '50000000'), ('50-100MB', '50000000', '100000000'), ('100-250MB', '100000000', '250000000'), ('250>', '250000000', '')]
        boundarypath = os.path.join(os.getcwd(), 'test_snapshotstats/boundaries.csv')

        self.snapshotstats.load_boundaries(boundarypath)
        self.assertEqual(self.snapshotstats.boundaries, assertBoundaries)
        pass

    def test_snapshotstats_loadcategories(self):
        self.testname = sys._getframe().f_code.co_name
        assertCategories = ['added', 'modified', 'unchanged', 'removed']
        for category in assertCategories:
            self.snapshotstats.add_category(category)
        self.assertEqual(self.snapshotstats.categories, assertCategories) 
        pass

    def test_snapshotstats_set_good(self):
        self.testname = sys._getframe().f_code.co_name
        listCategories = ['added', 'modified', 'unchanged', 'removed']
        snapshot_set = ['added', 'modified', 'unchanged']
        
        self.snapshotstats.add_categories(listCategories)
        self.snapshotstats.add_set('snapshot', snapshot_set)

        self.assertTrue(self.snapshotstats.sets['snapshot'], snapshot_set)
        pass

    def test_snapshotstats_set_bad(self):
        self.testname = sys._getframe().f_code.co_name
        listCategories = ['added', 'modified', 'unchanged', 'removed']
        snapshot_set = ['unchanged', 'invisible']

        self.snapshotstats.add_categories(listCategories)

        self.assertRaises(ValueError, self.snapshotstats.add_set, 'snapshot', snapshot_set)
        pass

    def test_snapshotstats_init_stats(self):
        self.testname = sys._getframe().f_code.co_name
	assertStats = {'unchanged': {'Category': 'unchanged', 'count 750KB-1MB': 0, 'bytes 250-500KB': 0, 'count 500-750KB': 0, 'bytes 1-10MB': 0, 'bytes <50KB': 0, 'count 1-10MB': 0, 'bytes 50-100KB': 0, 'bytes 50-100MB': 0, 'count 10-50MB': 0, 'count 250-500KB': 0, 'bytes 750KB-1MB': 0, 'count 50-100KB': 0, 'count <50KB': 0, 'bytes 250>': 0, 'count 50-100MB': 0, 'bytes 10-50MB': 0, 'count 250>': 0, 'bytes 100-250KB': 0, 'count 100-250KB': 0, 'bytes 500-750KB': 0, 'count 100-250MB': 0, 'bytes 100-250MB': 0}, 'added': {'Category': 'added', 'count 750KB-1MB': 0, 'bytes 250-500KB': 0, 'count 500-750KB': 0, 'bytes 1-10MB': 0, 'bytes <50KB': 0, 'count 1-10MB': 0, 'bytes 50-100KB': 0, 'bytes 50-100MB': 0, 'count 10-50MB': 0, 'count 250-500KB': 0, 'bytes 750KB-1MB': 0, 'count 50-100KB': 0, 'count <50KB': 0, 'bytes 250>': 0, 'count 50-100MB': 0, 'bytes 10-50MB': 0, 'count 250>': 0, 'bytes 100-250KB': 0, 'count 100-250KB': 0, 'bytes 500-750KB': 0, 'count 100-250MB': 0, 'bytes 100-250MB': 0}, 'removed': {'Category': 'removed', 'count 750KB-1MB': 0, 'bytes 250-500KB': 0, 'count 500-750KB': 0, 'bytes 1-10MB': 0, 'bytes <50KB': 0, 'count 1-10MB': 0, 'bytes 50-100KB': 0, 'bytes 50-100MB': 0, 'count 10-50MB': 0, 'count 250-500KB': 0, 'bytes 750KB-1MB': 0, 'count 50-100KB': 0, 'count <50KB': 0, 'bytes 250>': 0, 'count 50-100MB': 0, 'bytes 10-50MB': 0, 'count 250>': 0, 'bytes 100-250KB': 0, 'count 100-250KB': 0, 'bytes 500-750KB': 0, 'count 100-250MB': 0, 'bytes 100-250MB': 0}, 'modified': {'Category': 'modified', 'count 750KB-1MB': 0, 'bytes 250-500KB': 0, 'count 500-750KB': 0, 'bytes 1-10MB': 0, 'bytes <50KB': 0, 'count 1-10MB': 0, 'bytes 50-100KB': 0, 'bytes 50-100MB': 0, 'count 10-50MB': 0, 'count 250-500KB': 0, 'bytes 750KB-1MB': 0, 'count 50-100KB': 0, 'count <50KB': 0, 'bytes 250>': 0, 'count 50-100MB': 0, 'bytes 10-50MB': 0, 'count 250>': 0, 'bytes 100-250KB': 0, 'count 100-250KB': 0, 'bytes 500-750KB': 0, 'count 100-250MB': 0, 'bytes 100-250MB': 0}}
        boundarypath = os.path.join(os.getcwd(), 'test_snapshotstats/boundaries.csv')
        setCategories = {'snapshot' : ['added', 'modified', 'unchanged', 'removed'], 'ingest' : ['added', 'modified']}
        self.snapshotstats = snapshotstats.initWithCategories(boundarypath, setCategories)
        self.assertEqual(self.snapshotstats.stats, assertStats)
        pass

    def test_snapshotstats_init_stats_good(self):
        self.testname = sys._getframe().f_code.co_name
        boundarypath = os.path.join(os.getcwd(), 'test_snapshotstats/boundaries.csv')
        listCategories = ['added', 'modified', 'unchanged', 'removed']
        assertStats = {'unchanged': {'Category': 'unchanged', 'count 750KB-1MB': 0, 'bytes 250-500KB': 0, 'count 500-750KB': 0, 'bytes 1-10MB': 0, 'bytes <50KB': 0, 'count 1-10MB': 0, 'bytes 50-100KB': 0, 'bytes 50-100MB': 0, 'count 10-50MB': 0, 'count 250-500KB': 0, 'bytes 750KB-1MB': 0, 'count 50-100KB': 0, 'count <50KB': 0, 'bytes 250>': 0, 'count 50-100MB': 0, 'bytes 10-50MB': 0, 'count 250>': 0, 'bytes 100-250KB': 0, 'count 100-250KB': 0, 'bytes 500-750KB': 0, 'count 100-250MB': 0, 'bytes 100-250MB': 0}, 'added': {'Category': 'added', 'count 750KB-1MB': 0, 'bytes 250-500KB': 0, 'count 500-750KB': 0, 'bytes 1-10MB': 0, 'bytes <50KB': 0, 'count 1-10MB': 0, 'bytes 50-100KB': 0, 'bytes 50-100MB': 0, 'count 10-50MB': 0, 'count 250-500KB': 0, 'bytes 750KB-1MB': 0, 'count 50-100KB': 0, 'count <50KB': 0, 'bytes 250>': 0, 'count 50-100MB': 0, 'bytes 10-50MB': 0, 'count 250>': 0, 'bytes 100-250KB': 0, 'count 100-250KB': 0, 'bytes 500-750KB': 0, 'count 100-250MB': 0, 'bytes 100-250MB': 0}, 'removed': {'Category': 'removed', 'count 750KB-1MB': 0, 'bytes 250-500KB': 0, 'count 500-750KB': 0, 'bytes 1-10MB': 0, 'bytes <50KB': 0, 'count 1-10MB': 0, 'bytes 50-100KB': 0, 'bytes 50-100MB': 0, 'count 10-50MB': 0, 'count 250-500KB': 0, 'bytes 750KB-1MB': 0, 'count 50-100KB': 0, 'count <50KB': 0, 'bytes 250>': 0, 'count 50-100MB': 0, 'bytes 10-50MB': 0, 'count 250>': 0, 'bytes 100-250KB': 0, 'count 100-250KB': 0, 'bytes 500-750KB': 0, 'count 100-250MB': 0, 'bytes 100-250MB': 0}, 'modified': {'Category': 'modified', 'count 750KB-1MB': 0, 'bytes 250-500KB': 0, 'count 500-750KB': 0, 'bytes 1-10MB': 0, 'bytes <50KB': 0, 'count 1-10MB': 0, 'bytes 50-100KB': 0, 'bytes 50-100MB': 0, 'count 10-50MB': 0, 'count 250-500KB': 0, 'bytes 750KB-1MB': 0, 'count 50-100KB': 0, 'count <50KB': 0, 'bytes 250>': 0, 'count 50-100MB': 0, 'bytes 10-50MB': 0, 'count 250>': 0, 'bytes 100-250KB': 0, 'count 100-250KB': 0, 'bytes 500-750KB': 0, 'count 100-250MB': 0, 'bytes 100-250MB': 0}}
        self.snapshotstats.load_boundaries(boundarypath)
        self.snapshotstats.add_categories(listCategories)
        self.snapshotstats.initStats()
        self.assertEqual(self.snapshotstats.stats, assertStats)
        pass
    
    def test_snapshotstats_load_stats_good(self):
        self.testname = sys._getframe().f_code.co_name
        assertStats = {'unchanged': {'Category': 'unchanged', 'count 750KB-1MB': 0, 'bytes 250-500KB': 0, 'count 500-750KB': 0, 'bytes 1-10MB': 0, 'bytes <50KB': 0, 'count 1-10MB': 0, 'bytes 50-100KB': 0, 'bytes 50-100MB': 0, 'count 10-50MB': 0, 'count 250-500KB': 0, 'bytes 750KB-1MB': 0, 'count 50-100KB': 0, 'count <50KB': 0, 'bytes 250>': 0, 'count 50-100MB': 0, 'bytes 10-50MB': 0, 'count 250>': 0, 'bytes 100-250KB': 0, 'count 100-250KB': 0, 'bytes 500-750KB': 0, 'count 100-250MB': 0, 'bytes 100-250MB': 0}, 'added': {'Category': 'added', 'count 750KB-1MB': 0, 'bytes 250-500KB': 0, 'count 500-750KB': 0, 'bytes 1-10MB': 0, 'bytes <50KB': 3785, 'count 1-10MB': 0, 'bytes 50-100KB': 0, 'bytes 50-100MB': 0, 'count 10-50MB': 0, 'count 250-500KB': 0, 'bytes 750KB-1MB': 0, 'count 50-100KB': 0, 'count <50KB': 19, 'bytes 250>': 0, 'count 50-100MB': 0, 'bytes 10-50MB': 0, 'count 250>': 0, 'bytes 100-250KB': 0, 'count 100-250KB': 0, 'bytes 500-750KB': 0, 'count 100-250MB': 0, 'bytes 100-250MB': 0}, 'modified': {'Category': 'modified', 'count 750KB-1MB': 0, 'bytes 250-500KB': 0, 'count 500-750KB': 0, 'bytes 1-10MB': 0, 'bytes <50KB': 0, 'count 1-10MB': 0, 'bytes 50-100KB': 0, 'bytes 50-100MB': 0, 'count 10-50MB': 0, 'count 250-500KB': 0, 'bytes 750KB-1MB': 0, 'count 50-100KB': 0, 'count <50KB': 0, 'bytes 250>': 0, 'count 50-100MB': 0, 'bytes 10-50MB': 0, 'count 250>': 0, 'bytes 100-250KB': 0, 'count 100-250KB': 0, 'bytes 500-750KB': 0, 'count 100-250MB': 0, 'bytes 100-250MB': 0}}

        statspath = os.path.join(os.getcwd(), 'test_snapshotstats/load_stats_good/')
        boundarypath = os.path.join(os.getcwd(), 'test_snapshotstats/boundaries.csv')
        listCategories = ['added', 'modified', 'unchanged', 'removed']

        self.snapshotstats.load_boundaries(boundarypath)

        self.snapshotstats.add_categories(listCategories)
        self.snapshotstats.loadStats(statspath)
        self.assertEqual(self.snapshotstats.stats, assertStats)
        pass

    def test_snapshotstats_load_stats_good_singlestep(self):
        self.testname = sys._getframe().f_code.co_name
	assertStats = {'unchanged': {'Category': 'unchanged', 'count 750KB-1MB': 0, 'bytes 250-500KB': 0, 'count 500-750KB': 0, 'bytes 1-10MB': 0, 'bytes <50KB': 0, 'count 1-10MB': 0, 'bytes 50-100KB': 0, 'bytes 50-100MB': 0, 'count 10-50MB': 0, 'count 250-500KB': 0, 'bytes 750KB-1MB': 0, 'count 50-100KB': 0, 'count <50KB': 0, 'bytes 250>': 0, 'count 50-100MB': 0, 'bytes 10-50MB': 0, 'count 250>': 0, 'bytes 100-250KB': 0, 'count 100-250KB': 0, 'bytes 500-750KB': 0, 'count 100-250MB': 0, 'bytes 100-250MB': 0}, 'added': {'Category': 'added', 'count 750KB-1MB': 0, 'bytes 250-500KB': 0, 'count 500-750KB': 0, 'bytes 1-10MB': 0, 'bytes <50KB': 3785, 'count 1-10MB': 0, 'bytes 50-100KB': 0, 'bytes 50-100MB': 0, 'count 10-50MB': 0, 'count 250-500KB': 0, 'bytes 750KB-1MB': 0, 'count 50-100KB': 0, 'count <50KB': 19, 'bytes 250>': 0, 'count 50-100MB': 0, 'bytes 10-50MB': 0, 'count 250>': 0, 'bytes 100-250KB': 0, 'count 100-250KB': 0, 'bytes 500-750KB': 0, 'count 100-250MB': 0, 'bytes 100-250MB': 0}, 'modified': {'Category': 'modified', 'count 750KB-1MB': 0, 'bytes 250-500KB': 0, 'count 500-750KB': 0, 'bytes 1-10MB': 0, 'bytes <50KB': 0, 'count 1-10MB': 0, 'bytes 50-100KB': 0, 'bytes 50-100MB': 0, 'count 10-50MB': 0, 'count 250-500KB': 0, 'bytes 750KB-1MB': 0, 'count 50-100KB': 0, 'count <50KB': 0, 'bytes 250>': 0, 'count 50-100MB': 0, 'bytes 10-50MB': 0, 'count 250>': 0, 'bytes 100-250KB': 0, 'count 100-250KB': 0, 'bytes 500-750KB': 0, 'count 100-250MB': 0, 'bytes 100-250MB': 0}}
        statspath = os.path.join(os.getcwd(), 'test_snapshotstats/load_stats_good/')
        boundarypath = os.path.join(os.getcwd(), 'test_snapshotstats/boundaries.csv')
        listCategories = {'snapshot' : ['added', 'modified', 'unchanged', 'removed']}

        self.snapshotstats = snapshotstats.fromFile(boundarypath, listCategories, statspath)
      
        self.assertEqual(self.snapshotstats.stats, assertStats)
        pass

    def test_snapshotstats_load_bad(self):
        self.testname = sys._getframe().f_code.co_name
        self.assertRaises(IOError, self.snapshotstats.loadStats, 'test_snapshotstats/load_stats_bad/snapshot.csv')
        pass

    def test_snapshotstats_write_stats(self):
        self.testname = sys._getframe().f_code.co_name
        writepath = os.path.join(os.getcwd(), 'test_snapshotstats/write_stats/output/')
        assertStatsFile = os.path.join(os.getcwd(), 'test_snapshotstats/write_stats/expected/snapshot.csv')
        boundarypath = os.path.join(os.getcwd(), 'test_snapshotstats/boundaries.csv')
        listCategories = {'snapshot' : ['added', 'modified', 'unchanged', 'removed']}

        self.snapshotstats = snapshotstats.initWithCategories(boundarypath, listCategories)
        self.snapshotstats.export('snapshot', writepath)
        self.assertTrue(filecmp.cmp(os.path.join(writepath,"snapshot.csv"),  assertStatsFile), "output and expected file, do not match")
        pass

    def test_snapshotstats_update_stats(self):
        self.testname = sys._getframe().f_code.co_name
        writepath = os.path.join(os.getcwd(), 'test_snapshotstats/update_stats/output/')
        assertStatsFile = os.path.join(os.getcwd(), 'test_snapshotstats/update_stats/expected/snapshot.csv')
        boundarypath = os.path.join(os.getcwd(), 'test_snapshotstats/boundaries.csv')
        listCategories = {'snapshot' : ['added', 'modified', 'unchanged', 'removed']}

        self.snapshotstats = snapshotstats.initWithCategories(boundarypath, listCategories)
        self.snapshotstats.update('added', 1)
        self.snapshotstats.update('added', 1)
        self.snapshotstats.update('added', 1)
        self.snapshotstats.update('added', 1)
        self.snapshotstats.update('added', 1)
        self.snapshotstats.update('added', 50001)
        self.snapshotstats.update('added', 50001)
        self.snapshotstats.update('added', 50001)
        self.snapshotstats.update('added', 50001)
        self.snapshotstats.update('added', 50001)
        self.snapshotstats.update('added', 100001)
        self.snapshotstats.update('added', 100001)
        self.snapshotstats.update('added', 100001)
        self.snapshotstats.update('added', 100001)
        self.snapshotstats.update('added', 100001)
        self.snapshotstats.update('added', 250001)
        self.snapshotstats.update('added', 250001)
        self.snapshotstats.update('added', 250001)
        self.snapshotstats.update('added', 250001)
        self.snapshotstats.update('added', 500001)
        self.snapshotstats.update('added', 500001)
        self.snapshotstats.update('added', 500001)
        self.snapshotstats.update('added', 500001)
        self.snapshotstats.update('added', 750001)
        self.snapshotstats.update('added', 750001)
        self.snapshotstats.update('added', 750001)
        self.snapshotstats.update('added', 750001)
        self.snapshotstats.update('added', 1000001)
        self.snapshotstats.update('added', 1000001)
        self.snapshotstats.update('added', 1000001)
        self.snapshotstats.update('added', 1000001)
        self.snapshotstats.update('added', 10000001)
        self.snapshotstats.update('added', 10000001)
        self.snapshotstats.update('added', 10000001)
        self.snapshotstats.update('added', 10000001)
        self.snapshotstats.update('added', 50000001)
        self.snapshotstats.update('added', 50000001)
        self.snapshotstats.update('added', 50000001)
        self.snapshotstats.update('added', 50000001)
        self.snapshotstats.update('added', 100000001)
        self.snapshotstats.update('added', 100000001)
        self.snapshotstats.update('added', 100000001)
        self.snapshotstats.update('added', 100000001)
        self.snapshotstats.update('added', 250000001)
        self.snapshotstats.update('added', 250000001)
        self.snapshotstats.update('added', 250000001)
        self.snapshotstats.update('added', 250000001)

        self.snapshotstats.export('snapshot', writepath)
        self.assertTrue(filecmp.cmp(os.path.join(writepath,"snapshot.csv"),  assertStatsFile), "output and expected file, do not match")
        pass

    def test_snapshotstats_getBytes(self):
        self.testname = sys._getframe().f_code.co_name
        assertBytes = 1200
        boundarypath = os.path.join(os.getcwd(), 'test_snapshotstats/boundaries.csv')
        statspath = os.path.join(os.getcwd(), 'test_snapshotstats/getBytes/')
        listCategories = {'snapshot' : ['added', 'modified', 'unchanged', 'removed']}

        self.snapshotstats = snapshotstats.fromFile(boundarypath, listCategories, statspath)

        pass
unittest.main()
