#!/usr/bin/env python
#
#  snapshotstats  Identifying information about tests here.
#
#===============
#  This is based on a skeleton test file, more information at:
#
#     https://github.com/linsomniac/python-unittest-skeleton


import unittest2 as unittest

import os
import sys
sys.path.append('../arkcargo')      
from snapshotstats import snapshotstats


class test_snapshotstats(unittest.TestCase):
    def setUp(self):
        self.snapshotstats = snapshotstats()
        pass

    def tearDown(self):
        ###  XXX code to do tear down
        pass

    def test_snapshotstats_loadboundaries_bad(self):
        self.assertRaises(IOError, self.snapshotstats.load_boundaries, 'non-sense.csv')
        pass

    def test_snapshotstats_loadboundaries_good(self):
        assertBoundaries = [('<50KB', '0', '50000'), ('50-100KB', '50000', '100000'), ('100-250KB', '100000', '250000'), ('250-500KB', '250000', '500000'), ('500-750KB', '500000', '750000'), ('750KB-1MB', '750000', '1000000'), ('1-10MB', '1000000', '10000000'), ('10-50MB', '10000000', '50000000'), ('50-100MB', '50000000', '100000000'), ('100-250MB', '100000000', '250000000'), ('250>', '250000000', '')]
        boundarypath = os.path.join(os.getcwd(), 'test_snapshotstats/datasets/loadboundaries_good/boundaries.csv')

        self.snapshotstats.load_boundaries(boundarypath)
        self.assertEqual(self.snapshotstats.boundaries, assertBoundaries)
        pass

    def test_snapshotstats_loadcategories(self):
        assertCategories = ['added', 'modified', 'unchanged', 'removed']
        for category in assertCategories:
            self.snapshotstats.add_category(category)
        self.assertEqual(self.snapshotstats.categories, assertCategories) 
        pass

    def test_snapshotstats_set_good(self):
        listCategories = ['added', 'modified', 'unchanged', 'removed']
        snapshot_set = ['added', 'modified', 'unchanged']
        
        self.snapshotstats.add_categories(listCategories)
        self.snapshotstats.add_set('snapshot', snapshot_set)

        self.assertTrue(self.snapshotstats.sets['snapshot'], snapshot_set)
        pass

    def test_snapshotstats_set_bad(self):
        listCategories = ['added', 'modified', 'unchanged', 'removed']
        snapshot_set = ['unchanged', 'invisible']

        self.snapshotstats.add_categories(listCategories)

        self.assertRaises(ValueError, self.snapshotstats.add_set, 'snapshot', snapshot_set)
        pass

    def test_snapshotstats_init_stats(self):
        boundarypath = os.path.join(os.getcwd(), 'test_snapshotstats/datasets/init_stats_good/boundaries.csv')
        setCategories = {'snapshot' : ['added', 'modified', 'unchanged', 'removed']}
        self.snapshotstats.initWithCategories(boundarypath, setCategories)
#        self.assertEqual(self.snapshotstats.stats, assertStats)
        pass

    def test_snapshotstats_init_stats_good(self):
        boundarypath = os.path.join(os.getcwd(), 'test_snapshotstats/datasets/init_stats_good/boundaries.csv')
        listCategories = ['added', 'modified', 'unchanged', 'removed']
        assertStats = {'unchanged': {'count 750KB-1MB': 0, 'bytes 250-500KB': 0, 'count 500-750KB': 0, 'bytes 1-10MB': 0, 'category': 0, 'bytes <50KB': 0, 'count 1-10MB': 0, 'bytes 50-100KB': 0, 'bytes 50-100MB': 0, 'count 10-50MB': 0, 'count 250-500KB': 0, 'bytes 750KB-1MB': 0, 'count 50-100KB': 0, 'count <50KB': 0, 'bytes 250>': 0, 'count 50-100MB': 0, 'bytes 10-50MB': 0, 'count 250>': 0, 'bytes 100-250KB': 0, 'count 100-250KB': 0, 'bytes 500-750KB': 0, 'count 100-250MB': 0, 'bytes 100-250MB': 0}, 'added': {'count 750KB-1MB': 0, 'bytes 250-500KB': 0, 'count 500-750KB': 0, 'bytes 1-10MB': 0, 'category': 0, 'bytes <50KB': 0, 'count 1-10MB': 0, 'bytes 50-100KB': 0, 'bytes 50-100MB': 0, 'count 10-50MB': 0, 'count 250-500KB': 0, 'bytes 750KB-1MB': 0, 'count 50-100KB': 0, 'count <50KB': 0, 'bytes 250>': 0, 'count 50-100MB': 0, 'bytes 10-50MB': 0, 'count 250>': 0, 'bytes 100-250KB': 0, 'count 100-250KB': 0, 'bytes 500-750KB': 0, 'count 100-250MB': 0, 'bytes 100-250MB': 0}, 'removed': {'count 750KB-1MB': 0, 'bytes 250-500KB': 0, 'count 500-750KB': 0, 'bytes 1-10MB': 0, 'category': 0, 'bytes <50KB': 0, 'count 1-10MB': 0, 'bytes 50-100KB': 0, 'bytes 50-100MB': 0, 'count 10-50MB': 0, 'count 250-500KB': 0, 'bytes 750KB-1MB': 0, 'count 50-100KB': 0, 'count <50KB': 0, 'bytes 250>': 0, 'count 50-100MB': 0, 'bytes 10-50MB': 0, 'count 250>': 0, 'bytes 100-250KB': 0, 'count 100-250KB': 0, 'bytes 500-750KB': 0, 'count 100-250MB': 0, 'bytes 100-250MB': 0}, 'modified': {'count 750KB-1MB': 0, 'bytes 250-500KB': 0, 'count 500-750KB': 0, 'bytes 1-10MB': 0, 'category': 0, 'bytes <50KB': 0, 'count 1-10MB': 0, 'bytes 50-100KB': 0, 'bytes 50-100MB': 0, 'count 10-50MB': 0, 'count 250-500KB': 0, 'bytes 750KB-1MB': 0, 'count 50-100KB': 0, 'count <50KB': 0, 'bytes 250>': 0, 'count 50-100MB': 0, 'bytes 10-50MB': 0, 'count 250>': 0, 'bytes 100-250KB': 0, 'count 100-250KB': 0, 'bytes 500-750KB': 0, 'count 100-250MB': 0, 'bytes 100-250MB': 0}}
        self.snapshotstats.load_boundaries(boundarypath)
        self.snapshotstats.add_categories(listCategories)
        self.snapshotstats.init()
#        self.assertEqual(self.snapshotstats.stats, assertStats)
        pass
    
    def test_snapshotstats_load_stats_good(self):
        statspath = os.path.join(os.getcwd(), 'test_snapshotstats/datasets/load_stats_good/')
        boundarypath = os.path.join(os.getcwd(), 'test_snapshotstats/datasets/load_stats_good/boundaries.csv')
        listCategories = ['added', 'modified', 'unchanged', 'removed']

        self.snapshotstats.load_boundaries(boundarypath)

        self.snapshotstats.add_categories(listCategories)
        self.snapshotstats.load(statspath)
        print self.snapshotstats.stats
        #self.assertEqual(self.snapshotstats.stats, assertStats)
        pass

    def test_snapshotstats_load_stats_good_singlestep(self):
        statspath = os.path.join(os.getcwd(), 'test_snapshotstats/datasets/load_stats_good/')
        boundarypath = os.path.join(os.getcwd(), 'test_snapshotstats/datasets/load_stats_good/boundaries.csv')
        listCategories = {'snapshot' : ['added', 'modified', 'unchanged', 'removed']}

        self.snapshotstats.fromFile(boundarypath, listCategories, statspath)

        #self.assertEqual(self.snapshotstats.stats, assertStats)
        pass

    def test_snapshotstats_load_bad(self):
        self.assertRaises(IOError, self.snapshotstats.load, 'test_snapshotstats/datasets/load_stats_bad/snapshot.csv')
        pass


unittest.main()
