#!/usr/bin/env python
#
# ArkCargo - filesystem stats
#
# MIT License, 
# the code to claculate a file MD5 checksum is based on code from 
# https://github.com/joswr1ght/md5deep/blob/master/md5deep.py
# 
# @author Chris Pates <chris.pates@arkivum.com>
# @copyright Arkivum Limited 2015


import os 
#, sys, hashlib, re, multiprocessing
import csv


class snapshotstats:

    def __init__(self):
        self.fields = {'category' : ''}
        self.boundaries = []
        self.categories = []
        self.sets = {}
        self.stats = {}

        return;

    @classmethod
    def initWithCategories(cls, boundariesFile, categories):
        initstats = snapshotstats()
        initstats.load_boundaries(boundariesFile)
        initstats.add_categories(categories)
        initstats.init()
        return initstats;

    @classmethod
    def fromFile(cls, boundariesFile, categories, statsPath):
        initstats = snapshotstats()
        initstats.load_boundaries(boundariesFile)
        initstats.add_categories(categories)
        initstats.load(statsPath)
        return cls;

    def load_boundaries(self, boundariesFile):
        countfields = {}
        bytesfields = {}

        try:
            with open(boundariesFile) as csvfile:
                statsBoundaries = csv.DictReader(csvfile)
                for row in statsBoundaries:
                    self.boundaries.append((row['Name'], row['Lower'], row['Upper']))
                    bytesfields['bytes '+row['Name']] = 0
                    countfields['count '+row['Name']] = 0

        except ValueError:
            sys.stderr.write("Can't load stats boundaries from file %s (error: %s)\n"%(boundariesFile, ValueError))
            sys.exit(-1)

        self.fields = ['Category'] + bytesfields.keys() + countfields.keys()

        return;

    def add_category(self, category):
        if (category not in self.categories):
            self.categories.append( category )
        return;

    def add_categories(self, categories):
        self.categories = list(set(self.categories) | set(categories))
        return;

    def add_set(self, name, setofcategories):
        if len(list(set(setofcategories) - set(self.categories))) == 0:
            self.sets[name] = setofcategories
        else:
            raise ValueError('Is not a known category')
        return;

    def add_sets(self, listofsets):
        for key in listofsets.keys():
            self.add_categories(listofsets[key].values())
            self.add_set(key, listofsets[key])
        return;
        
    def load(self, statspath):
        stats = {}
        fields =list(set(['Category']) | set(self.fields))
        # imports stats and boundaries, only relevant for 'rework' mode
        try:
            snapshotFile = os.path.join(statspath,"snapshot.csv")
            # First load in the stats for the snapshot (so far)
            with open(snapshotFile) as csvfile:
                statsImport = csv.DictReader(csvfile)
                for row in statsImport:
                    stats[row['Category']] = {}
                    for field in fields:
                        if field == 'Category':
                            stats[row['Category']][field] = row[field]
                        else:
                            stats[row['Category']][field] = int(row[field])

        except ValueError:
            sys.stderr.write("Can't import stats from %s (error: %s)\n"%(snapshotFile, ValueError))
            exit(-1)

        self.stats = stats
        return;


    def init(self):
        stats = {}

        # imports boundaries and initialise stats
        for category in self.categories:
            self.stats[category] = {}
            for field in self.fields:
                if field == 'Category':
                    self.stats[category]['Category'] = category
                else:   
                    self.stats[category][field] = 0
        self.stats = stats

        return;


    def update(category, size):
        bytes = int(size)

        for boundary in statsBoundaries:
            name, lower, upper = boundary
            if (bytes >= int(lower) and upper == '') or (bytes >= int(lower) and bytes < int(upper)):
                stats[category]['count '+name] += 1
                stats[category]['bytes '+name] += bytes
        return;



    def export(self, set, filepath):
        filemode = "wb"
        # exports stats based on categories listed in includeStats & includeStats 
        try:
            for statSet in self.sets:
                file = os.path.join(filepath,set,".csv")
                with open(filepath, filemode) as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=statsFields)
                    #writer.writeheader()
                    fields = {}
                    for field in self.sets['category']:
                        fields[field] = field
                    writer.writerow(fields)
                    for category in args.includeStats[statSet]:
                        writer.writerow(stats[category])
                 
        except ValueError:
            sys.stderr.write("Can't export stats to %s (error: %s)\n"%(filepath, ValueError))
        return;


