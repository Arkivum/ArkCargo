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
        self.fields = {'Category' : ''}
        self.boundaries = []
        self.categories = []
        self.sets = {}
        self.stats = {}

        return;

    @classmethod
    def initWithCategories(cls, boundariesFile, sets):
        initstats = snapshotstats()
        initstats.load_boundaries(boundariesFile)
        initstats.add_sets(sets)
        initstats.initStats()
        return initstats;

    @classmethod
    def fromFile(cls, boundariesFile, sets, statsPath):
        initstats = snapshotstats()
        initstats.load_boundaries(boundariesFile)
        initstats.add_sets(sets)
        initstats.loadStats(statsPath)
        return initstats;

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
        for category in categories:
            if category not in self.categories:
		self.categories.append(category)
        return;

    def add_set(self, name, setofcategories):
        if len(list(set(setofcategories) - set(self.categories))) == 0:
            self.sets[name] = setofcategories
        else:
            raise ValueError('Is not a known category')
        return;

    def add_sets(self, listofsets):
        for key in listofsets.keys():
            self.add_categories(listofsets[key])
            self.add_set(key, listofsets[key])
        return;
        
    def loadStats(self, statspath):
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


    def initStats(self):
        newStats = {}

        # imports boundaries and initialise stats
        for category in self.categories:
            newStats[category] = {}
            for field in self.fields:
                if field == 'Category':
                    newStats[category]['Category'] = category
                else:   
                    newStats[category][field] = 0
        self.stats = newStats
        return;


    def update(self, category, size):
        bytes = int(size)

        for boundary in self.boundaries:
            name, lower, upper = boundary
            if (bytes >= int(lower) and upper == '') or (bytes >= int(lower) and bytes < int(upper)):
                self.stats[category]['count '+name] += 1
                self.stats[category]['bytes '+name] += bytes
        return;



    def export(self, set, outputPath):
        filemode = "wb"
        # exports stats based on categories listed in includeStats & includeStats 
        try:
            outputFile = os.path.join(outputPath,set+'.csv')
            with open(outputFile, filemode) as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=self.fields)
                fields = {}
                for field in self.fields:
                    fields[field] = field
                writer.writerow(fields)
                for category in self.sets[set]:
                    writer.writerow(self.stats[category])
                 
        except ValueError:
            sys.stderr.write("Can't export stats to %s (error: %s)\n"%(filepath, ValueError))
        return;
	
    def exportAll(self, outputPath):
        for set in self.sets.keys():
            self.export(set, outputPath)
        return;
