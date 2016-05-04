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


class boundaries:

    def __init__(self):
        self.fields = {'Category' : ''}
        self.boundaries = []
        return;

    @classmethod
    def initWithBoundaries(cls, boundariesFile):
        initBoundaries = boundaries()
        initBoundaries.load(boundariesFile)
        initBoundaries.init()
        return initBoundaries;
    
    def load(self, boundariesFile):
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

    def getBoundaries(self):
        return self.boundaries;

    def getFields(self):
        return self.fields;

    def init(self):

        return;


class stats:

    def __init__(self):
        self.fields = {'Category' : ''}
        self.boundaries = []
        self.categories = []
        self.stats = {}
        self.bytes = {}
        self.counts = {}
        return;

    @classmethod
    def initWithBoundaries(cls, boundaries):
        initStats = stats()
        initStats.boundaries = boundaries.getBoundaries()
        initStats.fields = boundaries.getFields()
        initStats.init()
        return initStats;

    @classmethod
    def initWithCategories(cls, boundaries, categories):
        initStats = stats()
        initStats.boundaries = boundaries.getBoundaries()
        initStats.fields = boundaries.getFields()
        initStats.addCategories(categories)
        initStats.init()
        return initStats;


    def setBoundaries(self, boundaries):
        self.boundaries = boundaries.getBoundaries()
        self.fields = boundaries.getFields()
        return;

    def getBoundaries(self):
        return self.boundaries;

    def addCategory(self, category):
        if (category not in self.categories):
            self.categories.append( category )
        return;

    def addCategories(self, categories):
        for category in categories:
            if category not in self.categories:
		self.categories.append(category)
        return;

    def getCategories(self):
        return self.categories;

    def init(self):
        newStats = {}
        newBytes = {}
        newCounts = {}

        # imports boundaries and initialise stats
        for category in self.categories:
            newBytes[category] = 0
            newCounts[category] = 0
            newStats[category] = {}
            for field in self.fields:
                if field == 'Category':
                    newStats[category]['Category'] = category
                else:   
                    newStats[category][field] = 0
        self.stats = newStats
        self.bytes = newBytes
        self.counts = newCounts
        return;


    def update(self, category, size):
        bytes = int(size)

        if category in self.categories:
            for boundary in self.boundaries:
                name, lower, upper = boundary
                if (bytes >= int(lower) and upper == '') or (bytes >= int(lower) and bytes < int(upper)):
                    self.stats[category]['count '+name] += 1
                    self.stats[category]['bytes '+name] += bytes
            self.bytes[category] += bytes
            self.counts[category] += 1 
        return;

    def export(self, name, path):
        filemode = "wb"
        # exports stats based on categories listed in includeStats & includeStats 
        try:
            outputFile = os.path.join(path,name+'.csv')
            with open(outputFile, filemode) as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=self.fields)
                fields = {}
                for field in self.fields:
                    fields[field] = field
                writer.writerow(fields)
                for category in self.categories:
                    writer.writerow(self.stats[category])
        except ValueError:
            sys.stderr.write("Can't export stats to %s (error: %s)\n"%(filepath, ValueError))
        return;

    def getBytesFor(self, category):
        if category in self.categories:
            return self.bytes[category];

    def getBytesAll(self):
        totalBytes =0
        for category in self.categories:
            totalBytes += self.bytes[category]
        return totalBytes;

    def getCountFor(self, category):
        if category in self.categories:
            return self.counts[category];

    def getCountAll(self):
        totalCounts =0
        for category in self.categories:
            totalCounts += self.counts[category]
        return totalCounts;
