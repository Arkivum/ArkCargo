#!/usr/bin/env python
#
# ArkCargo - snapshotdelta
#
# MIT License, 
# 
# @author Chris Pates <chris.pates@arkivum.com>
# @copyright Arkivum Limited 2015


import os, sys, hashlib, re, multiprocessing
import filecmp
import csv


class snapshotdelta:

    def __init__(self):
        self.fields = {'category' : ''}
        self.boundaries = []
        self.categories = []
        self.sets = {}
        self.stats = {}



