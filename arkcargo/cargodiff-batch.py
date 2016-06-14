#!/usr/bin/env python

import os, sys, re, argparse
import fs
from checks import *

parser = argparse.ArgumentParser(prog=__file__, description='Analysis a failed file, and figure out why stuff failed.')
parser.add_argument('--version', action='version', version='%(prog)s 0.1.0')
parser.add_argument('--wip', nargs='?', metavar='wip directory', dest='wipPath', type=str, default=os.path.join(os.getcwd(),"output"), help='the directory under which to write the output. <output directory>/<name>/<timestamp>/<output files>.')
parser.add_argument('--cargodiffs', nargs='?', metavar='cargodiff to analysis', dest='cargodiffPath', type=str, help='the path to the cargo diff to be analysised.', required=True)
parser.add_argument('--snapMetadata', nargs='?', metavar='snapshot Meta data folder', dest='snapMetadataPath', type=str, help='the path to the snapshot Metadata.', required=True)
parser.add_argument('--review', nargs='?', metavar='review folder to move output to', dest='reviewPath', type=str, help='the path to the cargo diff to be analysised.', required=True)

def processDataset(dataset):
    datasetPath = os.path.join(args.snapMetadataPath, dataset)
    regexJob = re.compile("([a-zA-Z0-9_\-]*)")

    if os.path.isdir(datasetPath):
        jobList = [f for f in os.listdir(datasetPath) if re.match(regexJob, f)]
        for job in jobList:
            print "Processing : %s - %s"%(dataset, job)
            processDatasetJob(dataset, job)
          

def processDatasetJob(dataset, job):
    regexPattern = "^(%s)-(%s)(?:-(\d{6}))?(?:\.md5)"%(dataset, job)
    regexCargo = re.compile(regexPattern)

    outputPath = os.path.join(args.wipPath, "%s-%s.rework"%(dataset, job))
    datasetJob = os.path.join(args.snapMetadataPath, dataset, job)
    processCargoJob(outputPath, datasetJob, args.cargodiffPath)


if __name__ == '__main__':
    global args
    args = parser.parse_args()
    regexDataset = re.compile("([a-zA-Z0-9_\-]*)")

    if not os.path.isdir(args.wipPath):
        os.makedirs(args.wipPath)

    snapshots = args.snapMetadataPath
    cargodiffPath = args.cargodiffPath

    if not os.path.isdir(snapshots):
        sys.stderr.write('--snapMetadata location does not exist')

    if not os.path.isdir(cargodiffPath):
        sys.stderr.write('--cargodiffs location does not exist')

    datasetList = [f for f in os.listdir(snapshots) if re.match(regexDataset, f)]

    for dataset in datasetList:
        processDataset(dataset)
        print "finished!"   
