#!/usr/bin/env python

import os, sys, re, argparse
import fs
from checks import *

parser = argparse.ArgumentParser(prog=__file__, description='Analysis a failed file, and figure out why stuff failed.')
parser.add_argument('--version', action='version', version='%(prog)s 0.1.0')
parser.add_argument('-o', nargs='?', metavar='output directory', dest='output', type=str, default=os.path.join(os.getcwd(),"output"), help='the directory under which to write the output. <output directory>/<name>/<timestamp>/<output files>.')
parser.add_argument('--cargodiff', nargs='?', metavar='cargodiff to analysis', dest='cargodiff', type=str, help='the path to the cargo diff to be analysised.', required=True)
parser.add_argument('cargoJob', metavar='the path of the cargo job to evaluate.',  type=str, help='provide a path to in which the cargo job output is stored')

if __name__ == u'__main__':
    global args
    args = parser.parse_args()

    if not os.path.isdir(args.output):
        os.makedirs(args.output)

    failedPath = u""
    currentDir = args.cargoJob
 
    configPath = os.path.join(currentDir, u'config')
    if not os.path.isfile(configPath):
        sys.stderr.write('You not in a snapshot metadata directory\n')
        exit();

    config = readConfig(configPath)
    snapshotDir = config['snapshotCurrentPath']
 
    if os.path.isdir(os.path.join(currentDir, u'snapshot')):
        failedPath = os.path.join(currentDir, u'snapshot', u'failed')
    else:
        # Just check whether this is an older mkcargo output
        failedPath = os.path.join(currentDir, u'failed')

    if not os.path.isfile(args.cargodiff):
        print u"There is no cargodiff file to process"
        exit();

    print u"Processing failed files for %s-%s..."%(config['dataset'], config['job'])
    processCargoDiff(args.output, snapshotDir, args.cargodiff)

    print u"finished!"   
