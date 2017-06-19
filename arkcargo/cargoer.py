#!/usr/bin/env python
#
# ArkCargo - cargoer
#
# @author Chris Pates <chris.pates@arkivum.com>
# @copyright Arkivum Limited 2016
#
# reads a arkcargo-ingestSet
# arckargo-ingestSet - set of files that list files to be cargoed (by cargoer) each 'chunk'
#                      representing and approximate volume for files e.g. 10GB
#
# For each arkcargo-ingestSet file produces a cargo file for ingest, where a cargo file is in MD5deep format
#
import cargoWriter from cargo

#initialise
parser = argparse.ArgumentParser(prog='mkcargo.py', description='Analysis a filesystem and create a cargo file to drive an ingest job.')

#ArkCargo-config
parser.add_argument('--version', action='version', version='%(prog)s 0.5.0')
parser.set_defaults(cargoEOL = '\0')
parser.set_defaults(cargoPad = 6)
parser.set_defaults(ingestExt = '.ingest')
parser.set_defaults(cargoExt = '.md5')

#System-config
parser.add_argument('-j', metavar='threads', nargs='?', dest='threads', type=int, default=(max(multiprocessing.cpu_count(),1)), help='Controls multi-threading. By default the program will create one thread per CPU core, one thread is used for writing to the output files, the rest for scanning the file system and calculating MD5 hashes. Multi-threading causes output filenames to be in non-deterministic order, as files that take longer to hash will be delayed while they are hashed.')
parser.add_argument('-i', nargs='?', metavar='ingest file', dest='ingestFile', type=str, default="", help='the file to be cargoed.')
parser.add_argument('-w', nargs='?', metavar='work-in-progress directory', dest='wipDir', type=str, default="", help='the directory under which to store the work in progress prior to completion.')
parser.add_argument('-o', nargs='?', metavar='output directory', dest='outputDir', type=str, default="output", help='the directory under which to write the output.')
parser.add_argument('-log', nargs='?', metavar='logging directory', dest='logDir', type=str, default="log", help='the directory under which to write any logs generated, log only generated on error.')

#Dataset-config 

#read cargoer section of config



