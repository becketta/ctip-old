#!/usr/bin/python
#
# Created by Aaron Beckett January, 2016
#

import sys, os
import getopt
import datetime
from multiprocessing import Process

from ctip_utils import CTIPDatabaseManager

whereClause = ""

p3executable = "~/MarkovBrain/testing/p3brain"
defaultConfig = "~/MarkovBrain/P3Brain/FastEfficientP3/config/default.cfg"

helpText = """
initTestSession.py [-o <out_dir>] <config_table_name>
initTestSession.py [-o <out_dir>] [-g] -f <config_file>

config_table_name
    Name of the configuration table in the data base that stores
    the configuration to be run by this script.

-f, --config-file
    Name of a csv file with configurations to run. The configurations
    from this file will be used to fill a new sqlite table with the
    config table name.

-g, --generate
    If this option is present along with the -f option, the csv file is
    assumed to contain each possible value for each config option
    and the generated table of configurations will have all possible
    combinations of these options.

-o, --outdir
    Directory in which to place output files.
"""

def runConfig(config, outdir="", tag=""):
    """Run a program using given configuration"""
    print "Run config " + str(config["id"])

def main(argv):
    #
    # Get an instance of the ctip local database manager
    #
    manager = CTIPDatabaseManager()

    #
    # Get command line args from sys.argv with getopt
    #
    shortArgs = "o:f:g"
    longArgs = ["outdir=", "config-file=", "generate"]
    try:
        opts,args = getopt.getopt(argv[1:], shortArgs, longArgs)
    except getopt.GetoptError:
        print helpText
        sys.exit(2)
    
    #
    # Parse command line args
    #
    configTable = ""
    outDir = ""
    configFileName = ""
    generate = False

    for opt, arg in opts:
        if opt in ("-o", "--outdir"):
            outDir = arg
        elif opt in ("-f", "--config-file"):
            configFileName = arg
        elif opt in ("-g", "--generate"):
            generate = True

    if len(args) == 1:
        configTable = args[0]

    if configTable != "" and configFileName != "":
        print helpText
        sys.exit(2)
    elif configFileName != "":
        with open(configFileName, 'r') as configFile:
            if generate:
                configTable = manager.generateConfigTable(configFile)
            else:
                configTable = manager.createConfigTable(configFile)

    #
    # Get configs from the database
    #
    colnames,coltypes,configs = manager.getConfigs(configTable, whereClause)

    #
    # Create a folder for this test session based on the date and time
    #   -> The directory being created is:
    #           <outDir>/<configTable>/<date_time>/
    #   -> Each config will create a folder in this directory
    #
    now = datetime.datetime.now()
    sec = now.second
    timestamp = now.strftime("%Y-%m-%d_%H:%M:") + str(sec)

    testBatchDir = os.path.join(outDir, configTable, timestamp)
    try:
        os.makedirs(testBatchDir)
    except OSError:
        if not os.path.isdir(testBatchDir):
            raise RuntimeError('Could not create the output directory')

    #
    # Store text file snapshot of config table at root session dir
    #
    snapshotPath = os.path.join(testBatchDir, "configs.csv")
    with open(snapshotPath, 'w') as sf:
        manager.writeConfigCsv(sf, configTable, colnames, coltypes, configs)

    #
    # Spawn processes for each config to test
    #
    procs = []
    for config in configs:
        p = Process(target=runConfig, args=(config, testBatchDir))
        p.start()
        procs.append(p)

    print "Tests running!"

    #
    # Wait for each configuration run to finish
    #
    for p in procs:
        p.join()

    print "Tests complete!"


if __name__ == "__main__":
    main(sys.argv)

