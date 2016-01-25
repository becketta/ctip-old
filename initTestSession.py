#!/usr/bin/python

import sqlite3 as lite
import sys, os
import getopt
import datetime
import csv
from multiprocessing import Process

whereClause = ""

configDatabase = "test.db"
p3executable = "~/MarkovBrain/p3brain"
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
    shortArgs = "o:f:g"
    longArgs = ["outdir=", "config-file=", "generate"]

    # Get command line args from sys.argv with getopt
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
                configTable = generateConfigTable(configFile)
            else:
                configTable = createConfigTable(configFile)

    #
    # Get configs from the database
    #
    colnames = []
    coltypes = []
    configs = []

    conn = lite.connect(configDatabase)
    with conn:

        conn.row_factory = lite.Row
        cur = conn.cursor()

        # Get the column info
        cur.execute("PRAGMA table_info(" + configTable + ")")
        columns = cur.fetchall()
        for col in columns:
            colnames.append(col[1])
            coltypes.append(col[2])

        # Get the relevant configurations
        #   -> if whereClause is "", get all the configs in the table
        cur.execute("SELECT * FROM " + configTable + " " + whereClause)
        configs = cur.fetchall()

    #
    # Create a folder for this test session based on the date and time
    #   -> The directory being created is:
    #           <outDir>/<configTable>/<date_time>/
    #   -> Each config will create a folder in this directory
    #
    now = datetime.datetime.now()
    sec = now.second
    timestamp = now.strftime("%Y-%m-%d_%H:%M:") + str(sec)

    dir = os.path.join(outDir, configTable, timestamp)
    try:
        os.makedirs(dir)
    except OSError:
        if not os.path.isdir(dir):
            raise RuntimeError('Could not create the output directory')

    #
    # Store text file snapshot of config table at root session dir
    #
    summaryPath = os.path.join(dir, "configs.txt")
    procs = []
    with open(summaryPath, 'w') as summary:
        writer = csv.writer(summary)
        writer.writerow([configTable])
        writer.writerow(colnames)
        writer.writerow(coltypes)
        for config in configs:
            writer.writerow(list(config))
            p = Process(target=runConfig, args=(config, outDir))
            p.start()
            procs.append(p)

    print "Tests running!"

    #
    # Wait for each configuration run to finish
    #
    for p in procs:
        p.join()

    print "Tests complete!"

def createConfigTable(csv_file):
    """Creates new config table filled with the csv file configs"""
    configTableName = ""
    reader = csv.reader(csv_file)

    # Get the name of this group of configs
    row = reader.next()
    if len(row) == 1:
        configTableName = row[0]
    else:
        raise RuntimeError('First line of csv file must be 

def generateConfigTable(csv_file):
    """Generates new config table of all combinations of csv config values"""
    raise NotImplementedError

if __name__ == "__main__":
    main(sys.argv)

