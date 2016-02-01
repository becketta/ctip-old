#
# Created by Aaron Beckett January, 2016
#
import getopt

import ctip_utils
from run_config import runConfig

whereClause = ""

def run(argv):
    #
    # Get command line args with getopt
    #
    shortArgs = "o:f:"
    longArgs = ["outdir=", "config-file="]
    try:
        opts,args = getopt.getopt(argv, shortArgs, longArgs)
    except getopt.GetoptError:
        print helpText
        sys.exit(2)
    #
    # Parse command line args
    #
    configTable = ""
    outDir = ""
    configFileName = ""

    for opt, arg in opts:
        if opt in ("-o", "--outdir"):
            outDir = arg
        elif opt in ("-f", "--config-file"):
            configFileName = arg

    if len(args) == 1:
        configTable = args[0]

    if configTable != "" and configFileName != "":
        print helpText
        sys.exit(2)
    elif configFileName != "":
        with open(configFileName, 'r') as configFile:
            configTable = ctip.createConfigTable(configFile)
    #
    # Initialize the test session!
    #
    ctip_utils.initTestSession(runConfig, configTable, whereClause, outDir)
    print "Jobs submitted!"

def gen(argv):
    #
    # Get the file with the config table schema
    #
    genfileName = ""
    if len(argv) == 1:
        genfileName = argv[0]

    try:
        with open(genfileName):
            pass
    except IOError:
        pass

    print "Gen"

def info(argv):
    print "Info"

def check(argv):
    print "Check"

