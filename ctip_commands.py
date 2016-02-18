#
# Created by Aaron Beckett January, 2016
#
import getopt
import string

import ctip_utils
from run_config import runConfig

def run(argv):
    #
    # Get command line args with getopt
    #
    shortArgs = "o:f:"
    longArgs = ["outdir=", "config-file="]
    try:
        opts,args = getopt.getopt(argv, shortArgs, longArgs)
    except getopt.GetoptError:
        m = "Unable to extract command line args."
        raise ctip_utils.ParseError('args', m)
    #
    # Get the where clause if present
    #
    whereClause = ""
    for s in args:
        if string.lower(s).startswith("where"):
            whereClause = s
            break
    if whereClause:
        args.remove(whereClause)
    #
    # Get the config table name if present
    #
    configTable = ""
    try:
        configTable = getPrimaryArg(args)
    except ctip_utils.ParseError:
        pass
    #
    # Parse command line args
    #
    outDir = ""
    configFileName = ""
    for opt, arg in opts:
        if opt in ("-o", "--outdir"):
            outDir = arg
        elif opt in ("-f", "--config-file"):
            configFileName = arg
    #
    # See if they've called run correctly and handle a config file
    #
    if configTable != "" and configFileName != "":
        m = "Cannot specify both a config table and a config file to run."
        raise ctip_utils.ParseError('args', m)
    elif configFileName != "":
        with open(configFileName, 'r') as configFile:
            configTable = ctip_utils.createConfigTable(configFile)
    #
    # Initialize the test session!
    #
    ctip_utils.initTestSession(runConfig, configTable, whereClause, outDir)
    print "Jobs submitted!"

def gen(argv):
    genfileName = getPrimaryArg(argv)

    try:
        with open(genfileName, 'r') as configSchema:
            ctip_utils.generateConfigTable(configSchema)
    except IOError:
        pass

def tables(argv):
    db = ctip_utils.DatabaseManager()
    db.listConfigTables()

def list(argv):
    configTableName = getPrimaryArg(argv)

    db = ctip_utils.DatabaseManager()
    db.printTable(configTableName)

def save(argv):
    #
    # Get command line args with getopt
    #
    shortArgs = "o:"
    longArgs = ["outfile="]
    try:
        opts,args = getopt.getopt(argv, shortArgs, longArgs)
    except getopt.GetoptError:
        m = "Unable to extract command line args."
        raise ctip_utils.ParseError('args', m)
    #
    # Parse command line args
    #
    configTableName = getPrimaryArg(args)
    outFileName = ""
    for opt, arg in opts:
        if opt in ("-o", "--outfile"):
            outFileName = arg
    if not outFileName:
        outFileName = configTableName + '.csv'

    with open(outFileName, 'w') as outfile:
        ctip_utils.storeSnapshot(configTableName, outfile)

def check(argv):
    only_one = False
    try:
        session_id = getPrimaryArg(argv)
        summary = ctip_utils.checkSession(session_id)
        only_one = True
    except ctip_utils.ParseError:
        summary = ctip_utils.checkSession()

    if only_one:
        r = summary[0]
        print("     id: {0}".format(r['id']))
        print("configs: {0} {1}".format(r['config_group'],r['where_clause']))
        print("   date: {0}".format(r['date']))

        total = 0
        for line in summary:
            total += line['count()']
        for line in summary:
            percent = "{0:.0f}%".format(line['count()']/float(total) * 100)
            print("{0}: {1}".format(line['status'], percent))
    else:
        # TODO: print shorted summary of all sessions
        pass

def status(argv):
    if len(argv) != 2:
        raise ctip_utils.CTIPError("ctip status requires a job id and valid status")

    db = ctip_utils.DatabaseManager()
    db.updateJobStatus(argv[0],argv[1])

def clean(argv):
    db = ctip_utils.DatabaseManager()
    try:
        session_id = getPrimaryArg(argv)
        db.deleteSession(session_id)
    except ctip_utils.ParseError:
        db.deleteFinishedSessions()

def getPrimaryArg(argv):
    """Gets the first argument in args."""
    configTableName = ""
    if len(argv) == 1:
        configTableName = argv[0]
    else:
        raise ctip_utils.ParseError("args", "Unable to parse table name.")

    return configTableName

