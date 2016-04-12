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
    shortArgs = "o:q:n:f:"
    longArgs = ["outdir=", "qsub=", "name=", "config-file="]
    try:
        opts,args = getopt.getopt(argv, shortArgs, longArgs)
    except getopt.GetoptError:
        m = "Unable to extract command line args."
        raise ctip_utils.ParseError('args', m)

    whereClause = getWhereClause(args)

    #
    # Get the config table name if present
    #
    configTable = None
    try:
        configTable = getPrimaryArg(args)
    except ctip_utils.ParseError:
        pass
    #
    # Parse command line args
    #
    outDir = ""
    qsubFile = None
    session_name = None
    configFileName = None
    for opt, arg in opts:
        if opt in ("-o", "--outdir"):
            outDir = arg
        elif opt in ("-q", "--qsub"):
            qsubFile = arg
        elif opt in ("-n", "--name"):
            session_name = arg
        elif opt in ("-f", "--config-file"):
            configFileName = arg
    #
    # See if they've called run correctly and handle a config file
    #
    if configTable and configFileName:
        m = "Cannot specify both a config table and a config file to run."
        raise ctip_utils.ParseError('args', m)
    elif configFileName:
        with open(configFileName, 'r') as configFile:
            configTable = ctip_utils.createConfigTable(configFile)
    #
    # Initialize the test session!
    #
    ctip_utils.initTestSession(runConfig,
            configTable,
            whereClause,
            outDir,
            qsubFile,
            session_name)
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
    whereClause = getWhereClause(argv)
    configTableName = getPrimaryArg(argv)

    db = ctip_utils.DatabaseManager()
    db.printTable(configTableName, whereClause)

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
        where = ""
        if r['where_clause']:
            where = r['where_clause']
        print("{0:>12}: {1}".format('id', r['id']))
        print("{0:>12}: {1}".format('session name', r['name']))
        print("{0:>12}: {1} {2}".format('configs', r['config_group'],where))
        print("{0:>12}: {1}".format('date', r['date']))

        total = 0
        for line in summary:
            total += line['count()']
        for line in summary:
            percent = percentString(line['count()'], total)
            print("{0:>12}: {1}".format(line['status'], percent))
    else:
        line_format = "{0:<6} {1:8} {2:8} {3:8} {4:8}"
        cols = ['id','queued','running','done','other'] 
        seps = ['-'*5]
        for i in range(4):
            seps.append('-' * 7)
        print(line_format.format(*cols))
        print(line_format.format(*seps))

        reports = {}
        for line in summary:
            session = line['id']
            status = line['status']
            count = line['count()']

            if session not in reports:
                reports[session] = [0] * 5

            reports[session][4] += count
            if status == 'queued':
                reports[session][0] = count
            elif status == 'running':
                reports[session][1] = count
            elif status == 'done':
                reports[session][2] = count
            else:
                reports[session][3] = count

        for id,report in reports.items():
            total = report[4]
            q = percentString(report[0], total)
            r = percentString(report[1], total)
            d = percentString(report[2], total)
            o = percentString(report[3], total)
            print(line_format.format(id, q, r, d, o))

def percentString(part, whole):
    return "{0:.0f}%".format(part/float(whole) * 100)

def update_status(argv):
    if len(argv) != 2:
        raise ctip_utils.CTIPError("update-status requires a job id and valid status")
    db = ctip_utils.DatabaseManager()
    db.updateJobStatus(argv[0],argv[1])

def update_id(argv):
    if len(argv) != 2:
        raise ctip_utils.CTIPError("update-id requires a job id and new job id")
    db = ctip_utils.DatabaseManager()
    db.updateJobId(argv[0],argv[1])

def log_start(argv):
    if len(argv) != 1:
        raise ctip_utils.CTIPError("log-start only requires a job id")
    db = ctip_utils.DatabaseManager()
    db.startJob(argv[0])

def log_pause(argv):
    if len(argv) != 1:
        raise ctip_utils.CTIPError("log-pause only requires a job id")
    db = ctip_utils.DatabaseManager()
    db.pauseJob(argv[0])

def log_resume(argv):
    if len(argv) != 1:
        raise ctip_utils.CTIPError("log-resume only requires a job id")
    db = ctip_utils.DatabaseManager()
    db.resumeJob(argv[0])

def log_end(argv):
    if len(argv) != 1:
        raise ctip_utils.CTIPError("log-end only requires a job id")
    db = ctip_utils.DatabaseManager()
    db.endJob(argv[0])

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

def getWhereClause(args):
    """Get the where clause if present."""
    whereClause = ""
    for s in args:
        if string.lower(s).startswith("where"):
            whereClause = s
            break
    if whereClause:
        args.remove(whereClause)
    return whereClause

