#
# Created by Aaron Beckett January, 2016
#

import os
import csv
import datetime
from subprocess import Popen, PIPE
from multiprocessing import Process, Queue
from Queue import Empty as QueueEmpty
from copy import deepcopy

from ctip_utils import *
import ctip_constants as ctip
from ctip_dbm import DatabaseManager

###########################################################
#   CTIP Functions
#

def storeSnapshot(table, outfile):
    """Store the contents of a config table as a csv file."""
    db = DatabaseManager()
    cols,configs = db.getRecords(table)
    writeConfigCsv(outfile, table, cols, configs)
    
def writeConfigCsv(outfile, tablename, cols, configs):
    """Write the given table info in csv format."""
    writer = csv.writer(outfile)
    writer.writerow([tablename])
    writer.writerow(cols)
    for config in configs:
        writer.writerow(list(config))
        
def createConfigTable(csv_file):
    """Creates new config table from a properly formatted csv file."""
    reader = csv.reader(csv_file)

    configTableName = parseTableName(reader)
    
    # Get the column names 
    cols = next(reader)

    configs = []
    for config in reader:
        missing_cols = len(cols) - len(config)
        for i in range(missing_cols):
            config.append("")
        configs.append(config)

    db = DatabaseManager()
    db.addConfigTable(configTableName, cols, configs)

    return configTableName

def generateConfigTable(gen_file):
    """Generates config table from csv file of valid config parameters."""
    reader = csv.reader(gen_file)
    configTableName = parseTableName(reader)

    cols = []
    colDict = {}
    for line in gen_file:
        if not line.startswith('#'):
            line = line.strip()
            key,tokens = line.split('|')
            key = key.strip()
            tokens = tokens.strip()
            cols.append(key)
            values = []
            for token in tokens.split(','):
                parts = token.split('-')
                if len(parts) == 2:
                    start,stop = parts
                    step = '1'
                    if ':' in stop:
                        stop,step = stop.split(':')
                    if '.' in start + stop + step:
                        vals = frange(float(start),float(stop),float(step))
                    else:
                        vals = range(int(start),int(stop)+1,int(step))
                    values.extend(vals)
                else:
                    values.append(token)
            colDict[key] = values

    configs = []
    for col in cols:
        configs = generateCombos(configs, colDict[col])

    db = DatabaseManager()
    db.addConfigTable(configTableName, cols, configs)

    return configTableName

def generateCombos(combos, vals):
    new_combos = []
    if not combos:
        for val in vals:
            new_combos.append([val])
    else:
        for combo in combos:
            for val in vals:
                new_combo = deepcopy(combo)
                new_combo.append(val)
                new_combos.append(new_combo)
    return new_combos

def parseTableName(reader):
    """Get the name of the config table represented in the csv file."""
    # Get the name of this group of configs
    configTableName = ""
    row = next(reader)
    while row[0].startswith('#'):
        row = next(reader)
    if len(row) == 1:
        configTableName = row[0]
    else:
        e = "First line of csv file must be the name of this \
                group of configs"
        raise CTIPError(e)

    return configTableName

def initTestSession(test_func, table, whereClause="", outdir="", qsub=None, name=None):
    """
    Initialize a test session of all configs in 'table' that satisfy
    the 'whereClause'.
    """

    manager = DatabaseManager()

    # Get configs from the database
    colnames,configs = manager.getRecords(table, whereClause)

    #
    # Create a folder for this test session based on the date and time
    #   -> The directory being created is:
    #           <outDir>/<configTable>/<date_time>/
    #   -> If a session_name is specified, the directory is:
    #           <outDir>/<configTable>/<session_name>/
    #   -> Each config will create a folder in this directory
    #
    now = datetime.datetime.now()
    sec = now.second
    timestamp = now.strftime("%Y-%m-%d_%H.%M.") + str(sec)
    sql_datetime_str = now.strftime("%Y-%m-%d %H:%M:") + str(sec)

    session_name = name
    if not session_name:
        session_name = timestamp

    testBatchDir = outdir
    if ctip.CREATE_DIR_STRUCTURE:
        testBatchDir = os.path.join(outdir, table, session_name)
        try:
            os.makedirs(testBatchDir)
        except OSError:
            if not os.path.isdir(testBatchDir):
                raise RuntimeError('Could not create the output directory')

    # Store text file snapshot of config table at root session dir
    snapshotPath = os.path.join(testBatchDir, table + ".csv")
    with open(snapshotPath, 'w') as sf:
        writeConfigCsv(sf, table, colnames, configs)

    # Add this session info to the sessions table
    session_id = manager.newSession(table, name, sql_datetime_str, whereClause)

    # Call the test_function for each config
    jobs = []
    id_queue = Queue()
    for config in configs:
        if qsub:
            p = Process(target=test_func, args=(config, id_queue, testBatchDir, qsub))
        else:
            p = Process(target=test_func, args=(config, id_queue, testBatchDir))
        p.start()
        jobs.append(p)
    for p in jobs:
        p.join()

    job_ids = []
    try:
        while True:
            job_ids.append(id_queue.get_nowait())
    except QueueEmpty:
        pass

    for id,cfg_id in job_ids:
        manager.addJobToSession(session_id, cfg_id, id)

def checkSession(session_id=None):
    updateJobs()
    manager = DatabaseManager()
    return manager.getSessionSummary(session_id)

def updateJobs():
    if ctip.ON_HPCC:
        # Get all job ids
        manager = DatabaseManager()
        colnames,jobs = manager.getRecords("jobs")
        job_ids = []
        for job in jobs:
            job_ids.append(str(job['job_id']))

        # Get output of qstat
        proc = Popen(['qstat'], stdout=PIPE)
        qstat = proc.stdout.read()
        qstat = qstat.split('\n')

        # Parse output to get the status of the active jobs
        job_stats = []
        for line in qstat:
            if line:
                line = line.split()
                id = line[0].split('.')[0]
                if id in job_ids:
                    job_stats.append( ( id, line[-2] ) )

        # Report the status' to the database
        for stat in job_stats:
            if stat[1] == 'Q':
                manager.updateJobStatus(stat[0], 'queued')
            elif stat[1] == 'R':
                manager.updateJobStatus(stat[0], 'running')
            elif stat[1] == 'H':
                manager.updateJobStatus(stat[0], 'held')
            elif stat[1] == 'S': 
                manager.updateJobStatus(stat[0], 'suspended')



