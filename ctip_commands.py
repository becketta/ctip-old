#
# Created by Aaron Beckett January, 2016
#
import getopt
import string
import os

import ctip_funcs
from ctip_dbm import DatabaseManager
from ctip_constants import RUN_CONFIG
_run_module = __import__(RUN_CONFIG)
run_cfg = _run_module.runConfig

def run_table(args):
    run(args.table_name, args)

def run_file(args):
    with open(args.csv_file, 'r') as cfg_file:
        table = ctip_funcs.createConfigTable(cfg_file)
    run(table, args)

def run_gen(args):
    with open(args.gen_file, 'r') as cfg_schema:
        table = ctip_funcs.generateConfigTable(cfg_schema)
    run(table, args)

def run(table, args):
    # Initialize the test session!
    ctip_funcs.initTestSession(
        run_cfg,
        table,
        ' '.join(args.where_clause),
        args.outdir,
        args.qsub,
        args.name
    )
    print("Jobs submitted!")

def tables(args):
    db = DatabaseManager()
    db.listConfigTables()

def list(args):
    db = DatabaseManager()
    db.printTable(args.table_name, ' '.join(args.where_clause))

def save(args):
    out_file_name = args.outfile
    if not out_file_name:
        out_file_name = args.table_name + '.csv'

    with open(out_file_name, 'w') as outfile:
        ctip_funcs.storeSnapshot(args.table_name, outfile)

def check(args):
    only_one = False
    if args.session_id:
        only_one = True
    summary = ctip_funcs.checkSession(args.session_id)

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

def update_status(args):
    db = DatabaseManager()
    db.updateJobStatus(args.job_id, args.new_status)

def update_id(args):
    db = DatabaseManager()
    db.updateJobId(args.job_id, args.new_id)

def log_start(args):
    db = DatabaseManager()
    db.startJob(args.job_id)

def log_pause(args):
    db = DatabaseManager()
    db.pauseJob(args.job_id)

def log_resume(args):
    db = DatabaseManager()
    db.resumeJob(args.job_id)

def log_end(args):
    db = DatabaseManager()
    db.endJob(args.job_id)

def clean(args):
    db = DatabaseManager()
    if args.session_id:
        db.deleteSession(args.session_id)
    else:
        db.deleteFinishedSessions()

