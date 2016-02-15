#
# Created by Aaron Beckett January, 2016
#

import sqlite3 as sql
import os
import csv
import datetime
from multiprocessing import Process, Queue
from Queue import Empty as QueueEmpty
from copy import deepcopy
from string import Template

###########################################################
#   Utility Classes
#

class CTIPError(Exception):
    """Base class for ctip exceptions."""
    def __init__(self, msg):
        self.msg = msg

class ParseError(CTIPError):
    """Raised when ctip is unable to parse command line args or csv files."""
    def __init__(self, category, msg):
        super(ParseError,self).__init__(msg)
        self.category = category

class QsubBuilder(Template):
    delimiter = '%='

class DatabaseManager:
    """Handles interactions with the local SQLite Database used by ctip."""

    dbname = "test.db"
    reserved_table_names = [ 'sessions', 'jobs' ]

    def __init__(self):
        self.conn = sql.connect(self.dbname)
        self.conn.row_factory = sql.Row
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS sessions(
                id INTEGER PRIMARY KEY,
                config_group TEXT,
                where_clause TEXT,
                date TEXT
            );

            CREATE TABLE IF NOT EXISTS jobs(
                session_id INT,
                job_id TEXT,
                status TEXT,
                PRIMARY KEY (session_id, job_id)
            );
        """)

    def __del__(self):
        self.conn.close()

    def newSession(self, config_group, datetime, whereClause=""):
        cur = self.conn.cursor()
        if whereClause:
            s = "INSERT INTO sessions(config_group, where_clause, date) values(?,?,?)"
            cur.execute(s, (config_group, whereClause, datetime))
        else:
            cur.execute("INSERT INTO sessions(config_group, date) values(?,?)", (config_group, datetime))

        new_session_id = cur.lastrowid
        self.conn.commit()
        return new_session_id

    def addJobToSession(self, session_id, job_id):
        s = "INSERT INTO jobs(session_id,job_id,status) values(?,?,?)"
        self.conn.execute(s, (session_id, job_id, "submitted"))
        self.conn.commit()

    def updateJobStatus(self, job_id, status):
        s = "UPDATE jobs SET status = ? WHERE job_id = ?"
        self.conn.execute(s, (status, job_id))
        self.conn.commit()

    def deleteSession(self, session_id):
        s = "DELETE FROM jobs WHERE session_id = ?"
        self.conn.execute(s, (session_id))
        s = "DELETE FROM sessions WHERE id = ?"
        self.conn.execute(s, (session_id))

    def listConfigTables(self):
        """List the names of all config tables in the database."""
        query = "SELECT name FROM sqlite_master WHERE type='table'"
        for row in self.conn.execute(query):
            if row[0] not in self.reserved_table_names:
                print row[0]

    def printTable(self, table):
        """Print the records from a specific table in a pretty format."""
        cur = self.conn.cursor()
        cur.execute('SELECT * FROM ' + table)

        printrows = [[cn[0] for cn in cur.description]]
        for row in cur.fetchall():
            strlist = []
            for e in row:
                strlist.append(str(e))
            printrows.append(strlist)

        widths = []
        for i in range(len(printrows[0])):
            widths.append(0)
            for j in range(len(printrows)):
                width = len(printrows[j][i])+1
                if width > widths[i]:
                    widths[i] = width

        printstr = ""
        for w in widths:
            printstr += "%-" + str(w) + "s "

        for row in printrows:
            print printstr % tuple(row)

    def getRecords(self, table, whereClause=""):

        query = "SELECT * FROM " + table + " " + whereClause

        cur = self.conn.cursor()
        cur.execute(query)

        # Get the column names
        colnames = [cn[0] for cn in cur.description]

        # Get the relevant configurations
        #   -> if whereClause is "", get all the configs in the table
        records = cur.fetchall()

        return colnames,records

    def addConfigTable(self, name, colnames, configs):

        if name.lower() in self.reserved_table_names:
            raise CTIPError(name + " is a reserved table name.")
        
        insertColNames = '(' + ','.join(colnames) + ')'
        alreadyHasId = False
        if colnames[0] in ["id", "ID"]:
            alreadyHasId = True

        #
        # Generate the create table query
        #
        # Ensure the first column is an auto-incremented primary key
        if alreadyHasId:
            colnames[0] = "{0} INTEGER PRIMARY KEY".format(colnames[0])
        else:
            colnames.insert(0, "id INTEGER PRIMARY KEY")
        # Add the column names to the create query
        colStr = "("
        for col in colnames:
            colStr += "{0},".format(col)
        colStr = colStr[:-1] + ')'
        createSql = "CREATE TABLE {0}{1};".format(name,colStr)

        #
        # Compile the sql queries into a list
        #
        sqls = [ "DROP TABLE IF EXISTS {0};".format(name),
                 createSql ]

        #
        # Construct the insert template
        #
        insertSql = "INSERT INTO {0}{1} values(".format(name,insertColNames)
        # Ensure the id column, if present, is stored as an int
        id_offset = 0
        if alreadyHasId:
            insertSql += "{0},"
            id_offset = 1
        for i in range(id_offset, len(configs[0])):
            insertSql += "'{" + str(i) + "}',"
        insertSql = insertSql[:-1] + ');'

        #
        # Add an insert statement for each config
        #
        for config in configs:
            sqls.append(insertSql.format(*config))

        # Execute the sql statements
        cur = self.conn.cursor()
        cur.executescript("".join(sqls))

###########################################################
#   Utility Functions
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
        line = line.strip()
        key,values = line.split('|')
        cols.append(key)
        colDict[key] = values.split(',')

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
    if len(row) == 1:
        configTableName = row[0]
    else:
        e = "First line of csv file must be the name of this \
                group of configs"
        raise CTIPError(e)

    return configTableName

def initTestSession(test_func, table, whereClause="", outdir=""):
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
    #   -> Each config will create a folder in this directory
    #
    now = datetime.datetime.now()
    sec = now.second
    timestamp = now.strftime("%Y-%m-%d_%H.%M.") + str(sec)
    sql_datetime_str = now.strftime("%Y-%m-%d %H:%M:") + str(sec)

    testBatchDir = os.path.join(outdir, table, timestamp)
    try:
        os.makedirs(testBatchDir)
    except OSError:
        if not os.path.isdir(testBatchDir):
            raise RuntimeError('Could not create the output directory')

    # Store text file snapshot of config table at root session dir
    snapshotPath = os.path.join(testBatchDir, "configs.csv")
    with open(snapshotPath, 'w') as sf:
        writeConfigCsv(sf, table, colnames, configs)

    # Add this session info to the sessions table
    session_id = manager.newSession(table, sql_datetime_str, whereClause)

    # Call the test_function for each config
    jobs = []
    id_queue = Queue()
    for config in configs:
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

    print(job_ids)
    for id in job_ids:
        manager.addJobToSession(session_id, id)

