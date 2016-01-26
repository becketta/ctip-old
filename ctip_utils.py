#
# Created by Aaron Beckett January, 2016
#

import sqlite3 as sql
import os
import csv
import datetime
from multiprocessing import Process

###########################################################
#   Utility Classes
#

class DatabaseManager:

    dbname = "test.db"

    def __init__(self):
        self.conn = sql.connect(self.dbname)
        self.conn.row_factory = sql.Row

    def __del__(self):
        self.conn.close()

    def listConfigTables(self):
        """List the names of all tables in the database."""
        query = "SELECT name FROM sqlite_master WHERE type='table'"
        for row in self.conn.execute(query):
            print row[0]

    def printConfigTable(self, table):
        """Print the configs from a specific table in a pretty format."""
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

    def getConfigs(self, table, whereClause=""):

        infoquery = "PRAGMA table_info(" + table + ")"
        configquery = "SELECT * FROM " + table + " " + whereClause

        # Get the column info
        colnames = []
        coltypes = []
        for col in self.conn.execute(infoquery):
            colnames.append(col[1])
            coltypes.append(col[2])

        # Get the relevant configurations
        #   -> if whereClause is "", get all the configs in the table
        configs = self.conn.execute(configquery).fetchall()

        return colnames,coltypes,configs

    def addConfigTable(self, name, colnames, coltypes, configs):
        pass


###########################################################
#   Utility Functions
#

def storeSnapshot(table, outfile):
    """Store the contents of a config table as a csv file."""
    db = DatabaseManager()
    cols,types,configs = db.getConfigs(table)
    writeConfigCsv(outfile, table, cols, types, configs)
    
def writeConfigCsv(outfile, tablename, cols, types, configs):
    """Write the given table info in csv format."""
    writer = csv.writer(outfile)
    writer.writerow([tablename])
    writer.writerow(cols)
    writer.writerow(types)
    for config in configs:
        writer.writerow(list(config))
        
def createConfigTable(csv_file):
    """Creates new config table from a properly formatted csv file."""
    configTableName = ""
    reader = csv.reader(csv_file)

    # Get the name of this group of configs
    row = next(reader)
    if len(row) == 1:
        configTableName = row[0]
    else:
        e = "First line of csv file must be the name of this \
                group of configs"
        raise RuntimeError(e)

    # Get the column names and types
    cols = next(reader)
    types = next(reader)

    # Zip the column names to the column types
    if len(cols) != len(types):
        e = "Line 2 and 3 must contain column names and data types \
                respectively"
        raise RuntimeError(e)
    else:
        colinfo = zip(cols,types)

    return configTableName

def generateConfigTable(csv_file):
    """Generates config table from csv file of valid config parameters."""
    configTableName = ""
    reader = csv.reader(csv_file)

    # Get the name of this group of configs
    row = next(reader)
    if len(row) == 1:
        configTableName = row[0]
    else:
        e = "First line of csv file must be the name of this \
                group of configs"
        raise RuntimeError(e)

    return configTableName

def initTestSession(test_func, table, whereClause="", outdir=""):
    """
    Initialize a test session of all configs in 'table' that satisfy
    the 'whereClause'.
    """

    manager = DatabaseManager()

    # Get configs from the database
    colnames,coltypes,configs = manager.getConfigs(table, whereClause)

    #
    # Create a folder for this test session based on the date and time
    #   -> The directory being created is:
    #           <outDir>/<configTable>/<date_time>/
    #   -> Each config will create a folder in this directory
    #
    now = datetime.datetime.now()
    sec = now.second
    timestamp = now.strftime("%Y-%m-%d_%H:%M:") + str(sec)

    testBatchDir = os.path.join(outdir, table, timestamp)
    try:
        os.makedirs(testBatchDir)
    except OSError:
        if not os.path.isdir(testBatchDir):
            raise RuntimeError('Could not create the output directory')

    # Store text file snapshot of config table at root session dir
    snapshotPath = os.path.join(testBatchDir, "configs.csv")
    with open(snapshotPath, 'w') as sf:
        writeConfigCsv(sf, table, colnames, coltypes, configs)

    # Spawn processes for each config to test
    test_processes = []
    for config in configs:
        p = Process(target=test_func, args=(config, testBatchDir))
        p.start()
        test_processes.append(p)

    return test_processes


