#
# Created by Aaron Beckett January, 2016
#

import sqlite3 as sql
import csv

class CTIPDatabaseManager:

    dbname = "test.db"

    def __init__(self):
        self.conn = sql.connect(self.dbname)
        self.conn.row_factory = sql.Row

    def __del__(self):
        self.conn.close()

    def storeSnapshot(self, table, outfile):
        """Store the contents of a config table as a csv file."""
        cols,types,configs = self.getConfigs(table)
        writeConfigCsv(outfile, table, cols, types, configs)
        
    def writeConfigCsv(self, outfile, tablename, cols, types, configs):
        writer = csv.writer(outfile)
        writer.writerow([tablename])
        writer.writerow(cols)
        writer.writerow(types)
        for config in configs:
            writer.writerow(list(config))

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

    def createConfigTable(self, csv_file):
        """Creates new config table filled with the csv file configs"""
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

    def generateConfigTable(self, csv_file):
        """
        Generates new config table of all combinations of csv config values
        """
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


