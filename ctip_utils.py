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

    def storeSnapshot(self, table, outfilename=None):
        """Store the contents of a config table as a csv file."""
        cols,types,configs = self.getConfigs(table)

        if not outfilename:
            outfilename = str(table) + ".csv"

        with open(outfilename, 'w') as summary:
            writer = csv.writer(summary)
            writer.writerow([table])
            writer.writerow(cols)
            writer.writerow(types)
            for config in configs:
                writer.writerow(list(config))


    def listConfigTables(self):
        """List the names of all tables in the database."""
        query = "SELECT name FROM sqlite_master WHERE type='table'"
        for row in self.conn.execute(query):
            print row[0]

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


