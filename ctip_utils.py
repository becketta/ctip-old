#
# Created by Aaron Beckett January, 2016
#

import sqlite3 as sql

class CTIPDatabaseManager:

    dbname = "test.db"

    def __init__(self):
        pass

    def storeSnapshot(self, table, outfilename):
        """Store the contents of a config table as a csv file."""
        pass

    def listConfigTables(self):
        """List the names of all tables in the database."""
        pass

    def getConfigs(self, table, whereClause=""):
        colnames = []
        coltypes = []
        configs = []

        conn = sql.connect(self.dbname)
        with conn:

            conn.row_factory = sql.Row

            infoquery = "PRAGMA table_info(" + table + ")"
            configquery = "SELECT * FROM " + table + " " + whereClause

            # Get the column info
            for col in conn.execute(infoquery):
                colnames.append(col[1])
                coltypes.append(col[2])

            # Get the relevant configurations
            #   -> if whereClause is "", get all the configs in the table
            configs = conn.execute(configquery).fetchall()

        return colnames,coltypes,configs


