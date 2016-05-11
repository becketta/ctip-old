#
# Created by Aaron Beckett January, 2016
#

import sqlite3 as sql

import ctip_constants as ctip


class DatabaseManager:
    """Handles interactions with the local SQLite Database used by ctip."""

    dbname = ctip.CONFIG_DB
    reserved_table_names = [ 'sessions', 'jobs' ]

    def __init__(self):
        self.conn = sql.connect(self.dbname)
        self.conn.row_factory = sql.Row
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS sessions(
                id INTEGER PRIMARY KEY,
                name TEXT,
                config_group TEXT,
                where_clause TEXT,
                date TEXT
            );

            CREATE TABLE IF NOT EXISTS jobs(
                session_id INT,
                config_id INT,
                job_id TEXT,
                status TEXT,
                time_log TEXT,
                runtime TEXT,
                PRIMARY KEY (session_id, job_id)
            );
        """)

    def __del__(self):
        self.conn.close()

    def newSession(self, config_group, session_name, datetime, whereClause=""):
        cur = self.conn.cursor()
        if whereClause:
            s = "INSERT INTO sessions(name, config_group, where_clause, date) values(?,?,?,?)"
            cur.execute(s, (session_name, config_group, whereClause, datetime))
        else:
            s = "INSERT INTO sessions(name, config_group, date) values(?,?,?)"
            cur.execute(s, (session_name, config_group, datetime))

        new_session_id = cur.lastrowid
        self.conn.commit()
        return new_session_id

    def addJobToSession(self, session_id, config_id, job_id):
        s = "INSERT INTO jobs(session_id,config_id,job_id,status) values(?,?,?,?)"
        self.conn.execute(s, (session_id, config_id, job_id, "submitted"))
        self.conn.commit()

    def updateJobStatus(self, job_id, status):
        job_id = job_id.split('.')[0]
        s = "UPDATE jobs SET status = ? WHERE job_id = ?"
        cur = self.conn.cursor()
        cur.execute(s, (status, job_id))
        self.conn.commit()
        if cur.rowcount == 0:
            raise CTIPError("Invalid job id: {0}".format(job_id))

    def startJob(self, job_id):
        job_id = job_id.split('.')[0]
        s = "UPDATE jobs SET time_log = datetime('now') WHERE job_id = ?"
        self.conn.execute(s, (job_id,))
        self.conn.commit()

    def pauseJob(self, job_id):
        self.incRuntime(job_id)
        self.conn.commit()

    def resumeJob(self, job_id):
        self.startJob(job_id)

    def endJob(self, job_id):
        self.incRuntime(job_id)
        job_id = job_id.split('.')[0]
        s = "UPDATE jobs SET time_log = NULL WHERE job_id = ?"
        self.conn.execute(s, (job_id,))
        self.conn.commit()

    def incRuntime(self, job_id):
        job_id = job_id.split('.')[0]
        s = """
        UPDATE jobs SET runtime = (
            SELECT CAST((
                strftime('%s', datetime('now')) -
                strftime('%s', (SELECT time_log FROM jobs
                                WHERE job_id = :id))
            ) AS TEXT)
        ) WHERE job_id = :id
        """
        self.conn.execute(s, {"id": job_id})

    def updateJobId(self, job_id, new_id):
        job_id = job_id.split('.')[0]
        s = "UPDATE jobs SET job_id = ? WHERE job_id = ?"
        cur = self.conn.cursor()
        cur.execute(s, (new_id, job_id))
        self.conn.commit()
        if cur.rowcount == 0:
            raise CTIPError("Invalid job id: {0}".format(job_id))

    def deleteFinishedSessions(self):
        cols,sessions = self.getRecords("sessions")

        finished_sessions = []
        query = "SELECT * from jobs WHERE session_id=? and status!='done'"
        for session in sessions:
            unfinished = self.conn.execute(query, (session['id'],))
            if not unfinished.fetchall():
                finished_sessions.append(session)

        print("Sessions deleted:")
        for session in finished_sessions:
            print(session)
            self.deleteSession(session['id'])

    def deleteSession(self, session_id):
        s = "DELETE FROM jobs WHERE session_id = ?"
        self.conn.execute(s, (session_id,))
        s = "DELETE FROM sessions WHERE id = ?"
        self.conn.execute(s, (session_id,))
        self.conn.commit()

    def listConfigTables(self):
        """List the names of all config tables in the database."""
        query = "SELECT name FROM sqlite_master WHERE type='table'"
        for row in self.conn.execute(query):
            if row[0] not in self.reserved_table_names:
                print row[0]

    def printTable(self, table, where=''):
        """Print the records from a specific table in a pretty format."""
        colnames,records = self.getRecords(table, where)
        printrows = [colnames]

        for row in records:
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

        cur = self.conn.cursor()
        cur.execute("SELECT * FROM {0} {1}".format(table, whereClause))

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
        num_cols = len(colnames)
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
        for i in range(id_offset, num_cols):
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

    def getSessionSummary(self, session_id=None):
        cur = self.conn.cursor()
        if session_id:
            whereClause = "where id = {0}".format(session_id)
            query = "select sessions.*, status, count() from sessions inner join jobs on id = session_id {0} group by status".format(whereClause)
            cur.execute(query)
        else:
            query = "select sessions.*, status, count() from sessions inner join jobs on id = session_id group by id, status"
            cur.execute(query)

        return cur.fetchall()


