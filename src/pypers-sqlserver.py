import pyodbc

class pypersSqlServer:
    def __init__(self, host, db, user, password, port=1433):
        self.host = host
        self.db = db
        self.user = user
        self.password = password
        self.port = port
        self._connection = pyodbc.connect(
            server=host,
            database=db,
            user=user,
            password=password,
            port=port,
            tds_version="7.4",
            driver="FreeTDS"
        )

    
    def _query(self, sql):
        """
            Query database
        """
        try:
            cursor = self._connection.cursor()
            return cursor.execute(sql).fetchall()
        except:
            print(f'Error in Query database.')
        

    def _select(self, query):
        """
            Runs a SELECT query and returns result in an Array
        """
        result = []
        cursor = self._connection.cursor()
        try:
            cursor.execute(query)
            row = cursor.fetchone()
            while row:
                print(f"Find row : \\n {vars(row)}")
                result.append(row)
                row = cursor.fetchone()

            return result
        except:
            print(f"Error in Select query.")
        

    def _execute(self, cmd):
        """
            Executes a SQL command agiants the Database.
        """
        cursor = self._connection.cursor()
        try:
            cursor.execute(cmd)
            self._connection.commit()
        except:
            self._connection.rollback()
        

    def _insert(self, table, data):
        """
            Insert a single row into the table name.
            Pass an object, json as data.
        """
        columns = []
        values = []
        for key in data:
            columns.append(key)
            if data[key] is None:
                values.append("NULL")
            else:
                values.append(f"'{str(data[key])}'")
            
        cmd = (f"INSRET INTO {table} (" +
                ",".join(columns) +
                ") VALUES (" + 
                ",".join(values) + 
                ")"
        )
        self._execute(cmd)
    
