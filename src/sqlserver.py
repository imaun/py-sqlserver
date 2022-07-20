from multiprocessing.dummy import Array
import pymssql

class SqlServerHelper:
    def __init__(self, host, databaseName, user, password, port=1433):
        self.host = host
        self.db_name = databaseName
        self.user = user
        self.password = password
        self.port = port
        self._connection =  pymssql.connect(
            host=host,
            database=databaseName,
            user=user,
            password=password,
            as_dict=True
        )


    def _query(self, sql):
        """
            Query databse
        """
        cursor = self._connection.cursor()
        try:
            cursor.execute(sql)
            return cursor.fetchall()
        except pymssql.ProgrammingError:
            print(f"Error in query database : {self.db_name}")
            raise pymssql.ProgrammingError
        
    
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
        except pymssql.ProgrammingError:
            print(f"Error in select query. database : {self.db_name}")
            raise pymssql.ProgrammingError
        

    def _execute(self, cmd):
        """
            Executes a SQL command against database
        """
        cursor = self._connection.cursor()
        try:
            cursor.execute(cmd)
            self._connection.commit()
        except pymssql.ProgrammingError:
            self._connection.rollback()
            raise pymssql.ProgrammingError
        

    
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
                values.append("null")
            else:
                values.append(f"'{str(data[key])}'")
            
        cmd = (f"INSERT INTO {table} (" +
                ",".join(columns) +
                ") VALUES (" +
                ",".join(values) + 
                ")"
        )
        self._execute(cmd)

    


