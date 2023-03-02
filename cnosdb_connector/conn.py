import time

from .error import NotSupportedError
from .client import Client
from .cursor import Cursor


class CnosDBConnection:
    def __init__(self, **kwargs):
        url = kwargs.get("url", "http://127.0.0.1:31007/")
        user = kwargs.get("user", "root")
        password = kwargs.get("password", "")
        database = kwargs.get("database", "public")
        self._client = Client(
            url,
            database,
            user,
            password,
        )

    # close, commit, rollback, cursor are PEP249 method
    def close(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        raise NotSupportedError()

    def cursor(self):
        return Cursor(self._client)

    def execute(self, sql: str):
        """
        database execute sql

        :param sql: the CnosDB sql, more information for CnosDB can be found in
                    https://docs.cnosdb.com/guide/introduction.html
        :type sql: str
        :return: the CnosDB execute sql result
        :rtype: dict list, the sql result of CnosDB
        """
        return self._client.sql(sql)

    def write_lines(self, lines: [str]):
        """
        write lineprotocol to database

        :param lines: The lines of LineProtocol
                      each item is lineprotocol str, more information for LineProtocol can be found in
                      https://docs.influxdata.com/influxdb/v2.6/reference/syntax/line-protocol/
        """
        self._client.line_protocol(lines)

    def write_csv(self, table, file_path):
        """
        write a csv file to database

        :param table: the table for csv file to write
                      before write csv file to database, database should
                      have a table which have same schema with csv
        :type table: str
        :param file_path: file path of the csv file
        :type file_path: str
        """
        # create a temp external table use csv file, then get table
        # schema of given table, through INSERT SELECT write data
        # to given table
        self._client.sql(f"create external table ___temp_external_table\
                            STORED AS CSV \
                            WITH HEADER ROW \
                            LOCATION \"{file_path}\";")
        schema = self._client.sql(f"DESCRIBE TABLE {table}")
        col_names = ""
        for column in schema:
            col_names += column["COLUMN_NAME"] + ","
        col_names = col_names.strip(',')
        self._client.sql(f"INSERT INTO {table}({col_names}) SELECT * FROM ___temp_external_table;")
        self._client.sql("DROP TABLE ___temp_external_table")

    def write_dataframe(self, dataframe, table_name, column_names: [str]):
        """
        write a pandas dataframe to database

        :param dataframe: the dataframe of python
        :type dataframe: Pandas DataFrame

        :param table_name: the table name of cnosdb
        :type table_name: str

        :param column_names: the column names of dataframe and table,
                            column name must same in dataframe and table
        :type column_names: [str]
        """
        columns = ''
        for column in column_names:
            columns += f'{column},'
        columns = columns.strip(',')
        if 'time' in column_names:
            for index, row in dataframe.iterrows():
                values = ''
                for column in column_names:
                    values += f"'{row[column]}',"
                values = values.strip(',')
                query = f"INSERT INTO {table_name}({columns}) VALUES ({values})"
                self._client.sql(query)
        else:
            for index, row in dataframe.iterrows():
                values = ''
                for column in column_names:
                    values += f"'{row[column]}',"
                values = values.strip(',')
                query = f"INSERT INTO {table_name}(time,{columns}) VALUES ({time.time_ns()},{values})"
                self._client.sql(query)

    def create_database(self, database_name):
        """
        create a database from CnosDB

        :param database_name: the name of database to create
        :type database_name: str
        """
        return self._client.sql(f"CREATE DATABASE {database_name};")

    def create_database_with_ttl(self, database_name, ttl):
        """
        create a database with ttl from CnosDB

        :param database_name: the name of database to create
        :type database_name: str
        :param ttl: time to live, means the data time to live in database,
                    default is 365 day, when set ttl to 30 day, then write
                    data with timestamp for before 30 day will be failed
        :type ttl: duration str, use number and unit, support d(day), h(hour), m(minutes),
                    such as "40d", "100h", "3600m"
        :return:
        """
        return self._client.sql(f"CREATE DATABASE {database_name} WITH TTL '{ttl}';")

    def create_user(self, user, password):
        """
        create a user from CnosDB

        :param user: the username of database to create
        :type user: str
        :param password: the password of the user
        :type password: str
        :return:
        """
        return self._client.sql(f"CREATE USER {user} WITH PASSWORD = {password}")

    def drop_database(self, database_name):
        """
        drop a database from CnosDB

        :param database_name: the database name of CnosDB to drop
        :type database_name: str
        :return:
        """
        return self._client.sql(f"DROP DATABASE {database_name};")

    def drop_table(self, table_name):
        """
        drop a table from CnosDB

        :param table_name: the table name of database to drop
        :type table_name: str
        :return:
        """
        return self._client.sql(f"DROP TABLE {table_name};")

    def drop_user(self, user):
        """
        drop a user from CnosDB

        :param user: the username of database to drop
        :type user: str
        :return:
        """
        return self._client.sql(f"DROP USER {user}")

    def switch_database(self, database_name):
        """
        switch client current database to given database

        :param database_name: the database name of client to switch
        :type database_name: str
        :return:
        """
        self._client.set_database(database_name)

    def switch_user(self, user, password):
        """
        switch client current database to given user

        :param user: the username of client to switch
        :type user: str
        :param password: the password of the user
        :type password: str
        :return:
        """
        self._client.set_user(user, password)

    def switch_url(self, url):
        """
        switch client current url to given url

        :param url: the url of client to switch
        :type url: str
        :return:
        """
        self._client.set_url(url)

    def list_database(self):
        """
        list all database in CnosDB

        :return: the database names in CnosDB
        :rtype: dict list
        """
        return self._client.sql("SHOW DATABASES")

    def list_table(self):
        """
        list all table in client current database

        :return: the table names in database
        :rtype: dict list
        """
        return self._client.sql("SHOW TABLES")
