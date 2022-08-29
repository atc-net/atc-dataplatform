import importlib.resources
import re
import time
from types import ModuleType
from typing import Union

import pyodbc
from pyspark.sql import DataFrame

from atc.config_master import TableConfigurator
from atc.spark import Spark
from atc.sql.sql_handle import SqlHandle


class SqlServer:
    def __init__(
        self,
        hostname: str,
        database: str,
        username: str,
        password: str,
        port: str = "1433"
    ):
        self.timeout = 180  # 180 sec due to serverless
        self.sleep_time = 5  # Every 5 seconds the connection tries to be established
        self.url = (
            f"jdbc:sqlserver://{hostname}:{port};"
            f"database={database};queryTimeout=0;loginTimeout={self.timeout}"
        )

        self.username = username
        self.password = password

        self.odbc = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={hostname};"
            f"DATABASE={database};"
            f"PORT={port};"
            f"UID={username};"
            f"PWD={password};"
            f"Connection Timeout={self.timeout}"
        )
        self.properties = {
            "user": username,
            "password": password,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        }

    @classmethod
    def from_connection_string(cls, connection_string):
        """Creates SqlServer object from connection string as described in:
        https://docs.microsoft.com/en-us/dotnet/api/system.data.sqlclient.sqlconnection.connectionstring?view=dotnet-plat-ext-6.0#remarks
        """
        try:
            server_pattern = r"(Server|Address|Data Source|Addr|Network Address)=(tcp:|np:|lpc:)?([\w\d\.]+),?(\d*);"
            server = re.search(server_pattern, connection_string).group(3)
            port = re.search(server_pattern, connection_string).group(4)
            if port == "":
                port = "1433"

            user_pattern = r"(User ID|UID|User)=([^;]+);"
            username = re.search(user_pattern, connection_string).group(2)

            password_pattern = "(Password|PWD)=([^;]+);"
            password = re.search(password_pattern, connection_string).group(2)

            database_pattern = r"(Initial Catalog|Database)=([^;]+);"
            database = re.search(database_pattern, connection_string).group(2)
        except AttributeError:
            raise ValueError("Connection string does not conform to standard")
        return cls(server, database, username, password, port)

    def execute_sql(self, sql: str):
        self.test_odbc_connection()
        conn = pyodbc.connect(self.odbc)
        conn.autocommit = True
        conn.execute(sql)
        conn.close()

    def from_tc(self, id: str) -> SqlHandle:
        """This method allows an instance of SqlServer to be a drop in for the class
        DeltaHandle. One can call from_tc(id) on either to get a table handle."""
        tc = TableConfigurator()
        return SqlHandle(name=tc.table_name(id), sql_server=self)

    get_handle = from_tc

    # Convenience class for align with Spark.get() method .sql.
    def sql(self, sql: str):
        self.execute_sql(sql)

    def load_sql(self, sql: str):
        self.test_odbc_connection()
        return Spark.get().read.jdbc(
            url=self.url, table=sql, properties=self.properties
        )

    def read_table_by_name(self, table_name: str):
        return self.load_sql(f"(SELECT * FROM {table_name}) target")

    def write_table_by_name(
        self,
        df_source: DataFrame,
        table_name: str,
        append: bool = False,
        big_data_set: bool = True,
        batch_size: int = 10 * 1024,
        partition_count: int = 60,
    ):
        df_source.repartition(partition_count).write.format(
            "com.microsoft.sqlserver.jdbc.spark" if big_data_set else "jdbc"
        ).mode("append" if append else "overwrite").option(
            "schemaCheckEnabled", False
        ).option(
            "batchSize", batch_size
        ).option(
            "truncate", not append
        ).option(
            "url", self.url
        ).option(
            "dbtable", table_name
        ).option(
            "user", self.username
        ).option(
            "password", self.password
        ).save()

    def truncate_table_by_name(self, table_name: str):
        self.execute_sql(f"TRUNCATE TABLE {table_name}")

    def drop_table_by_name(self, table_name: str):
        self.execute_sql(
            f"""
                IF OBJECT_ID('{table_name}', 'U') IS NOT NULL
                BEGIN
                  DROP TABLE {table_name}
                END
            """
        )

    def drop_view_by_name(self, table_name: str):
        self.execute_sql(
            f"""
                IF OBJECT_ID('{table_name}', 'V') IS NOT NULL
                BEGIN
                    DROP view {table_name}
                END
            """
        )

    def test_odbc_connection(self):
        """
        This function is introduced for handling serverless database automatic pausing.
        It tries to reconnect to the database if no connection is established.

        :return:
        prints if connection failed
        """
        connected = False
        retry_limit = self.timeout // self.sleep_time
        retries = 0  # Keeps track of the number of retries

        while not connected and retries < retry_limit:
            retries = retries + 1
            try:
                cnxn = pyodbc.connect(self.odbc)
                cnxn.close()
                connected = True  # Database connection success!
            except pyodbc.OperationalError:
                print(
                    "Database connection failed. "
                    f"Retries {retry_limit - retries} more time."
                )
                time.sleep(self.sleep_time)  # Sleeps for 5 seconds

        if retries >= retry_limit:
            raise pyodbc.OperationalError

    def execute_sql_file(
        self,
        resource_path: Union[str, ModuleType],
        sql_file: str,
        arguments: dict = None,
    ):

        for sql in self.get_sql_file(
            resource_path=resource_path, sql_file=sql_file, arguments=arguments
        ):
            self.execute_sql(sql)

    @staticmethod
    def table_name(table_id: str):
        return TableConfigurator().table_name(table_id)

    def read_table(self, table_id: str):
        return self.read_table_by_name(SqlServer.table_name(table_id))

    def write_table(
        self,
        df_source: DataFrame,
        table_id: str,
        append: bool = False,
        big_data_set: bool = True,
        batch_size: int = 10 * 1024,
        partition_count: int = 60,
    ):
        self.write_table_by_name(
            df_source,
            SqlServer.table_name(table_id),
            append,
            big_data_set,
            batch_size,
            partition_count,
        )

    def truncate_table(self, table_id: str):
        table_name = SqlServer.table_name(table_id)
        self.execute_sql(f"TRUNCATE TABLE {table_name}")

    def drop_table(self, table_id: str):
        self.drop_table_by_name(SqlServer.table_name(table_id))

    def drop_view(self, table_id: str):
        self.drop_view_by_name(SqlServer.table_name(table_id))

    def get_sql_file(
        self,
        resource_path: Union[str, ModuleType],
        sql_file: str = None,
        arguments: dict = None,
    ):
        """
        Returns SQL statements from the specified .sql file.
        The return value is a generator.
        :param sql_file: name of the .sql file - if empty,
            all .sql files with names ending with "-create" are returned.
        :param resource_path: path to the .sql files.
        :param arguments: values of optional arguments to be replaced in the .sql files.
        :return: generator with SQL statements.
        """

        sql_arguments = TableConfigurator().get_all_details()

        if arguments is not None:
            sql_arguments.update(arguments)
        for file_name in importlib.resources.contents(resource_path):
            if (
                (sql_file is None and file_name.endswith("-create.sql"))
                or (sql_file == "*" and file_name.endswith(".sql"))
                or (sql_file is not None and file_name == (sql_file + "-create.sql"))
                or (sql_file is not None and file_name == (sql_file + ".sql"))
            ):
                with importlib.resources.path(resource_path, file_name) as file_path:
                    with open(file_path) as file:
                        for sql in file.read().split(sep="-- COMMAND ----------"):
                            yield sql.format(**sql_arguments)
