from atc.sql.SqlExecutor import SqlExecutor
from tests.cluster.job1.delta import extras


class SparkSqlExecutor(SqlExecutor):
    def __init__(self):
        super().__init__(base_module=extras)
