from atc.sql.SqlServer import SqlServer
from tests.cluster.secrets import sqlServerUser, sqlServerUserPassword


class DeliverySqlServer(SqlServer):
    def __init__(
        self,
        database: str = "Delivery",
        hostname: str = None,
        username: str = None,
        password: str = None,
        port: str = "1433",
    ):

        self.hostname = (
            "{resourceName}test.database.windows.net" if hostname is None else hostname
        )
        self.username = sqlServerUser() if username is None else username
        self.password = sqlServerUserPassword() if password is None else password
        self.database = database
        self.port = port
        super().__init__(
            self.hostname, self.database, self.username, self.password, self.port
        )
