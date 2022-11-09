from atc.sql.SqlServer import SqlServer
from tests.cluster.secrets import clientId, clientSecret


class DeliverySqlServerSpn(SqlServer):
    def __init__(
        self,
    ):
        super().__init__(
            hostname="{resourceName}test.database.windows.net",
            database="Delivery",
            spnpassword=clientSecret(),
            spnid=clientId(),
        )
