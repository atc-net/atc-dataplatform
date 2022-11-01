from atc.sql.SqlServer import SqlServer
from tests.cluster.secrets import dbDeployClientId, dbDeployClientSecret


class DeliverySqlServerSpn(SqlServer):
    def __init__(
        self,
    ):
        super().__init__(
            hostname="{resourceName}test.database.windows.net",
            database="Delivery",
            spnpassword=dbDeployClientSecret(),
            spnid=dbDeployClientId(),
        )
