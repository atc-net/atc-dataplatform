import unittest

from tests.cluster.config import InitConfigurator
from tests.cluster.sql.DeliverySqlExecutor import DeliverySqlExecutor
from tests.cluster.sql.DeliverySqlServer import DeliverySqlServer

from . import extras


class DeliverySqlExecutorTests(unittest.TestCase):
    tc = None
    sql_server = None

    @classmethod
    def setUpClass(cls):

        # Register the delivery table for the table configurator
        cls.tc = InitConfigurator(clear=True)
        cls.tc.add_resource_path(extras)
        cls.tc.set_debug()

        cls.sql_server = DeliverySqlServer()

        # Ensure no table is there
        cls.sql_server.drop_table("SqlTestTable1")
        cls.sql_server.drop_table("SqlTestTable2")

    @classmethod
    def tearDownClass(cls):
        cls.sql_server.drop_table("SqlTestTable1")
        cls.sql_server.drop_table("SqlTestTable2")

    def test_can_execute(self):
        DeliverySqlExecutor().execute_sql_file("test1")

        self.sql_server.read_table("SqlTestTable1")

        sh = self.sql_server.from_tc("SqlTestTable2")
        sh.read()

        self.assertTrue(True)
