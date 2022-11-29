from typing import List, Tuple

from atc_tools.testing import DataframeTestCase

from atc import Configurator
from atc.delta import DbHandle, DeltaHandle
from atc.delta.autoloader_handle import AutoLoaderHandle
from atc.etl.loaders.UpsertLoader import UpsertLoader
from atc.functions import init_dbutils
from atc.spark import Spark
from atc.utils import DataframeCreator
from atc.utils.FileExists import file_exists
from atc.utils.stop_all_streams import stop_all_streams
from tests.cluster.delta import extras
from tests.cluster.delta.SparkExecutor import SparkSqlExecutor


class UpsertLoaderTestsAutoloader(DataframeTestCase):

    source_table_checkpoint_path = None
    join_cols = ["col1", "col2"]

    data1 = [
        (5, 6, "foo"),
        (7, 8, "bar"),
    ]
    data2 = [
        (1, 2, "baz"),
    ]
    data3 = [(5, 6, "boo"), (5, 7, "spam")]
    # data5 is the merge result of data2 + data3 + data4
    data4 = [(1, 2, "baz"), (5, 6, "boo"), (5, 7, "spam"), (7, 8, "bar")]

    dummy_columns: List[str] = ["col1", "col2", "col3"]

    dummy_schema = None
    target_dh_dummy: DeltaHandle = None
    target_ah_dummy: AutoLoaderHandle = None

    def __init__(self, methodName: str = ...):
        super().__init__(methodName)
        self.source_table_id = None

    @classmethod
    def setUpClass(cls) -> None:
        tc = Configurator()
        tc.add_resource_path(extras)
        tc.set_debug()

        tc.register(
            "AutoDbUpsert",
            {"name": "TestUpsertAutoDb{ID}", "path": "/mnt/atc/silver/testdb{ID}"},
        )
        DbHandle.from_tc("AutoDbUpsert").create()

        source_table_id = "Test1Table"
        source_table_checkpoint_path = "tmp/" + source_table_id + "/_checkpoint_path"
        tc.register(
            source_table_id,
            {
                "name": "TestUpsertAutoDb{ID}." + source_table_id,
                "path": "/mnt/atc/silver/TestUpsertAutoDb{ID}/" + source_table_id,
                "checkpoint_path": source_table_checkpoint_path,
            },
        )

        if not file_exists(source_table_checkpoint_path):
            init_dbutils().fs.mkdirs(source_table_checkpoint_path)

        # Autoloader pointing at source table
        cls.source_ah = AutoLoaderHandle.from_tc(source_table_id)

        # Autoloader/Deltahandle pointing at target table
        cls.target_ah_dummy = AutoLoaderHandle.from_tc("UpsertLoaderDummy")
        cls.target_dh_dummy = DeltaHandle.from_tc("UpsertLoaderDummy")

        # Create target table
        SparkSqlExecutor().execute_sql_file("upsertloader-test")

        cls.dummy_schema = cls.target_dh_dummy.read().schema

        # make sure target is empty
        df_empty = DataframeCreator.make_partial(cls.dummy_schema, [], [])
        cls.target_dh_dummy.overwrite(df_empty)

    @classmethod
    def tearDownClass(cls) -> None:
        DbHandle.from_tc("UpsertLoaderDb").drop_cascade()
        DbHandle.from_tc("AutoDbUpsert").drop_cascade()

        if file_exists(cls.source_table_checkpoint_path):
            init_dbutils().fs.rm(cls.source_table_checkpoint_path)
        stop_all_streams()

    def test_01_can_perform_incremental_on_empty(self):
        """Stream two rows to the empty target table"""

        self._create_test_source_data(data=self.data1)

        loader = UpsertLoader(handle=self.target_ah_dummy, join_cols=self.join_cols)

        source_df = self.source_ah.read()

        loader.save(source_df)

        self.assertDataframeMatches(self.target_dh_dummy.read(), None, self.data1)

    def test_02_can_perform_incremental_append(self):
        """The target table is already filled from before."""
        existing_rows = self.target_dh_dummy.read().collect()
        self.assertEqual(2, len(existing_rows))

        loader = UpsertLoader(handle=self.target_ah_dummy, join_cols=self.join_cols)

        self._create_test_source_data(data=self.data2)

        source_df = self.source_ah.read()

        loader.save(source_df)

        self.assertDataframeMatches(
            self.target_dh_dummy.read(), None, self.data1 + self.data2
        )

    def test_03_can_perform_merge(self):
        """The target table is already filled from before."""
        existing_rows = self.target_dh_dummy.read().collect()
        self.assertEqual(3, len(existing_rows))

        loader = UpsertLoader(handle=self.target_ah_dummy, join_cols=self.join_cols)

        self._create_test_source_data(data=self.data3)

        source_df = self.source_ah.read()

        loader.save(source_df)

        self.assertDataframeMatches(self.target_dh_dummy.read(), None, self.data4)

    def _create_test_source_data(
        self, tableid: str = None, data: List[Tuple[int, int, str]] = None
    ):

        if tableid is None:
            tableid = self.source_table_id
        if data is None:
            raise ValueError("Testdata missing.")

        dh = DeltaHandle.from_tc(tableid)

        Spark.get().sql(
            f"""
            CREATE TABLE IF NOT EXISTS {dh.get_tablename()}
            (
            id int,
            name string
            )
            """
        )

        df_source = DataframeCreator.make_partial(
            self.dummy_schema, self.dummy_columns, data
        )

        dh.append(df_source)
