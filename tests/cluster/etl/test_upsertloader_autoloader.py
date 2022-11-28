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
    target_id = "UpsertLoaderDummy"

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

    @classmethod
    def setUpClass(cls) -> None:
        tc = Configurator()
        tc.add_resource_path(extras)
        tc.set_debug()

        tc.register(
            "AutoDbUpsert",
            {"name": "TestUpsertAutoDb{ID}", "path": "/mnt/atc/silver/testdb{ID}"},
        )

        cls._configure_views(tc)

        cls.target_ah_dummy = AutoLoaderHandle.from_tc("UpsertLoaderDummy")
        cls.target_dh_dummy = DeltaHandle.from_tc("UpsertLoaderDummy")

        SparkSqlExecutor().execute_sql_file("upsertloader-test")

        cls.dummy_schema = cls.target_dh_dummy.read().schema

        # make sure target is empty
        df_empty = DataframeCreator.make_partial(cls.dummy_schema, [], [])
        cls.target_dh_dummy.overwrite(df_empty)

    @classmethod
    def tearDownClass(cls) -> None:
        DbHandle.from_tc("UpsertLoaderDummy").drop_cascade()
        DbHandle.from_tc("AutoDbUpsert").drop_cascade()
        cls._remove_checkpoints()
        stop_all_streams()

    def test_01_can_perform_incremental_on_empty(self):

        self._create_test_source_data("Test1View", self.data1)

        # Use autoloader
        loader = UpsertLoader(handle=self.target_ah_dummy, join_cols=self.join_cols)

        read_tes1_df = AutoLoaderHandle.from_tc("Test1View").read()

        loader.save(read_tes1_df)
        self.assertDataframeMatches(self.target_dh_dummy.read(), None, self.data1)

    def test_02_can_perform_incremental_append(self):
        """The target table is already filled from before."""
        existing_rows = self.target_dh_dummy.read().collect()
        self.assertEqual(2, len(existing_rows))

        loader = UpsertLoader(handle=self.target_ah_dummy, join_cols=self.join_cols)

        self._create_test_source_data("Test2View", self.data2)

        read_tes2_df = AutoLoaderHandle.from_tc("Test2View").read()

        loader.save(read_tes2_df)

        self.assertDataframeMatches(
            self.target_dh_dummy.read(), None, self.data1 + self.data2
        )

    def test_03_can_perform_merge(self):
        """The target table is already filled from before."""
        existing_rows = self.target_dh_dummy.read().collect()
        self.assertEqual(3, len(existing_rows))

        loader = UpsertLoader(handle=self.target_dh_dummy, join_cols=self.join_cols)

        self._create_test_source_data("Test3View", self.data3)

        read_tes3_df = AutoLoaderHandle.from_tc("Test3View").read()

        loader.save(read_tes3_df)

        self.assertDataframeMatches(self.target_dh_dummy.read(), None, self.data4)

    def _create_test_source_data(self, tableid: str, data: List[Tuple[int, int, str]]):

        dh = DeltaHandle.from_tc(tableid)

        Spark.get().sql(
            f"""
                                            CREATE TABLE {dh.get_tablename()}
                                            (
                                            id int,
                                            name string
                                            )
                                        """
        )

        df_source = DataframeCreator.make_partial(
            self.dummy_schema, self.dummy_columns, data
        )

        dh.overwrite(df_source)

    @staticmethod
    def _configure_views(tc: Configurator):
        for view_name in ["Test1View", "Test2View", "Test3View"]:
            view_checkpoint_path = "tmp/" + view_name + "/_checkpoint_path"
            tc.register(
                view_name,
                {
                    "name": "TestUpsertAutoDb{ID}." + view_name,
                    "path": "/mnt/atc/silver/TestUpsertAutoDb{ID}/" + view_name,
                    "checkpoint_path": view_checkpoint_path,
                },
            )

            if not file_exists(view_checkpoint_path):
                init_dbutils().fs.mkdirs(view_checkpoint_path)

    @staticmethod
    def _remove_checkpoints():
        for view_name in ["Test1View", "Test2View", "Test3View"]:
            view_checkpoint_path = "tmp/" + view_name + "/_checkpoint_path"
            if file_exists(view_checkpoint_path):
                init_dbutils().fs.rm(view_checkpoint_path)
