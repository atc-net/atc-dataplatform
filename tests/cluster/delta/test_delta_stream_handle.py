import unittest

from pyspark.sql.utils import AnalysisException

from atc import Configurator
from atc.delta import DbHandle, DeltaHandle, DeltaStreamHandle
from atc.etl import Orchestrator
from atc.etl.extractors import SimpleExtractor
from atc.etl.loaders import SimpleLoader
from atc.spark import Spark
from atc.utils.stop_all_streams import stop_all_streams


@unittest.skipUnless(
    Spark.version() >= Spark.DATABRICKS_RUNTIME_10_4,
    f"DeltaStreamHandle not available for Spark version {Spark.version()}",
)
class DeltaStreamHandleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        Configurator().clear_all_configurations()
        Configurator().set_debug()

    @classmethod
    def tearDownClass(cls) -> None:
        DbHandle.from_tc("MyDb").drop_cascade()
        stop_all_streams()

    def test_01_configure(self):
        tc = Configurator()
        tc.register(
            "MyDb", {"name": "TestDb{ID}", "path": "/mnt/atc/silver/testdb{ID}"}
        )

        tc.register(
            "MyTbl",
            {
                "name": "TestDb{ID}.TestTbl",
                "path": "/mnt/atc/silver/testdb{ID}/testtbl",
                "format": "delta",
                "checkpoint_path": "/mnt/atc/silver/testdb{ID}/_checkpoint_path_tbl",
            },
        )

        mirror_cp_path = "/mnt/atc/silver/testdb{ID}/_checkpoint_path_tblmirror"
        tc.register(
            "MyTblMirror",
            {
                "name": "TestDb{ID}.TestTblMirror",
                "path": "/mnt/atc/silver/testdb{ID}/testtblmirror",
                "format": "delta",
                "checkpoint_path": mirror_cp_path,
                "await_termination": True,
            },
        )

        tc.register(
            "MyTbl2",
            {
                "name": "TestDb{ID}.TestTbl2",
                "format": "delta",
                "checkpoint_path": "/mnt/atc/silver/testdb{ID}/_checkpoint_path_tbl2",
            },
        )

        tc.register(
            "MyTbl3",
            {
                "path": "/mnt/atc/silver/testdb{ID}/testtbl3",
                "format": "delta",
                "checkpoint_path": "/mnt/atc/silver/testdb{ID}/_checkpoint_path_tbl3",
                "await_termination": True,
            },
        )

        tc.register(
            "MyTbl4",
            {
                "name": "TestDb{ID}.TestTbl4",
                "path": "/mnt/atc/silver/testdb{ID}/testtbl4",
                "format": "delta",
                "checkpoint_path": "/mnt/atc/silver/testdb{ID}/_checkpoint_path_tbl4",
            },
        )

        tc.register(
            "MyTbl5",
            {
                "name": "TestDb{ID}.TestTbl5",
                "path": "/mnt/atc/silver/testdb{ID}/testtbl5",
                "format": "delta",
                "checkpoint_path": "/mnt/atc/silver/testdb{ID}/_checkpoint_path_tbl5",
            },
        )

        # test instantiation without error
        DbHandle.from_tc("MyDb")
        DeltaStreamHandle.from_tc("MyTbl")
        DeltaStreamHandle.from_tc("MyTblMirror")
        DeltaStreamHandle.from_tc("MyTbl2")
        DeltaStreamHandle.from_tc("MyTbl3")
        DeltaStreamHandle.from_tc("MyTbl4")
        DeltaStreamHandle.from_tc("MyTbl5")

    def test_02_write_data_with_deltahandle(self):
        self._overwrite_two_rows_to_table("MyTbl")

    def test_03_create(self):
        db = DbHandle.from_tc("MyDb")
        db.create()

        dsh = DeltaStreamHandle.from_tc("MyTbl")
        dsh.create_hive_table()

        # test hive access:
        df = DeltaHandle.from_tc("MyTbl").read()
        self.assertEqual(2, df.count())

    def test_04_read(self):
        df = DeltaStreamHandle.from_tc("MyTbl").read()
        self.assertTrue(df.isStreaming)

    def test_05_truncate(self):
        dsh = DeltaStreamHandle.from_tc("MyTbl")
        dsh.truncate()

        result = DeltaHandle.from_tc("MyTbl").read()
        self.assertEqual(0, result.count())

    def test_06_etl(self):
        self._overwrite_two_rows_to_table("MyTbl")
        self._create_tbl_mirror()

        o = Orchestrator()
        o.extract_from(
            SimpleExtractor(DeltaStreamHandle.from_tc("MyTbl"), dataset_key="MyTbl")
        )
        o.load_into(
            SimpleLoader(DeltaStreamHandle.from_tc("MyTblMirror"), mode="append")
        )
        o.execute()

        result = DeltaHandle.from_tc("MyTblMirror").read()
        self.assertEqual(2, result.count())

    def test_07_write_path_only(self):
        self._overwrite_two_rows_to_table("MyTbl")
        # check that we can write to the table with no "name" property
        ah = DeltaStreamHandle.from_tc("MyTbl").read()

        dsh3 = DeltaStreamHandle.from_tc("MyTbl3")

        dsh3.append(ah, mergeSchema=True)

        # Read data from mytbl3
        result = DeltaHandle.from_tc("MyTbl3").read()
        self.assertEqual(2, result.count())

    def test_08_delete(self):
        dsh = DeltaStreamHandle.from_tc("MyTbl")
        dsh.drop_and_delete()

        ah = DeltaStreamHandle.from_tc("MyTbl")

        with self.assertRaises(AnalysisException):
            ah.read()

    def test_09_partitioning(self):
        dsh = DeltaStreamHandle.from_tc("MyTbl4")
        Spark.get().sql(
            f"""
            CREATE TABLE IF NOT EXISTS {dsh.get_tablename()}
            (
            colA string,
            colB int,
            payload string
            )
            PARTITIONED BY (colB,colA)
        """
        )

        self.assertEqual(dsh.get_partitioning(), ["colB", "colA"])

        dsh2 = DeltaStreamHandle.from_tc("MyTbl5")
        Spark.get().sql(
            f"""
            CREATE TABLE IF NOT EXISTS {dsh2.get_tablename()}
            (
            colA string,
            colB int,
            payload string
            )
        """
        )

        self.assertEqual(dsh2.get_partitioning(), [])

    def _overwrite_two_rows_to_table(self, tblid: str):
        dh = DeltaHandle.from_tc(tblid)

        df = Spark.get().createDataFrame([(1, "a"), (2, "b")], "id int, name string")

        dh.overwrite(df, mergeSchema=True)

    def _create_tbl_mirror(self):
        dh = DeltaHandle.from_tc("MyTblMirror")
        Spark.get().sql(
            f"""
                            CREATE TABLE {dh.get_tablename()}
                            (
                            id int,
                            name string
                            )
                        """
        )
