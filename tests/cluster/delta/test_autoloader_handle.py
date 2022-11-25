import unittest

from pyspark.sql.utils import AnalysisException

from atc import Configurator
from atc.delta import DbHandle, DeltaHandle
from atc.delta.autoloader_handle import AutoLoaderHandle
from atc.etl import Orchestrator
from atc.etl.extractors import SimpleExtractor
from atc.etl.loaders import SimpleLoader
from atc.functions import init_dbutils
from atc.spark import Spark
from atc.utils.FileExists import file_exists
from atc.utils.stop_all_streams import stop_all_streams
from tests.cluster.values import resourceName


class AutoloaderTests(unittest.TestCase):
    eh_checkpoint_path = "/mnt/atc/silver/eh/_checkpoint_path"

    @classmethod
    def setUpClass(cls) -> None:
        Configurator().clear_all_configurations()

        if not file_exists(cls.eh_checkpoint_path):
            init_dbutils().fs.mkdirs(cls.eh_checkpoint_path)

    @classmethod
    def tearDownClass(cls) -> None:
        DbHandle.from_tc("MyDb").drop_cascade()
        if file_exists(cls.eh_checkpoint_path):
            init_dbutils().fs.rm(cls.eh_checkpoint_path, True)
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
                "checkpoint_path": "/mnt/atc/silver/testdb{ID}/_checkpoint_path_tbl",
            },
        )

        mirror_cp_path = "/mnt/atc/silver/testdb{ID}/_checkpoint_path_tblmirror"
        tc.register(
            "MyTblMirror",
            {
                "name": "TestDb{ID}.TestTblMirror",
                "path": "/mnt/atc/silver/testdb{ID}/testtblmirror",
                "checkpoint_path": mirror_cp_path,
            },
        )

        tc.register(
            "MyTbl2",
            {
                "name": "TestDb{ID}.TestTbl2",
                "checkpoint_path": "/mnt/atc/silver/testdb{ID}/_checkpoint_path_tbl2",
            },
        )

        tc.register(
            "MyTbl3",
            {
                "path": "/mnt/atc/silver/testdb{ID}/testtbl3",
                "checkpoint_path": "/mnt/atc/silver/testdb{ID}/_checkpoint_path_tbl3",
            },
        )

        tc.register(
            "MyTbl4",
            {
                "name": "TestDb{ID}.TestTbl4",
                "path": "/mnt/atc/silver/testdb{ID}/testtbl4",
                "checkpoint_path": "/mnt/atc/silver/testdb{ID}/_checkpoint_path_tbl4",
            },
        )

        tc.register(
            "MyTbl5",
            {
                "name": "TestDb{ID}.TestTbl5",
                "path": "/mnt/atc/silver/testdb{ID}/testtbl5",
                "checkpoint_path": "/mnt/atc/silver/testdb{ID}/_checkpoint_path_tbl5",
            },
        )

        eh_checkpoint_path = "/mnt/atc/silver/eh/_checkpoint_path"
        init_dbutils().fs.mkdirs(eh_checkpoint_path)

        # add eventhub
        tc.register(
            "AtcEh",
            {
                "name": "AtcEh",
                "path": f"/mnt/{resourceName()}/silver/{resourceName()}/atceh",
                "format": "avro",
                "partitioning": "ymd",
                "checkpoint_path": self.eh_checkpoint_path,
            },
        )

        init_dbutils().fs.mkdirs(eh_checkpoint_path)
        # add eventhub sink
        tc.register(
            "EhSink",
            {
                "name": "TestDb{ID}.EhSink",
                "path": "/mnt/atc/silver/testdb{ID}/EhSink",
                "format": "delta",
                "checkpoint_path": "/mnt/atc/silver/testdb{ID}/_checkpoint_path_ehsink",
            },
        )

        # test instantiation without error
        DbHandle.from_tc("MyDb")
        AutoLoaderHandle.from_tc("MyTbl")
        AutoLoaderHandle.from_tc("MyTblMirror")
        AutoLoaderHandle.from_tc("MyTbl2")
        AutoLoaderHandle.from_tc("MyTbl3")
        AutoLoaderHandle.from_tc("AtcEh")
        AutoLoaderHandle.from_tc("EhSink")

    def test_02_write_data_with_deltahandle(self):
        self._overwrite_two_rows_to_table("MyTbl")

    def test_03_create(self):
        db = DbHandle.from_tc("MyDb")
        db.create()

        ah = AutoLoaderHandle.from_tc("MyTbl")
        ah.create_hive_table()

        # test hive access:
        df = Spark.get().table("TestDb.TestTbl")
        self.assertTrue(6, df.count())

    def test_04_read(self):
        df = AutoLoaderHandle.from_tc("MyTbl").read()
        self.assertTrue(df.isStreaming)

    def test_05_truncate(self):
        ah = AutoLoaderHandle.from_tc("MyTbl")
        ah.truncate()

        result = DeltaHandle.from_tc("MyTbl").read()
        self.assertEqual(0, result.count())

    def test_06_etl(self):
        self._overwrite_two_rows_to_table("MyTbl")
        self._create_tbl_mirror()

        o = Orchestrator()
        o.extract_from(
            SimpleExtractor(AutoLoaderHandle.from_tc("MyTbl"), dataset_key="MyTbl")
        )
        o.load_into(
            SimpleLoader(AutoLoaderHandle.from_tc("MyTblMirror"), mode="append")
        )
        o.execute()

        result = DeltaHandle.from_tc("MyTblMirror").read()
        self.assertEqual(2, result.count())

    def test_07_write_path_only(self):
        self._overwrite_two_rows_to_table("MyTbl")
        # check that we can write to the table with no "name" property
        ah = AutoLoaderHandle.from_tc("MyTbl").read()

        ah3 = AutoLoaderHandle.from_tc("MyTbl3")

        ah3.append(ah, mergeSchema=True)

        # Read data from mytbl3
        result = DeltaHandle.from_tc("MyTbl3").read()
        self.assertEqual(2, result.count())

    def test_08_delete(self):
        dh = AutoLoaderHandle.from_tc("MyTbl")
        dh.drop_and_delete()

        with self.assertRaises(AnalysisException):
            dh.read()

    def test_09_read_avro(self):

        ah_sink = AutoLoaderHandle.from_tc("EhSink")
        Spark.get().sql(
            f"""
                    CREATE TABLE {ah_sink.get_tablename()}
                    (
                    id int,
                    name string
                    )
                """
        )

        o = Orchestrator()
        o.extract_from(
            SimpleExtractor(AutoLoaderHandle.from_tc("AtcEh"), dataset_key="AtcEh")
        )
        o.load_into(SimpleLoader(ah_sink, mode="append"))
        o.execute()

        result = DeltaHandle.from_tc("EhSink").read()

        self.assertTrue(0 < result.count())

    def test_10_partitioning(self):
        dh = AutoLoaderHandle.from_tc("MyTbl4")
        Spark.get().sql(
            f"""
            CREATE TABLE {dh.get_tablename()}
            (
            colA string,
            colB int,
            payload string
            )
            PARTITIONED BY (colB,colA)
        """
        )

        self.assertEqual(dh.get_partitioning(), ["colB", "colA"])

        dh2 = AutoLoaderHandle.from_tc("MyTbl5")
        Spark.get().sql(
            f"""
            CREATE TABLE {dh2.get_tablename()}
            (
            colA string,
            colB int,
            payload string
            )
        """
        )

        self.assertEqual(dh2.get_partitioning(), [])

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
