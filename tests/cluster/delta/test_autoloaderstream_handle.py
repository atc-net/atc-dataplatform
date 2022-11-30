import unittest
import uuid as _uuid
from typing import List, Tuple

from pyspark.sql.utils import AnalysisException

from atc import Configurator
from atc.delta import DbHandle, DeltaHandle
from atc.delta.autoloaderstream_handle import AutoloaderStreamHandle
from atc.etl import Orchestrator
from atc.etl.extractors import SimpleExtractor
from atc.etl.loaders import SimpleLoader
from atc.functions import init_dbutils
from atc.spark import Spark
from atc.utils.FileExists import file_exists
from atc.utils.stop_all_streams import stop_all_streams
from tests.cluster.values import resourceName


class AutoloaderTests(unittest.TestCase):
    avrosource_checkpoint_path = (
        f"/mnt/{resourceName()}/silver/{resourceName()}"
        f"/avrolocation/_checkpoint_path_avro"
    )

    avro_source_path = (
        f"/mnt/{resourceName()}/silver/{resourceName()}/avrolocation/AvroSource"
    )

    @classmethod
    def setUpClass(cls) -> None:
        Configurator().clear_all_configurations()
        Configurator().set_debug()

        if not file_exists(cls.avrosource_checkpoint_path):
            init_dbutils().fs.mkdirs(cls.avrosource_checkpoint_path)

        if not file_exists(cls.avro_source_path):
            init_dbutils().fs.mkdirs(cls.avro_source_path)

    @classmethod
    def tearDownClass(cls) -> None:
        DbHandle.from_tc("MyDb").drop_cascade()
        if file_exists(cls.avrosource_checkpoint_path):
            init_dbutils().fs.rm(cls.avrosource_checkpoint_path, True)

        if file_exists(cls.avro_source_path):
            init_dbutils().fs.rm(cls.avro_source_path, True)
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

        # add eventhub
        tc.register(
            "AvroSource",
            {
                "name": "AvroSource",
                "path": self.avro_source_path,
                "format": "avro",
                "partitioning": "ymd",
                "checkpoint_path": self.avrosource_checkpoint_path,
            },
        )

        sink_checkpoint_path = "/mnt/atc/silver/testdb{ID}/_checkpoint_path_avrosink"
        init_dbutils().fs.mkdirs(sink_checkpoint_path)
        # add eventhub sink
        tc.register(
            "AvroSink",
            {
                "name": "TestDb{ID}.AvroSink",
                "path": "/mnt/atc/silver/testdb{ID}/AvroSink",
                "format": "delta",
                "checkpoint_path": sink_checkpoint_path,
            },
        )

        # test instantiation without error
        DbHandle.from_tc("MyDb")
        AutoloaderStreamHandle.from_tc("MyTbl")
        AutoloaderStreamHandle.from_tc("MyTblMirror")
        AutoloaderStreamHandle.from_tc("MyTbl2")
        AutoloaderStreamHandle.from_tc("MyTbl3")
        AutoloaderStreamHandle.from_tc("MyTbl4")
        AutoloaderStreamHandle.from_tc("MyTbl5")
        AutoloaderStreamHandle.from_tc("AvroSource")
        AutoloaderStreamHandle.from_tc("AvroSink")

    def test_02_write_data_with_deltahandle(self):
        self._overwrite_two_rows_to_table("MyTbl")

    def test_03_create(self):
        db = DbHandle.from_tc("MyDb")
        db.create()

        ah = AutoloaderStreamHandle.from_tc("MyTbl")
        ah.create_hive_table()

        # test hive access:
        df = DeltaHandle.from_tc("MyTbl").read()
        self.assertTrue(6, df.count())

    def test_04_read(self):
        df = AutoloaderStreamHandle.from_tc("MyTbl").read()
        self.assertTrue(df.isStreaming)

    def test_05_truncate(self):
        ah = AutoloaderStreamHandle.from_tc("MyTbl")
        ah.truncate()

        result = DeltaHandle.from_tc("MyTbl").read()
        self.assertEqual(0, result.count())

    def test_06_etl(self):
        self._overwrite_two_rows_to_table("MyTbl")
        self._create_tbl_mirror()

        o = Orchestrator()
        o.extract_from(
            SimpleExtractor(
                AutoloaderStreamHandle.from_tc("MyTbl"), dataset_key="MyTbl"
            )
        )
        o.load_into(
            SimpleLoader(AutoloaderStreamHandle.from_tc("MyTblMirror"), mode="append")
        )
        o.execute()

        result = DeltaHandle.from_tc("MyTblMirror").read()
        self.assertEqual(2, result.count())

    def test_07_write_path_only(self):
        self._overwrite_two_rows_to_table("MyTbl")
        # check that we can write to the table with no "name" property
        ah = AutoloaderStreamHandle.from_tc("MyTbl").read()

        ah3 = AutoloaderStreamHandle.from_tc("MyTbl3")

        ah3.append(ah, mergeSchema=True)

        # Read data from mytbl3
        result = DeltaHandle.from_tc("MyTbl3").read()
        self.assertEqual(2, result.count())

    def test_08_delete(self):
        dh = AutoloaderStreamHandle.from_tc("MyTbl")
        dh.drop_and_delete()

        with self.assertRaises(AnalysisException):
            dh.read()

    def test_09_read_avro(self):

        self._add_avro_data_to_source([(1, "a"), (2, "b")])

        ah_sink = AutoloaderStreamHandle.from_tc("AvroSink")
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
            SimpleExtractor(
                AutoloaderStreamHandle.from_tc("AvroSource"), dataset_key="AvroSource"
            )
        )
        o.load_into(SimpleLoader(ah_sink, mode="append"))
        o.execute()

        result = DeltaHandle.from_tc("AvroSink").read()

        self.assertTrue(2, result.count())

        # Run again. Should not append more.
        o.execute()
        self.assertTrue(2, result.count())

        self._add_avro_data_to_source([(3, "c"), (4, "d")])

        # Run again. Should append.
        o.execute()
        self.assertTrue(4, result.count())

    def test_10_partitioning(self):
        dh = AutoloaderStreamHandle.from_tc("MyTbl4")
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

        dh2 = AutoloaderStreamHandle.from_tc("MyTbl5")
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

    def _add_avro_data_to_source(self, input_data: List[Tuple[int, str]]):
        df = Spark.get().createDataFrame(input_data, "id int, name string")

        df.write.format("avro").save(self.avro_source_path + "/" + str(_uuid.uuid4()))
