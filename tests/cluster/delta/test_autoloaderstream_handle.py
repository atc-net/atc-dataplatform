import unittest
import uuid as _uuid
from typing import List, Tuple

from atc import Configurator
from atc.delta import AutoloaderStreamHandle, DbHandle, DeltaHandle, DeltaStreamHandle
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
        # add avro source
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

        # Add sink table
        sink_checkpoint_path = "/mnt/atc/silver/testdb{ID}/_checkpoint_path_avrosink"
        init_dbutils().fs.mkdirs(sink_checkpoint_path)
        # add eventhub sink
        tc.register(
            "AvroSink",
            {
                "name": "{MyDb}.AvroSink",
                "path": "{MyDb_path}/AvroSink",
                "format": "delta",
                "checkpoint_path": sink_checkpoint_path,
            },
        )

        # test instantiation without error
        DbHandle.from_tc("MyDb")
        AutoloaderStreamHandle.from_tc("AvroSource")
        DeltaStreamHandle.from_tc("AvroSink")

    def test_01_read_avro(self):

        self._add_avro_data_to_source([(1, "a"), (2, "b")])

        dsh_sink = DeltaStreamHandle.from_tc("AvroSink")
        Spark.get().sql(
            f"""
                    CREATE TABLE {dsh_sink.get_tablename()}
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
        o.load_into(SimpleLoader(dsh_sink, mode="append"))
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

        # Add specific data to source
        self._add_specific_data_to_source()
        o.execute()
        self.assertTrue(5, result.count())

        # Test what happens if data is altered
        self._alter_specific_data()
        o.execute()
        result.show()

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

    def _add_specific_data_to_source(self):
        df = Spark.get().createDataFrame([(10, "specific")], "id int, name string")

        df.write.format("avro").save(self.avro_source_path + "/specific")

    def _alter_specific_data(self):
        df = Spark.get().createDataFrame([(11, "specific")], "id int, name string")

        df.write.format("avro").save(self.avro_source_path + "/specific")
