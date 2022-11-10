import time
import unittest

from py4j.protocol import Py4JJavaError
from pyspark.sql.utils import AnalysisException

from atc import Configurator
from atc.delta import DbHandle, DeltaHandle
from atc.etl import Orchestrator
from atc.etl.extractors import SimpleExtractor
from atc.etl.loaders import SimpleLoader
from atc.functions import init_dbutils
from atc.spark import Spark
from tests.cluster.config import InitConfigurator


class DeltaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        InitConfigurator(clear=True)

    def test_01_configure(self):
        tc = Configurator()

        tc.register(
            "MyDb", {"name": "TestDb{ID}", "path": "{storageAccount}/testdb{ID}"}
        )

        tc.register(
            "MyTbl",
            {
                "name": "{MyDb}.TestTbl",
                "path": "{MyDb_path}/testtbl",
            },
        )

        tc.register(
            "MyTbl2",
            {
                "name": "{MyDb}.TestTbl2",
            },
        )

        tc.register(
            "MyTbl3",
            {
                "path": "{storageAccount}/testdb/testtbl3",
            },
        )

        # test instantiation without error
        DbHandle.from_tc("MyDb")
        DeltaHandle.from_tc("MyTbl")
        DeltaHandle.from_tc("MyTbl2")

    def test_02_write(self):
        dh = DeltaHandle.from_tc("MyTbl")

        df = Spark.get().createDataFrame([(1, "a"), (2, "b")], "id int, name string")

        dh.overwrite(df, mergeSchema=True)
        dh.append(df, mergeSchema=False)  # schema matches

        df = Spark.get().createDataFrame(
            [(1, "a", "yes"), (2, "b", "no")],
            """
            id int,
            name string,
            response string
            """,
        )

        dh.append(df, mergeSchema=True)

    # @unittest.skip("Flaky test")
    def test_03_create(self):
        # print(Configurator().get_all_details())
        # print(
        #     {
        #         k: v[:-15] + v[-12:]
        #         for k, v in Spark.get().sparkContext.getConf().getAll()
        #         if k.startswith("fs.azure.account")
        #     }
        # )

        db = DbHandle.from_tc("MyDb")
        db.create()

        dh = DeltaHandle.from_tc("MyTbl")
        tc = Configurator()
        print(init_dbutils().fs.ls(tc.get("MyTbl", "path")))
        print(
            init_dbutils().fs.put(
                tc.get("MyTbl", "path") + "/some.file.txt", "Hello, ATC!", True
            )
        )
        print(init_dbutils().fs.ls(tc.get("MyTbl", "path")))
        for i in range(10, 0, -1):
            try:
                dh.create_hive_table()
                break
            except (AnalysisException, Py4JJavaError) as e:
                if i > 0:
                    print(e)
                    print("trying again in 10 seconds")
                    time.sleep(10)
                else:
                    raise e

        # test hive access:
        df = Spark.get().table("TestDb.TestTbl")
        self.assertTrue(6, df.count())

    def test_04_read(self):
        df = DeltaHandle.from_tc("MyTbl").read()
        self.assertEqual(6, df.count())

    def test_05_truncate(self):
        dh = DeltaHandle.from_tc("MyTbl")
        dh.truncate()
        df = dh.read()
        self.assertEqual(0, df.count())

    def test_06_etl(self):
        o = Orchestrator()
        o.extract_from(
            SimpleExtractor(DeltaHandle.from_tc("MyTbl"), dataset_key="MyTbl")
        )
        o.load_into(SimpleLoader(DeltaHandle.from_tc("MyTbl"), mode="overwrite"))
        o.execute()

    def test_07_write_path_only(self):
        # check that we can write to the table with no path
        df = DeltaHandle.from_tc("MyTbl").read()

        dh3 = DeltaHandle.from_tc("MyTbl3")

        dh3.append(df, mergeSchema=True)

        df = dh3.read()
        df.show()

    def test_08_delete(self):
        dh = DeltaHandle.from_tc("MyTbl")
        dh.drop_and_delete()

        with self.assertRaises(AnalysisException):
            dh.read()

    def test_09_partitioning(self):
        dh = DeltaHandle.from_tc("MyTbl")
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

        dh2 = DeltaHandle.from_tc("MyTbl2")
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
