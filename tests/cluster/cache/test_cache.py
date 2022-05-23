import unittest

from atc_tools.time import TimeSequence
from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql.dataframe import DataFrame

from atc.cache import CachedLoader, CachedLoaderParameters
from atc.config_master import TableConfigurator
from atc.delta import DbHandle, DeltaHandle
from atc.spark import Spark

transaction_times = TimeSequence("2021-03-14T06:00:00Z")

cache_times = TimeSequence(delta_seconds=1000)


class ChildCacher(CachedLoader):
    written: DataFrame

    def write_operation(self, df: DataFrame):
        self.written = df
        return df.withColumn("myId", f.lit(12345)).cache()


class CachedLoaderTests(unittest.TestCase):

    params: CachedLoaderParameters
    old_cache = [
        (
            "3",
            3,
            453652661,
            cache_times.reverse(),
            99,
        ),  # 1000s ago, match, not for refresh
        (
            "4",
            4,
            -1687287262,
            cache_times.reverse(),
            99,
        ),  # 2000s ago, match, soft refresh, but rate suppressed
        (
            "5",
            5,
            -1814931282,
            cache_times.reverse(),
            99,
        ),  # 3000s ago, match, soft refresh
        ("6", 6, 1234, cache_times.reverse(), 99),  # 4000s ago,  mismatch, overwrite
        (
            "7",
            7,
            1284583559,
            cache_times.reverse(),
            99,
        ),  # 5000s ago, match, hard refresh
        ("8", 8, 1234, cache_times.reverse(), 99),  # unknown row, no update
    ]

    new_data = [
        ("1", 1, "foo1"),  # new
        ("2", 2, "foo2"),  # new
        ("3", 3, "foo3"),  # match, not for refresh
        ("4", 4, "foo4"),  # 2000s ago, match, soft refresh, but rate suppressed
        ("5", 5, "foo5"),  # 3000s ago, match, soft refresh
        ("6", 6, "foo6"),  # 4000s ago, match, mismatch, overwrite
        ("7", 7, "foo7"),  # 5000s ago, match, hard refresh refresh
        ("7", 7, "foo7"),  # duplicate row will only be loaded once
    ]

    expected_new_cache_after = [
        ("1", 1, -1168633837, 12345),
        ("2", 2, 1498007016, 12345),
        ("5", 5, -1814931282, 12345),
        ("6", 6, -355996791, 12345),
        ("7", 7, 1284583559, 12345),
    ]

    expected_old_cache_after = [
        ("3", 3, 453652661, 99),
        ("4", 4, -1687287262, 99),
        ("8", 8, 1234, 99),
    ]

    expected_data_written = [
        ("1", 1, "foo1"),  # new
        ("2", 2, "foo2"),  # new
        ("5", 5, "foo5"),  # 2000s ago, match, soft refresh
        ("6", 6, "foo6"),  # 4000s ago, match, mismatch, overwrite
        ("7", 7, "foo7"),  # 5000s ago, match, hard refresh refresh
    ]

    @classmethod
    def setUpClass(cls) -> None:
        tc = TableConfigurator()
        tc.clear_all_configurations()
        tc.set_debug()

        tc.register("TestDb", dict(name="test{ID}", path="/tmp/test{ID}.db"))
        tc.register(
            "CachedTest",
            dict(
                name="test{ID}.cachedloader_cache",
                path="/tmp/test{ID}.db/cachedloader_cache",
            ),
        )
        tc.register(
            "CachedTestTarget",
            dict(
                name="test{ID}.cachedloader_target",
                path="/tmp/test{ID}.db/cachedloader_target",
            ),
        )
        DbHandle("TestDb").create()
        spark = Spark.get()
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS {CachedTest_name}
            (
                a STRING,
                b INTEGER,
                rowHash INTEGER,
                loadedTime TIMESTAMP,
                myId INTEGER
            )
            USING DELTA
            COMMENT "Caching Test"
            LOCATION "{CachedTest_path}"
        """.format(
                **tc.get_all_details()
            )
        )

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS {CachedTestTarget_name}
            (
                a STRING,
                b INTEGER,
                payload STRING
            )
            USING DELTA
            COMMENT "Caching target"
            LOCATION "{CachedTestTarget_path}"
        """.format(
                **tc.get_all_details()
            )
        )

        cls.params = CachedLoaderParameters(
            cache_table_name=tc.table_name("CachedTest"),
            key_cols=["a", "b"],
            refresh_ttl=1500,
            refresh_row_limit=3,
            max_ttl=4500,
            cache_id_cols=["myId"],
        )

        cls.sut = ChildCacher(cls.params)

    @classmethod
    def tearDownClass(cls) -> None:
        DbHandle("TestDb").drop_cascade()

    def test_01_can_perform_cached_write(self):
        df_new = Spark.get().createDataFrame(
            self.new_data, schema=DeltaHandle("CachedTestTarget").read().schema
        )

        self.sut.save(df_new)
        print(self.sut.written.columns)
        rows = {tuple(row) for row in self.sut.written.collect()}
        self.assertEqual(set(self.expected_data_written), rows)

        cache = (
            Spark.get()
            .table(self.params.cache_table_name)
            .withColumn(
                "isRecent",
                (
                    f.current_timestamp().cast(t.LongType())
                    - f.col("loadedTime").cast(t.LongType())
                )
                < 100,
            )
        )
        cache.show()
        new_cache = cache.filter(f.col("isRecent"))
        old_cache = cache.filter(~f.col("isRecent"))

        new_cache_rows = {(row.a, row.b, row.rowHash, row.myId) for row in new_cache}
        self.assertEqual(set(self.expected_new_cache_after), new_cache_rows)

        old_cache_rows = {(row.a, row.b, row.rowHash, row.myId) for row in old_cache}
        self.assertEqual(set(self.expected_old_cache_after), old_cache_rows)
