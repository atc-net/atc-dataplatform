import unittest

from atc_tools.time import TimeSequence
from pyspark.sql import functions as f
from pyspark.sql.dataframe import DataFrame

from atc.cache import CachedLoader, CachedLoaderParameters
from atc.config_master import TableConfigurator
from atc.delta import DbHandle, DeltaHandle
from atc.spark import Spark

transaction_times = TimeSequence("2021-03-14T06:00:00Z")

cache_times = TimeSequence(delta_seconds=1000)


class ChildCacher(CachedLoader):
    to_be_written: DataFrame
    written: DataFrame
    to_be_deleted: DataFrame
    deleted: DataFrame

    def write_operation(self, df: DataFrame):
        self.to_be_written = df
        self.written = df.filter(df["b"].isin([1, 2]))
        DeltaHandle.from_tc("CachedTestTarget").append(self.written)
        return self.written.withColumn("myId", f.lit(12345))

    def delete_operation(self, df: DataFrame) -> DataFrame:
        target_name = TableConfigurator().table_name("CachedTestTarget")
        self.to_be_deleted = df
        Spark.get().sql(f"DELETE FROM {target_name} WHERE b = 8")
        self.deleted = df.filter(df["b"] == 8)
        return self.deleted


class CachedLoaderTests(unittest.TestCase):

    params: CachedLoaderParameters
    old_cache = [
        (
            "3",
            3,
            453652661,
            cache_times.reverse(),
            None,
            99,
        ),  # 1000s ago, match, not for refresh
        (
            "4",
            4,
            -1687287262,
            cache_times.reverse(),
            None,
            99,
        ),  # 2000s ago, match, soft refresh, but rate suppressed
        (
            "5",
            5,
            -1814931282,
            cache_times.reverse(),
            None,
            99,
        ),  # 3000s ago, match, soft refresh
        (
            "6",
            6,
            123456789,
            cache_times.reverse(),
            None,
            99,
        ),  # 4000s ago,  mismatch, overwrite
        (
            "7",
            7,
            1284583559,
            cache_times.reverse(),
            None,
            99,
        ),  # 5000s ago, match, hard refresh
        (
            "8",
            8,
            123456789,
            cache_times.reverse(),
            None,
            99,
        ),  # unknown row, to be deleted
        (
            "9",
            9,
            123456789,
            cache_times.reverse(),
            None,
            99,
        ),  # unknown row, to be deleted
    ]

    new_data = [
        ("1", 1, "foo1"),  # new
        ("2", 2, "foo2"),  # new
        ("3", 3, "foo3"),  # match, not for refresh
        ("4", 4, "foo4"),  # 2000s ago, match, soft refresh, but rate suppressed
        ("5", 5, "foo5"),  # 3000s ago, match, soft refresh
        ("6", 6, "foo6"),  # 4000s ago, mismatch, overwrite
        ("7", 7, "foo7"),  # 5000s ago, match, hard refresh
        ("7", 7, "foo7"),  # duplicate row will only be loaded once
    ]

    expected_new_cache_after = [
        ("1", 1, -1168633837, 12345),
        ("2", 2, 1498007016, 12345),
    ]

    expected_old_cache_after = [
        ("3", 3, 453652661, 99),
        ("4", 4, -1687287262, 99),
        ("5", 5, -1814931282, 12345),
        ("6", 6, -355996791, 12345),
        ("7", 7, 1284583559, 12345),
        ("8", 8, 123456789, 99),
        ("9", 9, 123456789, 99),
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
        DbHandle.from_tc("TestDb").create()
        spark = Spark.get()
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS {CachedTest_name}
            (
                a STRING,
                b INTEGER,
                rowHash INTEGER,
                loadedTime TIMESTAMP,
                deletedTime TIMESTAMP,
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
            refresh_row_limit=5,
            max_ttl=4500,
            cache_id_cols=["myId"],
        )

        cls.sut = ChildCacher(cls.params)

    @classmethod
    def tearDownClass(cls) -> None:
        DbHandle("TestDb").drop_cascade()

    def test_01_can_perform_cached_write(self):
        cache_dh = DeltaHandle.from_tc("CachedTest")
        # prime the cache
        df_old_cache = Spark.get().createDataFrame(
            self.old_cache, schema=cache_dh.read().schema
        )
        cache_dh.overwrite(df_old_cache)

        # prepare the new data
        target_dh = DeltaHandle.from_tc("CachedTestTarget")
        df_new = Spark.get().createDataFrame(
            self.new_data, schema=target_dh.read().schema
        )

        # execute the system under test
        self.sut.save(df_new)

        cache = cache_dh.read().withColumn(
            "isRecent",
            (f.current_timestamp().cast("long") - f.col("loadedTime").cast("long"))
            < 100,
        )
        cache.show()

        # Section on writing

        # We expect that the rows that were requested for write were
        # - new rows 1 & 2
        # - soft refresh row 5
        # - mismatch row 6
        # - hard refresh row 7
        to_be_written_ids = {row.a for row in self.sut.to_be_written.collect()}
        self.assertEqual(to_be_written_ids, {"1", "2", "5", "6", "7"})
        # filtered actually written
        written_ids = {row.a for row in self.sut.written.collect()}
        self.assertEqual(written_ids, {"1", "2"})
        # verify that this agrees with the current state of the cache:
        new_cache = cache.filter(f.col("isRecent"))
        new_cache_ids = {row.a for row in new_cache}
        self.assertEqual(new_cache_ids, {"1", "2"})

        # Section on deleting
        # to be deleted were the missing rows 8 and 9
        to_be_deleted_ids = {row.a for row in self.sut.to_be_deleted.collect()}
        self.assertEqual(to_be_deleted_ids, {"8", "9"})

        # only row 8 was passed to be deleted. Check.
        del_cache = cache.filter(cache[self.sut.params.deletedTime].isNotNull())
        (del_id,) = [row.a for row in del_cache.collect()]
        self.assertEqual(del_id, "8")
