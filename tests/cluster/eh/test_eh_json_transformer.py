import json

from atc_tools.testing import DataframeTestCase
from atc_tools.time import dt_utc
from pyspark.sql.types import BinaryType, StructField, StructType, TimestampType

from atc import Configurator
from atc.delta import DeltaHandle
from atc.orchestrators.ehjson2delta.EhJsonToDeltaTransformer import (
    EhJsonToDeltaTransformer,
)
from atc.spark import Spark


class JsonEhTransformerUnitTests(DataframeTestCase):
    tc: Configurator
    capture_eventhub_output_schema = StructType(
        [
            StructField("Body", BinaryType(), True),
            StructField("pdate", TimestampType(), True),
            StructField("EnqueuedTimestamp", TimestampType(), True),
        ]
    )

    df_in = Spark.get().createDataFrame(
        [
            (
                json.dumps(
                    {
                        "id": 1234,
                        "name": "John",
                    }
                ).encode(
                    "utf-8"
                ),  # Body
                dt_utc(2021, 10, 31, 0, 0, 0),  # pdate
                dt_utc(2021, 10, 31, 0, 0, 0),  # EnqueuedTimestamp
            ),
        ],
        capture_eventhub_output_schema,
    )

    @classmethod
    def setUpClass(cls) -> None:
        cls.tc = Configurator()
        cls.tc.set_debug()
        cls.tc.clear_all_configurations()

        cls.tc.register("TblPdate2", {"name": "TablePdate2{ID}"})
        cls.tc.register("TblPdate3", {"name": "TablePdate3{ID}"})

        spark = Spark.get()

        spark.sql(f"DROP TABLE IF EXISTS {cls.tc.table_name('TblPdate2')}")
        spark.sql(f"DROP TABLE IF EXISTS {cls.tc.table_name('TblPdate3')}")

        spark.sql(
            f"""
                CREATE TABLE {cls.tc.table_name('TblPdate2')}
                (id int, name string, pdate timestamp, EnqueuedTimestamp timestamp)
                PARTITIONED BY (pdate)
            """
        )

        spark.sql(
            f"""
                CREATE TABLE {cls.tc.table_name('TblPdate3')}
                (id int, name string, pdate timestamp, EnqueuedTimestamp timestamp,
                Unknown string)
                PARTITIONED BY (pdate)
            """
        )

    def test_01_transformer(self):
        """Test if the data is correctly extracted"""
        dh = DeltaHandle.from_tc("TblPdate2")

        expected = [
            (
                1234,
                "John",
                dt_utc(2021, 10, 31, 0, 0, 0),
                dt_utc(2021, 10, 31, 0, 0, 0),
            ),
        ]

        df_result = EhJsonToDeltaTransformer(target_dh=dh).process(self.df_in)

        # Check that data is correct
        self.assertDataframeMatches(df_result, None, expected)

    def test_02_transformer_unknown_target_field(self):
        """This should test what happens if the target
        schema has a field that does not exist in the source dataframe."""
        dh = DeltaHandle.from_tc("TblPdate3")

        expected = [
            (
                1234,
                "John",
                dt_utc(2021, 10, 31, 0, 0, 0),
                dt_utc(2021, 10, 31, 0, 0, 0),
            ),
        ]

        df_result = EhJsonToDeltaTransformer(target_dh=dh).process(self.df_in)

        # Check that data is correct
        self.assertDataframeMatches(df_result, None, expected)
