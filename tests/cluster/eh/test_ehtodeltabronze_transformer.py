import json
from datetime import timedelta

from atc_tools.testing import DataframeTestCase
from atc_tools.testing.TestHandle import TestHandle
from atc_tools.time import dt_utc
from pyspark.sql.types import (
    BinaryType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from atc.orchestrators.eh2bronze.EhToDeltaBronzeTransformer import (
    EhToDeltaBronzeTransformer,
)
from atc.spark import Spark
from atc.utils import DataframeCreator


class EhtoDeltaTransformerUnitTests(DataframeTestCase):
    _target_schema = StructType(
        [
            StructField("BodyId", LongType(), True),
            StructField("Body", StringType(), True),
            StructField("EnqueuedTimestamp", TimestampType(), True),
            StructField("StreamingTime", TimestampType(), True),
            StructField("pdate", TimestampType(), True),
        ]
    )

    _target_cols = [
        "BodyId",
        "Body",
        "EnqueuedTimestamp",
        "StreamingTime",
        "pdate",
    ]

    _capture_eventhub_output_schema = StructType(
        [
            StructField("Body", BinaryType(), True),
            StructField("pdate", TimestampType(), True),
            StructField("EnqueuedTimestamp", TimestampType(), True),
        ]
    )

    def test_01(self):
        test_handle = TestHandle(
            provides=DataframeCreator.make_partial(
                self._target_schema, self._target_cols, []
            )
        )

        df_in = Spark.get().createDataFrame(
            [
                (
                    json.dumps(
                        {
                            "id": "1234",
                            "name": "John",
                        }
                    ).encode(
                        "utf-8"
                    ),  # Body
                    dt_utc(2021, 10, 31, 0, 0, 0),  # pdate
                    dt_utc(2021, 10, 31, 0, 0, 0),  # EnqueuedTimestamp
                ),
            ],
            self._capture_eventhub_output_schema,
        )

        expected = [
            (
                146072039196263699,  # BodyId
                json.dumps(  # Body (as string)
                    {
                        "id": "1234",
                        "name": "John",
                    }
                ),
                dt_utc(2021, 10, 31, 0, 0, 0),  # EnqueuedTimestamp
                dt_utc(2021, 10, 31, 0, 0, 0),  # pdate
            ),
        ]

        df_result = EhToDeltaBronzeTransformer(test_handle).process(df_in)

        self.assertDataframeMatches(
            df_result, ["BodyId", "Body", "EnqueuedTimestamp", "pdate"], expected
        )

        # The streaming is tested assuming that the time of the transformation
        # was less than 5 min ago
        test_time = dt_utc()
        self.assertLess(
            test_time - df_result.collect()[0]["StreamingTime"], timedelta(minutes=5)
        )
