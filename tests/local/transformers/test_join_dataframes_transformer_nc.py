import unittest

import pyspark.sql.types as T
from atc_tools.testing import DataframeTestCase

from atc.spark import Spark
from atc.transformers.join_dataframes_transformer_nc import JoinDataframesTransformerNC


class TestJoinDataframesTransformerNC(DataframeTestCase):
    def test_data_join_inner(self):
        dataset = {}

        input1_schema = T.StructType(
            [
                T.StructField("Id", T.StringType(), True),
                T.StructField("Name", T.StringType(), True),
            ]
        )

        input1_data = [
            ("Id1", "Name1"),
            ("Id2", "Name2"),
        ]
        dataset["input1_df"] = Spark.get().createDataFrame(
            schema=input1_schema, data=input1_data
        )

        input2_schema = T.StructType(
            [
                T.StructField("_Id", T.StringType(), True),
                T.StructField("Country", T.StringType(), True),
            ]
        )

        input2_data = [
            ("Id1", "Country1"),
            ("Id2", "Country2"),
            ("Id3", "Country3"),
        ]

        dataset["input2_df"] = Spark.get().createDataFrame(
            schema=input2_schema, data=input2_data
        )

        df_transformed = JoinDataframesTransformerNC(
            first_dataframe_join_key="Id",
            second_dataframe_join_key="_Id",
            join_type="inner",
            dataset_input_keys=["input1_df", "input2_df"],
        ).process_many(dataset)

        expected_data = [
            (
                "Id1",
                "Name1",
                "Id1",
                "Country1",
            ),
            (
                "Id2",
                "Name2",
                "Id2",
                "Country2",
            ),
        ]

        self.assertDataframeMatches(
            df=df_transformed,
            expected_data=expected_data,
        )

    def test_data_join_outer(self):
        dataset = {}

        input1_schema = T.StructType(
            [
                T.StructField("Id", T.StringType(), True),
                T.StructField("Name", T.StringType(), True),
            ]
        )

        input1_data = [
            ("Id1", "Name1"),
            ("Id2", "Name2"),
        ]
        dataset["input1_df"] = Spark.get().createDataFrame(
            schema=input1_schema, data=input1_data
        )

        input2_schema = T.StructType(
            [
                T.StructField("_Id", T.StringType(), True),
                T.StructField("Country", T.StringType(), True),
            ]
        )

        input2_data = [
            ("Id1", "Country1"),
            ("Id2", "Country2"),
            ("Id3", "Country3"),
        ]

        dataset["input2_df"] = Spark.get().createDataFrame(
            schema=input2_schema, data=input2_data
        )

        df_transformed = JoinDataframesTransformerNC(
            first_dataframe_join_key="Id",
            second_dataframe_join_key="_Id",
            join_type="outer",
            dataset_input_keys=["input1_df", "input2_df"],
        ).process_many(dataset)

        expected_data = [
            (
                "Id1",
                "Name1",
                "Id1",
                "Country1",
            ),
            (
                "Id2",
                "Name2",
                "Id2",
                "Country2",
            ),
            (None, None, "Id3", "Country3"),
        ]

        self.assertDataframeMatches(
            df=df_transformed,
            expected_data=expected_data,
        )

    def test_data_join_left_outer(self):
        dataset = {}

        input1_schema = T.StructType(
            [
                T.StructField("Id", T.StringType(), True),
                T.StructField("Name", T.StringType(), True),
            ]
        )

        input1_data = [
            ("Id1", "Name1"),
            ("Id2", "Name2"),
        ]
        dataset["input1_df"] = Spark.get().createDataFrame(
            schema=input1_schema, data=input1_data
        )

        input2_schema = T.StructType(
            [
                T.StructField("_Id", T.StringType(), True),
                T.StructField("Country", T.StringType(), True),
            ]
        )

        input2_data = [
            ("Id1", "Country1"),
            ("Id2", "Country2"),
            ("Id3", "Country3"),
        ]

        dataset["input2_df"] = Spark.get().createDataFrame(
            schema=input2_schema, data=input2_data
        )

        df_transformed = JoinDataframesTransformerNC(
            first_dataframe_join_key="Id",
            second_dataframe_join_key="_Id",
            join_type="left_outer",
            dataset_input_keys=["input1_df", "input2_df"],
        ).process_many(dataset)

        expected_data = [
            (
                "Id1",
                "Name1",
                "Id1",
                "Country1",
            ),
            (
                "Id2",
                "Name2",
                "Id2",
                "Country2",
            ),
        ]

        self.assertDataframeMatches(
            df=df_transformed,
            expected_data=expected_data,
        )

    def test_data_join_right_outer(self):
        dataset = {}

        input1_schema = T.StructType(
            [
                T.StructField("Id", T.StringType(), True),
                T.StructField("Name", T.StringType(), True),
            ]
        )

        input1_data = [
            ("Id1", "Name1"),
            ("Id2", "Name2"),
        ]
        dataset["input1_df"] = Spark.get().createDataFrame(
            schema=input1_schema, data=input1_data
        )

        input2_schema = T.StructType(
            [
                T.StructField("_Id", T.StringType(), True),
                T.StructField("Country", T.StringType(), True),
            ]
        )

        input2_data = [
            ("Id1", "Country1"),
            ("Id2", "Country2"),
            ("Id3", "Country3"),
        ]

        dataset["input2_df"] = Spark.get().createDataFrame(
            schema=input2_schema, data=input2_data
        )

        df_transformed = JoinDataframesTransformerNC(
            first_dataframe_join_key="Id",
            second_dataframe_join_key="_Id",
            join_type="right_outer",
            dataset_input_keys=["input1_df", "input2_df"],
        ).process_many(dataset)

        expected_data = [
            (
                "Id1",
                "Name1",
                "Id1",
                "Country1",
            ),
            (
                "Id2",
                "Name2",
                "Id2",
                "Country2",
            ),
            (None, None, "Id3", "Country3"),
        ]

        self.assertDataframeMatches(
            df=df_transformed,
            expected_data=expected_data,
        )

    def test_data_join_semi(self):
        dataset = {}

        input1_schema = T.StructType(
            [
                T.StructField("Id", T.StringType(), True),
                T.StructField("Name", T.StringType(), True),
            ]
        )

        input1_data = [
            ("Id1", "Name1"),
            ("Id2", "Name2"),
        ]
        dataset["input1_df"] = Spark.get().createDataFrame(
            schema=input1_schema, data=input1_data
        )

        input2_schema = T.StructType(
            [
                T.StructField("_Id", T.StringType(), True),
                T.StructField("Country", T.StringType(), True),
            ]
        )

        input2_data = [
            ("Id1", "Country1"),
            ("Id2", "Country2"),
            ("Id3", "Country3"),
        ]

        dataset["input2_df"] = Spark.get().createDataFrame(
            schema=input2_schema, data=input2_data
        )

        df_transformed = JoinDataframesTransformerNC(
            first_dataframe_join_key="Id",
            second_dataframe_join_key="_Id",
            join_type="outer",
            dataset_input_keys=["input1_df", "input2_df"],
        ).process_many(dataset)

        expected_data = [
            (
                "Id1",
                "Name1",
            ),
            (
                "Id2",
                "Name2",
            ),
        ]

        self.assertDataframeMatches(
            df=df_transformed,
            expected_data=expected_data,
        )

    """
    def test_data_join_anti(self):
        dataset = {}

        input1_schema = T.StructType(
            [
                T.StructField("Id", T.StringType(), True),
                T.StructField("Name", T.StringType(), True),
            ]
        )

        input1_data = [
            ("Id1", "Name1"),
            ("Id2", "Name2"),
        ]
        dataset["input1_df"] = Spark.get().createDataFrame(
            schema=input1_schema, data=input1_data
        )

        input2_schema = T.StructType(
            [
                T.StructField("_Id", T.StringType(), True),
                T.StructField("Country", T.StringType(), True),
            ]
        )

        input2_data = [
            ("Id1", "Country1"),
            ("Id2", "Country2"),
            ("Id3", "Country3"),
        ]

        dataset["input2_df"] = Spark.get().createDataFrame(
            schema=input2_schema, data=input2_data
        )

        df_transformed = JoinDataframesTransformerNC(
            first_dataframe_join_key="Id",
            second_dataframe_join_key="_Id",
            join_type="outer",
            dataset_input_keys=["input1_df", "input2_df"],
        ).process_many(dataset)

        expected_data = [
            (
                "Id1",
                "Name1",
                "Id1",
                "Country1",
            ),
            (
                "Id2",
                "Name2",
                "Id2",
                "Country2",
            ),
            (None, None, "Id3", "Country3"),
        ]

        self.assertDataframeMatches(
            df=df_transformed,
            expected_data=expected_data,
        )
        """


if __name__ == "__main__":
    unittest.main()
