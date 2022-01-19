import unittest
from unittest.mock import MagicMock

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from atc.etl import Loader
from atc.spark import Spark


class LoaderTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.df = create_dataframe()

    def test_save_single_anon(self):

        loader = Loader()
        loader.save = MagicMock()

        input = {"myset": self.df}
        result = loader.etl(input)

        # assert that loader returns something:
        self.assertIs(input, result)

        args = loader.save.call_args[0]
        # the .args part became available in python 3.8

        print(args)
        self.assertEqual(len(args), 1)
        self.assertIs(args[0], self.df)

    def test_save_many_anon(self):

        loader = Loader()
        loader.save_many = MagicMock()

        loader.etl({"myset1": self.df, "myset2": self.df})

        args = loader.save_many.call_args[0]
        # the .args part became available in python 3.8

        print(args)
        self.assertEqual(len(args), 1)
        datasets = args[0]
        self.assertEqual(len(datasets), 2)
        self.assertEqual(set(datasets.keys()), {"myset1", "myset2"})

    def test_save_single_consuming(self):

        loader = Loader("myset")
        loader.save = MagicMock()

        input = {"myset": self.df}
        result = loader.etl(input)

        # assert that loader returns nothing:
        self.assertEqual({}, result)

        args = loader.save.call_args[0]
        # the .args part became available in python 3.8

        print(args)
        self.assertEqual(len(args), 1)
        self.assertIs(args[0], self.df)

    def test_save_many_consuming(self):

        loader = Loader("myset1", "myset2")
        loader.save_many = MagicMock()

        result = loader.etl({"myset1": self.df, "myset2": self.df, "other": "dummy"})

        # assert that loader consumes its keys:
        self.assertEqual({"other": "dummy"}, result)

        args = loader.save_many.call_args[0]
        # the .args part became available in python 3.8

        print(args)
        self.assertEqual(len(args), 1)
        datasets = args[0]
        self.assertEqual(len(datasets), 2)
        self.assertEqual(set(datasets.keys()), {"myset1", "myset2"})


def create_dataframe():
    return Spark.get().createDataFrame(
        Spark.get().sparkContext.parallelize([(1, "1"), (2, "2"), (3, "3")]),
        StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("text", StringType(), False),
            ]
        ),
    )


if __name__ == "__main__":
    unittest.main()
