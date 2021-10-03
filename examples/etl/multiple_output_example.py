import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from atc.etl import Extractor, Transformer, Loader, OrchestratorFactory
from atc.spark import Spark


class GuitarExtractor(Extractor):
    def read(self) -> DataFrame:
        return Spark.get().createDataFrame(
            Spark.get().sparkContext.parallelize([
                ('1', 'Fender', 'Telecaster', '1950'),
                ('2', 'Gibson', 'Les Paul', '1959'),
                ('3', 'Ibanez', 'RG', '1987')
            ]),
            StructType([
                StructField('id', StringType()),
                StructField('brand', StringType()),
                StructField('model', StringType()),
                StructField('year', StringType()),
            ]))


class BasicTransformer(Transformer):
    def process(self, df: DataFrame) -> DataFrame:
        print('Current DataFrame schema')
        df.printSchema()

        df = df.withColumn('id', f.col('id').cast(IntegerType()))
        df = df.withColumn('year', f.col('year').cast(IntegerType()))

        print('New DataFrame schema')
        df.printSchema()
        return df


class NoopSilverLoader(Loader):
    def save(self, df: DataFrame) -> DataFrame:
        df.write.format('noop').mode('overwrite').save()
        return df


class NoopGoldLoader(Loader):
    def save(self, df: DataFrame) -> DataFrame:
        df.write.format('noop').mode('overwrite').save()
        return df


print('ETL Orchestrator using multiple loaders')
etl = OrchestratorFactory.create_for_multiple_destinations(GuitarExtractor(),
                                                           BasicTransformer(),
                                                           [NoopSilverLoader(), NoopGoldLoader()])
result = etl.execute()
result.printSchema()
result.show()
