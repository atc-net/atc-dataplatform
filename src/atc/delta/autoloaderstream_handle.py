from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.streaming import DataStreamWriter

from atc.configurator.configurator import Configurator
from atc.functions import init_dbutils
from atc.spark import Spark
from atc.tables.SparkHandle import SparkHandle
from atc.utils import GetMergeStatement
from atc.utils.FileExists import file_exists


class AutoloaderStreamHandle(SparkHandle):
    def __init__(
        self,
        *,
        name: str,
        checkpoint_path: str,
        location: str = None,
        data_format: str = "delta",
        # trigger_type ?
    ):
        """
        name: name of the delta table

        checkpoint_path: The location of the checkpoints, <table_name>/_checkpoints
            The Delta Lake VACUUM function removes all files not managed by Delta Lake
            but skips any directories that begin with _. You can safely store
            checkpoints alongside other data and metadata for a Delta table
            using a directory structure such as <table_name>/_checkpoints
            See: https://docs.databricks.com/structured-streaming/delta-lake.html

        location: the location of the delta table (Optional)

        data_format: the data format of the files that are read (Default delta)

        """
        super().__init__(name, location, data_format)

        self._checkpoint_path = checkpoint_path

    @classmethod
    def from_tc(cls, id: str) -> "AutoloaderStreamHandle":
        tc = Configurator()
        return cls(
            name=tc.table_property(id, "name", ""),
            location=tc.table_property(id, "path", ""),
            data_format=tc.table_property(id, "format", "delta"),
            checkpoint_path=tc.table_property(id, "checkpoint_path", ""),
        )

    def read(self) -> DataFrame:
        # https://docs.delta.io/latest/delta-streaming.html

        reader = Spark.get().readStream

        if self._data_format == "delta":
            reader = reader.format("delta")
        else:
            reader = (
                reader.format("cloudFiles")
                .option("cloudFiles.format", self._data_format)
                .option("cloudFiles.schemaLocation", self._checkpoint_path)
            )

        if self._location:
            reader = reader.load(self._location)
        else:
            reader = reader.table(self._name)

        return reader

    def write_or_append(
        self, df: DataFrame, mode: str, mergeSchema: bool = None
    ) -> None:

        assert mode in {"append", "overwrite", "complete"}
        assert df.isStreaming
        self._validate()

        if mode == "overwrite":
            mode = "complete"

        writer = (
            df.writeStream.option("checkpointLocation", self._checkpoint_path)
            .outputMode(mode)
            .trigger(availableNow=True)
        )

        writer = self._add_write_options(writer, mergeSchema)

        writer.awaitTermination()

    def overwrite(self, df: DataFrame, mergeSchema: bool = None) -> None:
        return self.write_or_append(df, "complete", mergeSchema)

    def append(self, df: DataFrame, mergeSchema: bool = None) -> None:
        return self.write_or_append(df, "append", mergeSchema)

    def truncate(self) -> None:
        self._validate()
        Spark.get().sql(f"TRUNCATE TABLE {self._name};")

        if file_exists(self._checkpoint_path):
            init_dbutils().fs.rm(self._checkpoint_path, True)

    def drop(self) -> None:
        self._validate()
        Spark.get().sql(f"DROP TABLE IF EXISTS {self._name};")

        if file_exists(self._checkpoint_path):
            init_dbutils().fs.rm(self._checkpoint_path, True)

    def drop_and_delete(self) -> None:
        self._validate()
        self.drop()
        if self._location:
            init_dbutils().fs.rm(self._location, True)

    def upsert(self, df: DataFrame, join_cols: List[str]) -> None:
        assert df.isStreaming
        self._validate()

        target_table_name = self.get_tablename()
        non_join_cols = [col for col in df.columns if col not in join_cols]

        merge_sql_statement = GetMergeStatement(
            merge_statement_type="delta",
            target_table_name=target_table_name,
            source_table_name="stream_updates",
            join_cols=join_cols,
            insert_cols=df.columns,
            update_cols=non_join_cols,
            special_update_set="",
        )

        streamingmerge = UpsertHelper(query=merge_sql_statement)

        writer = (
            df.writeStream.format("delta")
            .foreachBatch(streamingmerge.upsert_to_delta)
            .outputMode("update")
            .option("checkpointLocation", self._checkpoint_path)
            .trigger(availableNow=True)
            .start()
        )
        writer.awaitTermination()

    def _add_write_options(self, writer: DataStreamWriter, mergeSchema: bool):

        if self._partitioning:
            writer = writer.partitionBy(self._partitioning)

        if mergeSchema is not None:
            writer = writer.option("mergeSchema", "true" if mergeSchema else "false")

        if self._location:
            writer = writer.start(self._location)
        else:
            writer = writer.toTable(self._name)

        return writer

    def create_hive_table(self) -> None:
        if not file_exists(self._checkpoint_path):
            init_dbutils().fs.mkdirs(self._checkpoint_path)

        sql = f"CREATE TABLE IF NOT EXISTS {self._name} "
        if self._location:
            sql += f" USING DELTA LOCATION '{self._location}'"
        Spark.get().sql(sql)


class UpsertHelper:
    """
    In order to write upserts from a streaming query, this helper class can be used
    in the foreachBatch method.

    The class helps upserting microbatches to the target table.

    See: https://docs.databricks.com/structured-streaming/
         delta-lake.html#upsert-from-streaming-queries-using-foreachbatch
    """

    def __init__(self, query: str, update_temp: str = "stream_updates"):
        self.query = query
        self.update_temp = update_temp

    def upsert_to_delta(self, micro_batch_df, batch):
        micro_batch_df.createOrReplaceTempView(self.update_temp)
        micro_batch_df._jdf.sparkSession().sql(self.query)
