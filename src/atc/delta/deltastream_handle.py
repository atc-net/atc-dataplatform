from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.streaming import DataStreamWriter

from atc.configurator.configurator import Configurator
from atc.functions import init_dbutils
from atc.spark import Spark
from atc.tables.SparkHandle import SparkHandle
from atc.utils import GetMergeStatement
from atc.utils.FileExists import file_exists


class DeltaStreamHandle(SparkHandle):
    def __init__(
        self,
        *,
        checkpoint_path: str,
        name: str = None,
        data_format: str = None,
        location: str = None,
        trigger_type: str = "availablenow",
        trigger_time: str = None,
        await_termination=False,
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

        trigger_type: the trigger type of the stream.
            See: https://docs.databricks.com/structured-streaming/triggers.html

        trigger_time: if the trigger has is "processingtime",
            it should have a trigger time associated

        awaitTermination: if true, the ETL will wait for the termination of THIS query.

        """
        assert (
            Spark.version() >= Spark.DATABRICKS_RUNTIME_10_4
        ), f"DeltaStreamHandle not available for Spark version {Spark.version()}"

        super().__init__(name, location, data_format)

        self._checkpoint_path = checkpoint_path
        self._trigger_type = trigger_type.lower() if trigger_type else "availablenow"
        self._trigger_time = trigger_time.lower() if trigger_time else None
        self._awaitTermination = await_termination if await_termination else True
        self._validate()
        self._validate_trigger_type()
        self._validate_checkpoint()

    @classmethod
    def from_tc(cls, id: str) -> "DeltaStreamHandle":
        tc = Configurator()
        return cls(
            name=tc.table_property(id, "name", ""),
            location=tc.table_property(id, "path", ""),
            data_format=tc.table_property(id, "format", None),
            checkpoint_path=tc.table_property(id, "checkpoint_path", None),
            trigger_type=tc.table_property(id, "trigger_type", None),
            trigger_time=tc.table_property(id, "trigger_time", None),
            await_termination=tc.table_property(id, "await_termination", None),
        )

    def _validate_trigger_type(self):
        valid_trigger_types = {"availablenow", "once", "processingtime", "continuous"}
        assert (
            self._trigger_type in valid_trigger_types
        ), f"Triggertype should either be {valid_trigger_types}"

        # if trigger type is processingtime, then it should have a trigger time
        assert (self._trigger_type == "processingtime") is (
            self._trigger_time is not None
        )

    def _validate_checkpoint(self):
        if "/_" not in self._checkpoint_path:
            print(
                "RECOMMENDATION: You can safely store checkpoints alongside "
                "other data and metadata for a Delta table using a directory "
                "structure such as <table_name>/_checkpoints"
            )

    def read(self) -> DataFrame:

        reader = Spark.get().readStream.format("delta")

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

        if mode == "overwrite":
            print("WARNING: The term overwrite is called complete in streaming.")
            mode = "complete"

        writer = df.writeStream.option(
            "checkpointLocation", self._checkpoint_path
        ).outputMode(mode)

        writer = self._add_trigger_type(writer)

        writer = self._add_write_options(writer, mergeSchema)

        if self._awaitTermination:
            writer.awaitTermination()

    def overwrite(self, df: DataFrame, mergeSchema: bool = None) -> None:
        return self.write_or_append(df, "complete", mergeSchema)

    def append(self, df: DataFrame, mergeSchema: bool = None) -> None:
        return self.write_or_append(df, "append", mergeSchema)

    def truncate(self) -> None:
        Spark.get().sql(f"TRUNCATE TABLE {self._name};")

        self.remove_checkpoint()

    def drop(self) -> None:
        Spark.get().sql(f"DROP TABLE IF EXISTS {self._name};")

        self.remove_checkpoint()

    def drop_and_delete(self) -> None:
        self.drop()
        if self._location:
            init_dbutils().fs.rm(self._location, True)

    def upsert(self, df: DataFrame, join_cols: List[str]) -> None:
        assert df.isStreaming

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
        )

        writer = self._add_trigger_type(writer)

        if self._awaitTermination:
            writer.start().awaitTermination()  # Consider removing awaitTermination
        else:
            writer.start()

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

    def _add_trigger_type(self, writer: DataStreamWriter):

        if self._trigger_type == "availablenow":
            return writer.trigger(availableNow=True)
        elif self._trigger_type == "once":
            return writer.trigger(once=True)
        elif self._trigger_type == "processingtime":
            return writer.trigger(processingTime=self._trigger_time)
        elif self._trigger_type == "continuous":
            return writer.trigger(continuous=self._trigger_time)
        else:
            raise ValueError("Unknown trigger type.")

    def create_hive_table(self) -> None:
        self.remove_checkpoint()

        sql = f"CREATE TABLE IF NOT EXISTS {self._name} "
        if self._location:
            sql += f" USING DELTA LOCATION '{self._location}'"
        Spark.get().sql(sql)

    def remove_checkpoint(self):
        if not file_exists(self._checkpoint_path):
            init_dbutils().fs.mkdirs(self._checkpoint_path)


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
