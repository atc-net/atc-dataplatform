from typing import List, Optional

from pyspark.sql import DataFrame
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery

from atc.configurator.configurator import Configurator
from atc.functions import get_unique_tempview_name, init_dbutils
from atc.spark import Spark
from atc.tables import TableHandle
from atc.utils import GetMergeStatement
from atc.utils.CheckDfMerge import CheckDfMerge


class AutoLoaderHandle(TableHandle):
    def __init__(
        self,
        *,
        name: str,
        checkpoint_path: str,
        location: str = None,
        data_format: str = "delta",
        # trigger_type ?
    ):
        self._name = name
        self._location = location
        self._data_format = data_format
        self._checkpoint_path = checkpoint_path
        # Initialize Delta Handle
        self._partitioning: Optional[List[str]] = None

        _ = self.get_partitioning()

        # do we need delta valication?
        # self._validate()

    @classmethod
    def from_tc(cls, id: str) -> "AutoLoaderHandle":
        tc = Configurator()
        return cls(
            name=tc.table_property(id, "name", ""),
            location=tc.table_property(id, "path", ""),
            data_format=tc.table_property(id, "format", "delta"),
            checkpoint_path=tc.table_property(id, "checkpoint_path", ""),
        )

    def read(self) -> DataFrame:

        reader = (
            Spark.get()
            .readStream.format("cloudFiles")
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
        NotImplementedError()

    def overwrite(self, df: DataFrame, mergeSchema: bool = None) -> StreamingQuery:

        writer = (
            df.writeStream.option("checkpointLocation", self._checkpoint_path)
            .outputMode("complete")
            .trigger(availableNow=True)
        )

        return self._add_write_options(writer, mergeSchema)

    def append(self, df: DataFrame, mergeSchema: bool = None) -> StreamingQuery:
        writer = (
            df.writeStream.option("checkpointLocation", self._checkpoint_path)
            .outputMode("append")
            .trigger(availableNow=True)
        )

        return self._add_write_options(writer, mergeSchema)

    # Truncate checkpoints too
    def truncate(self) -> None:
        Spark.get().sql(f"TRUNCATE TABLE {self._name};")
        init_dbutils().fs.rm(self._checkpoint_path, True)

    # What about check points?
    def drop(self) -> None:
        Spark.get().sql(f"DROP TABLE IF EXISTS {self._name};")
        init_dbutils().fs.rm(self._checkpoint_path, True)

    # What about check points?
    def drop_and_delete(self) -> None:
        self.drop()
        if self._location:
            init_dbutils().fs.rm(self._location, True)

    def upsert(
        self,
        df: DataFrame,
        join_cols: List[str],
    ) -> StreamingQuery:

        df_target = self.read()

        # If the target is empty, always do faster full load
        if len(df_target.take(1)) == 0:
            return self.overwrite(df)

        # Find records that need to be updated in the target (happens seldom)

        # Define the column to be used for checking for new rows
        # Checking the null-ness of one right row is sufficient to mark the row as new,
        # since null keys are disallowed.

        df, merge_required = CheckDfMerge(
            df=df,
            df_target=df_target,
            join_cols=join_cols,
            avoid_cols=[],
        )

        if not merge_required:
            return self.append(df)

        target_table_name = self.get_tablename()
        non_join_cols = [col for col in df.columns if col not in join_cols]
        # temp_view_name = get_unique_tempview_name()
        # df.createOrReplaceTempView(temp_view_name)

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
        # .awaitTermination() ?

        return writer

    def _add_write_options(self, writer: DataStreamWriter, mergeSchema: bool):

        if self._partitioning:
            writer = writer.partitionBy(self._partitioning)

        if mergeSchema is not None:
            writer = writer.option("mergeSchema", "true" if mergeSchema else "false")

        if self._location:
            writer = writer.start(self._location)
        else:
            writer = writer.toTable(self._name)

        # What about this??
        # .option("path", f"{DA.paths.user_db}/heart_rate_silver.delta")

        return writer

    def create_hive_table(self) -> None:
        sql = f"CREATE TABLE IF NOT EXISTS {self._name} "
        if self._location:
            sql += f" USING DELTA LOCATION '{self._location}'"
        Spark.get().sql(sql)

    def recreate_hive_table(self):
        self.drop()
        self.create_hive_table()

    def get_partitioning(self):
        """The result of DESCRIBE TABLE tablename is like this:
        +-----------------+---------------+-------+
        |         col_name|      data_type|comment|
        +-----------------+---------------+-------+
        |           mycolA|         string|       |
        |           myColB|            int|       |
        |                 |               |       |
        |   # Partitioning|               |       |
        |           Part 0|         mycolA|       |
        +-----------------+---------------+-------+
        but this method return the partitioning in the form ['mycolA'],
        if there is no partitioning, an empty list is returned.
        """
        if self._partitioning is None:
            # create an iterator object and use it in two steps
            rows_iter = iter(
                Spark.get().sql(f"DESCRIBE TABLE {self.get_tablename()}").collect()
            )

            # roll over the iterator until you see the title line
            for row in rows_iter:
                # discard rows until the important section header
                if row.col_name.strip() == "# Partitioning":
                    break
            # at this point, the iterator has moved past the section heading
            # leaving only the rows with "Part 1" etc.

            # create a list from the rest of the iterator like [(0,colA), (1,colB)]
            parts = [
                (int(row.col_name[5:]), row.data_type)
                for row in rows_iter
                if row.col_name.startswith("Part ")
            ]
            # sort, just in case the parts were out of order.
            parts.sort()

            # discard the index and put into an ordered list.
            self._partitioning = [p[1] for p in parts]
        return self._partitioning


class UpsertHelper:
    def __init__(self, query: str, update_temp: str = "stream_updates"):
        self.query = query
        self.update_temp = update_temp

    def upsert_to_delta(self, micro_batch_df, batch):
        micro_batch_df.createOrReplaceTempView(self.update_temp)
        micro_batch_df._jdf.sparkSession().sql(self.query)
