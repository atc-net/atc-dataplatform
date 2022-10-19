from typing import Dict, List, Union

from pyspark.sql import DataFrame, types
from pyspark.sql.types import StructType
from pyspark.sql.utils import AnalysisException

from atc.atc_exceptions import AtcException
from atc.config_master import TableConfigurator
from atc.functions import get_unique_tempview_name, init_dbutils
from atc.spark import Spark
from atc.tables.TableHandle import TableHandle
from atc.utils.CheckDfMerge import CheckDfMerge
from atc.utils.GetMergeStatement import GetMergeStatement


class DeltaHandleException(AtcException):
    pass


class DeltaHandleInvalidName(DeltaHandleException):
    pass


class DeltaHandleInvalidFormat(DeltaHandleException):
    pass


class DeltaHandleInvalidSchema(DeltaHandleException):
    pass


class DeltaHandle(TableHandle):
    def __init__(
        self,
        name: str,
        location: str = None,
        data_format: str = "delta",
        schema: StructType = None,
        partitioning: List[str] = None,
        tblproperties: Dict[str, str] = None,
    ):
        self._name = name
        self._location = location
        self._data_format = data_format

        self._schema = schema
        self._partitioning = partitioning
        self._tblproperties = tblproperties

        self._validate()

    @classmethod
    def from_tc(cls, id: str) -> "DeltaHandle":
        tc = TableConfigurator()
        schema_str = tc.table_property(id, "schema", "")

        return cls(
            name=tc.table_property(id, "name", ""),
            location=tc.table_property(id, "path", ""),
            data_format=tc.table_property(id, "format", "delta"),
            schema=types._parse_datatype_string(schema_str) if schema_str else None,
            tblproperties=tc.table_property(id, "tblproperties", {}),
        )

    def _validate(self):
        """Validates that the name is either db.table or just table."""
        if not self._name:
            if not self._location:
                raise DeltaHandleInvalidName(
                    "Cannot create DeltaHandle without name or path"
                )
            self._name = f"delta.`{self._location}`"
        else:
            name_parts = self._name.split(".")
            if len(name_parts) == 1:
                self._db = None
                self._table_name = name_parts[0]
            elif len(name_parts) == 2:
                self._db = name_parts[0]
                self._table_name = name_parts[1]
            else:
                raise DeltaHandleInvalidName(f"Could not parse name {self._name}")

        # only format delta is supported.
        if self._data_format != "delta":
            raise DeltaHandleInvalidFormat("Only format delta is supported.")

    def read(self) -> DataFrame:
        """Read table by path if location is given, otherwise from name."""
        if self._location:
            return Spark.get().read.format(self._data_format).load(self._location)
        return Spark.get().table(self._name)

    def get_schema(self):
        if not self._schema:
            self._schema = self.read().schema
        return self._schema

    def get_partitioning(self):
        if self._partitioning is None:
            rows_iter = iter(
                Spark.get().sql(f"DESCRIBE TABLE {self.get_tablename()}").collect()
            )
            for row in rows_iter:
                if row.col_name.strip() == "# Partitioning":
                    break
            parts = [
                row.data_type for row in rows_iter if row.col_name.startswith("Part ")
            ]
            self._partitioning = parts
        return self._partitioning

    def write_or_append(
        self, df: DataFrame, mode: str, mergeSchema: bool = None
    ) -> None:
        assert mode in {"append", "overwrite"}

        writer = df.write.format(self._data_format).mode(mode)
        if mergeSchema is not None:
            writer = writer.option("mergeSchema", "true" if mergeSchema else "false")

        if self._location:
            return writer.save(self._location)

        return writer.saveAsTable(self._name)

    def overwrite(self, df: DataFrame, mergeSchema: bool = None) -> None:
        return self.write_or_append(df, "overwrite", mergeSchema=mergeSchema)

    def append(self, df: DataFrame, mergeSchema: bool = None) -> None:
        return self.write_or_append(df, "append", mergeSchema=mergeSchema)

    def truncate(self) -> None:
        Spark.get().sql(f"TRUNCATE TABLE {self._name};")

    def drop(self) -> None:
        Spark.get().sql(f"DROP TABLE IF EXISTS {self._name};")

    def drop_and_delete(self) -> None:
        self.drop()
        if self._location:
            init_dbutils().fs.rm(self._location, True)

    def create_hive_table(self) -> None:
        if not self._name:
            raise DeltaHandleInvalidName("Cannot create Hive table without name.")

        spark = Spark.get()
        # does table exist?
        try:
            spark.table(self._name)
            return  # Table exists. Done.
        except AnalysisException:
            # table does not exist
            pass

        # to create a table, we need to have one of two conditions
        # - either there is already data that we can read
        # - or we have been give a schema so that we can create it

        # simply try creating the table. If this succeeds we are done
        if self._location:
            try:
                spark.sql(
                    f"CREATE TABLE {self._name} "
                    "USING DELTA "
                    f"LOCATION '{self._location}'"
                )

                return self.apply_tblproperties()
            except AnalysisException:
                pass
            # ok, it wasn't that easy

        if not self._schema:
            raise DeltaHandleInvalidSchema(
                "To create a table we need existing data or a schema."
            )

        # we now know that we have a schema (and maybe a location) and there is no data
        (
            # create and empty dataframe of the correct schema
            spark.createDataFrame([], self._schema)
            # and write it as the table
            .write.saveAsTable(
                self._name,
                format=self._data_format,
                # using specified partitioning
                partitionBy=self._partitioning,
                # using specified location
                path=self._location,
            )
        )
        return self.apply_tblproperties()

    def ensure_read_success(self) -> None:
        """Make sure that the necessary data exist
        in storage so that a .read() will not fail."""
        try:
            self.read()
            return
        except AnalysisException:
            pass

        # read failed. There is no data, yet.
        if not self._schema:
            raise DeltaHandleInvalidSchema("To create data, we need a schema.")
        writer = Spark.get().createDataFrame([], self._schema).write
        if self._partitioning:
            writer = writer.partitionBy(self._partitioning)
        if self._location:
            writer.save(self._location)
        else:
            writer.saveAsTable(self._name)

    def recreate_hive_table(self):
        self.get_schema()  # preserve a schema, if any
        self.get_partitioning()  # preserve partitioning, if any
        self.drop()
        self.create_hive_table()

    def delete_and_recreate(self):
        self.get_schema()  # preserve a schema, if any
        self.get_partitioning()  # preserve partitioning, if any
        self.drop_and_delete()
        self.create_hive_table()

    def get_tblproperties(self) -> Dict[str, str]:
        if not self._tblproperties:
            self._tblproperties = {}
            details = {
                row.col_name: row.data_type
                for row in Spark.get()
                .sql(f"DESCRIBE TABLE EXTENDED {self._name}")
                .collect()
            }["Table Properties"]
            for property in details.strip("[]").split(","):
                key, value = property.split("=")
                self._tblproperties[key] = value
        return self._tblproperties

    def apply_tblproperties(self) -> None:
        if not self._tblproperties:
            return
        TBLPRPTS = [f"{repr(key)}={repr(value)}" for key, value in self._tblproperties]

        Spark.get().sql(
            f"ALTER TABLE {self._name} SET TBLPROPERTIES ({','.join(TBLPRPTS)});"
        )

    def get_tablename(self) -> str:
        return self._name

    def upsert(
        self,
        df: DataFrame,
        join_cols: List[str],
    ) -> Union[DataFrame, None]:

        if df is None:
            return None

        df = df.filter(" AND ".join(f"({col} is NOT NULL)" for col in join_cols))
        print(
            "Rows with NULL join keys found in input dataframe"
            " will be discarded before load."
        )

        df_target = self.read()

        # If the target is empty, always do faster full load
        if len(df_target.take(1)) == 0:
            return self.write_or_append(df, mode="overwrite")

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
            return self.write_or_append(df, mode="append")

        temp_view_name = get_unique_tempview_name()
        df.createOrReplaceTempView(temp_view_name)

        target_table_name = self.get_tablename()
        non_join_cols = [col for col in df.columns if col not in join_cols]

        merge_sql_statement = GetMergeStatement(
            merge_statement_type="delta",
            target_table_name=target_table_name,
            source_table_name=temp_view_name,
            join_cols=join_cols,
            insert_cols=df.columns,
            update_cols=non_join_cols,
            special_update_set="",
        )

        Spark.get().sql(merge_sql_statement)

        print("Incremental Base - incremental load with merge")

        return df
