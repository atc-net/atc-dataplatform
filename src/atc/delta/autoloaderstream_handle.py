from pyspark.sql import DataFrame

from atc.configurator.configurator import Configurator
from atc.spark import Spark
from atc.tables import TableHandle
from atc.tables.SparkHandle import DeltaHandleInvalidFormat


class AutoloaderStreamHandle(TableHandle):
    def __init__(
        self,
        *,
        location: str,
        checkpoint_path: str,
        data_format: str,
    ):
        """
        location: the location of the delta table

        checkpoint_path: The location of the checkpoints, <table_name>/_checkpoints
            The Delta Lake VACUUM function removes all files not managed by Delta Lake
            but skips any directories that begin with _. You can safely store
            checkpoints alongside other data and metadata for a Delta table
            using a directory structure such as <table_name>/_checkpoints
            See: https://docs.databricks.com/structured-streaming/delta-lake.html

        data_format: the data format of the files that are read

        """

        self._location = location
        self._data_format = data_format
        self._checkpoint_path = checkpoint_path

        self._validate()

    @classmethod
    def from_tc(cls, id: str) -> "AutoloaderStreamHandle":
        tc = Configurator()
        return cls(
            location=tc.table_property(id, "path", None),
            data_format=tc.table_property(id, "format", None),
            checkpoint_path=tc.table_property(id, "checkpoint_path", None),
        )

    def _validate(self):
        """Validates that the name is either db.table or just table."""
        if self._data_format == "delta":
            raise DeltaHandleInvalidFormat("Use DeltaStreamHandle for delta.")

    def read(self) -> DataFrame:

        reader = (
            Spark.get()
            .readStream.format("cloudFiles")
            .option("cloudFiles.format", self._data_format)
            .option("cloudFiles.schemaLocation", self._checkpoint_path)
            .load(self._location)
        )

        return reader
