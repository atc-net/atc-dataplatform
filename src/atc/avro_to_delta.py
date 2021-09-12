"""
This class takes a path to avro files and updates a delta table accordingly.
The path is where avro files are dumped by EventHub according to the pattern:
/y=YYYY/m=MM/d=DD[/h=HH]/YYYY_MM_DD_HH_mm_ss_P.avro
where P represents partition number and the other values represent standard time format values.

Beyond the columns of the given schema, the target delta table will also have a column for the message time from
event hub. This field is by default called "EnqueuedTime", but this can be overridden
"""
from datetime import timedelta
from typing import List

from py4j.protocol import Py4JJavaError
from pyspark.sql.types import StructType
import pyspark.sql.functions as F
import Spark


class EventHubAvroJsonToDelta:
    def __init__(
        self,
        source_path: str,
        target_table_name: str,
        schema: StructType,
        primary_keys: List[str],
        enqueued_time_column: str = "EnqueuedTime",
        partition_overlap: timedelta = timedelta(minutes=15),
    ):
        """
        :param source_path: the root path where avro files are stored
        :param target_table_name: the delta table where
        :param schema: the schema with which to unpack the json payload
        :param primary_keys: The primary key column names in the target table.
        """
        self.source_path = source_path
        self.target_table_name = target_table_name
        self.schema = schema
        self.primary_keys = primary_keys
        self.enqueued_time_column = enqueued_time_column
        self.partition_overlap = partition_overlap

    def extract(self):
        # find out where we read to last time:
        target = Spark.get().table(self.target_table_name)
        try:
            max_time = (
                target.groupBy().agg(F.max(self.enqueued_time_column)).collect()[0][0]
            )
        except Py4JJavaError:
            max_time = None
