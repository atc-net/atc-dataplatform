from typing import List, Optional

from atc.spark import Spark
from atc.tables import TableHandle


class SparkHandle(TableHandle):
    """Common handle class for both DeltaHandle and StreamingHandle"""

    def __init__(self, name: str, location: str = None, data_format: str = "delta"):
        self._name = name
        self._location = location
        self._data_format = data_format

        self._partitioning: Optional[List[str]] = None

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

    def get_tablename(self) -> str:
        return self._name
