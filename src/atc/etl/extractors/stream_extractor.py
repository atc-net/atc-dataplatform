from typing import Protocol

from pyspark.sql import DataFrame

from atc.etl import Extractor
from atc.tables import TableHandle


class StreamExtractor(Extractor):
    """This extractor will extract from any object that has a .read() method."""

    def __init__(self, handle: TableHandle, dataset_key: str):
        super().__init__(dataset_key=dataset_key)
        self.handle = handle

    def read(self) -> DataFrame:
        return self.handle.read_stream()
