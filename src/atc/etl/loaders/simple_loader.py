from typing import Protocol, Union

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from atc.etl import Loader


class Overwritable(Protocol):
    def overwrite(self, df: DataFrame) -> Union[None, StreamingQuery]:
        pass


class Appendable(Protocol):
    def append(self, df: DataFrame) -> None:
        pass


class SimpleLoader(Loader):
    def __init__(
        self, handle: Union[Overwritable, Appendable], *, mode: str = "overwrite"
    ):
        super().__init__()
        self.mode = mode
        self.handle = handle

    def save(self, df: DataFrame) -> None:
        if self.mode == "overwrite":
            self.handle.overwrite(df)
        else:
            self.handle.append(df)
