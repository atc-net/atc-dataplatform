from pyspark.sql import DataFrame

from atc.delta.autoloader_handle import AutoLoaderHandle
from atc.etl import Loader, dataset_group


class StreamLoader(Loader):
    """
    This class is maybe needed for executing foreachbatch on multiple batch loaders.

    """

    def __init__(self, stream_handle: AutoLoaderHandle):
        super().__init__()

        self.handle = stream_handle

    def save_many(self, datasets: dataset_group) -> None:
        raise NotImplementedError()

    def save(self, df: DataFrame) -> None:
        """Unknown for know"""
        # self.handle.upsert(df=df, join_cols=self.join_cols)
