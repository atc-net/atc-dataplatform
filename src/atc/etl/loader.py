from abc import abstractmethod

from pyspark.sql import DataFrame

from .types import dataset_group
from .etl import EtlBase


class Loader(EtlBase):
    """
    A loader is a data sink, which is indicated by the fact that
    the save method has no return.

    In regards to the etl step, a loader can be used in two ways.
    If no dataset keys are given, it USES the input dataset(s)
    and does not consume or change it.
    If one or several dataset keys are given, those are CONSUMED
    and passed on to the save or save_many methods, while the rest
    are passed on
    """

    def __init__(self, *dataset_keys: str):
        super().__init__()
        self.dataset_keys = dataset_keys

    def etl(self, inputs: dataset_group) -> dataset_group:
        if not self.dataset_keys:
            if len(inputs) == 1:
                df = next(iter(inputs.values()))
                self.save(df)
            else:
                self.save_many(inputs)
        else:
            if len(self.dataset_keys) == 1:
                self.save(inputs.pop(self.dataset_keys[0]))
            else:
                self.save_many({key: inputs[key] for key in self.dataset_keys})
                for key in self.dataset_keys:
                    del inputs[key]
        return inputs

    @abstractmethod
    def save(self, df: DataFrame) -> None:
        raise NotImplementedError()

    @abstractmethod
    def save_many(self, datasets: dataset_group) -> None:
        raise NotImplementedError()
