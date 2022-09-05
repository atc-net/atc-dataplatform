from abc import abstractmethod
from typing import List

from pyspark.sql import DataFrame

from atc.etl.types import EtlBase, dataset_group


class ExtendedTransformer(EtlBase):
    """If you only want to transform a single input dataframe,
    implement `process`
    if you want to transform a set of dataframes,
    implement `process_many`

    In regards to the etl step, the transformer CONSUMES the inputs
    and ADDs the result of its transformation stage.
    """

    def __init__(
        self,
        dataset_output_key: str = None,
        dataset_input_key: str = None,
        dataset_input_key_list: List[str] = None,
    ):
        if dataset_output_key is None:
            dataset_output_key = type(self).__name__
        self.dataset_output_key = dataset_output_key
        self.dataset_input_key = dataset_input_key
        self.dataset_input_key_list = dataset_input_key_list

    def etl(self, inputs: dataset_group) -> dataset_group:

        if self.dataset_input_key:
            df = self.process(inputs[self.dataset_input_key])
        elif self.dataset_input_key_list:
            df = self.process_many(
                {
                    datasetKey: df
                    for datasetKey, df in inputs.items()
                    if datasetKey in self.dataset_input_key_list
                }
            )
        elif len(inputs) == 1:
            df = self.process(next(iter(inputs.values())))
        else:
            df = self.process_many(inputs)

        inputs[self.dataset_output_key] = df
        return inputs

    @abstractmethod
    def process(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError()

    @abstractmethod
    def process_many(self, datasets: dataset_group) -> DataFrame:
        raise NotImplementedError()
