from abc import abstractmethod
from typing import List, Union

from pyspark.sql import DataFrame

from atc.etl.types import dataset_group

from .transformer import Transformer


class TransformerNC(Transformer):
    """If you only want to transform a single input dataframe,
    implement `process`
    If you want to transform a set of dataframes,
    implement `process_many`

    In regards to the etl step, the TransformerNC does NOT CONSUME the inputs
    and ADDs the result of its transformation stage to the dataset dict.
    """

    def __init__(
        self,
        *,
        dataset_input_keys: Union[str, List[str]] = None,
        dataset_output_key: str = None,
    ):
        super().__init__(
            dataset_input_keys=dataset_input_keys,
            dataset_output_key=dataset_output_key,
            consume_inputs=False,
        )

    @abstractmethod
    def process(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError()

    @abstractmethod
    def process_many(self, datasets: dataset_group) -> DataFrame:
        raise NotImplementedError()
