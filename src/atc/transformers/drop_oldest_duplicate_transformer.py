from typing import List, Union

from pyspark.sql import DataFrame

from atc.etl import Transformer
from atc.utils import DropOldestDuplicates


class DropOldestDuplicatesTransformer(Transformer):
    """
    This transformer drops the oldest duplicates based on some ordering.

    Attributes:
    ----------
        cols : List[str]
            List of columns to use for deduplication
        orderByColumn : str
            Column used for ordering
        dataset_input_keys : Union[str, List[str]]
            list of input dataset keys
        dataset_output_key : str
            output dataset key
        consume_inputs : bool
            Flag to control dataset consuming behavior
    """

    def __init__(
        self,
        *,
        cols: List[str],
        orderByColumn: str,
        dataset_input_keys: Union[str, List[str]] = None,
        dataset_output_key: str = None,
        consume_inputs: bool = True,
    ):
        super().__init__(
            dataset_input_keys=dataset_input_keys,
            dataset_output_key=dataset_output_key,
            consume_inputs=consume_inputs,
        )

        self.cols = cols
        self.orderByColumn = orderByColumn

    def process(self, df: DataFrame) -> DataFrame:

        return DropOldestDuplicates(
            df=df, cols=self.cols, orderByColumn=self.orderByColumn
        )
