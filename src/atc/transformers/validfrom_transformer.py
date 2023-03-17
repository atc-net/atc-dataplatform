from datetime import datetime, timezone
from typing import List

import pyspark.sql.functions as f
from pyspark.sql import DataFrame, Window

from atc.etl import Transformer
from atc.utils import DropOldestDuplicates


class ValidFromTransformer(Transformer):
    """
    The class introduces SCD2 columns:
        ValidFrom
        ValidTo
        IsCurrent

    NB: Be aware, if incremental extraction is used, the logic does not work properly

    """

    def __init__(self, time_col: str, wnd_cols: List[str]):
        super().__init__()
        self._time_col = time_col
        self._wnd_cols = wnd_cols

    def process(self, df: DataFrame) -> DataFrame:
        df = df.withColumn("ValidFrom", f.col(self._time_col))

        # Drop duplicates based on the window cols and time col
        # If there is duplicates,
        # the ValidFromTransformer could introduce multiple is_current
        df = DropOldestDuplicates(
            df=df,
            cols=self._wnd_cols + [self._time_col],
            orderByColumn=self._time_col,
        )

        max_time = datetime(9999, 1, 1, tzinfo=timezone.utc)

        return (
            df.withColumn(
                "NextValidFrom",
                f.lead("ValidFrom").over(
                    Window.partitionBy(self._wnd_cols).orderBy("ValidFrom")
                ),
            )
            .withColumn("ValidTo", f.coalesce(f.col("NextValidFrom"), f.lit(max_time)))
            .withColumn("IsCurrent", f.col("NextValidFrom").isNull())
        )
