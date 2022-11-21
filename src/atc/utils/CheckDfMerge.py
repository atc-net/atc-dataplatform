from pyspark.sql import DataFrame


def CheckDfMerge(
    *,
    df: DataFrame,
    df_target: DataFrame,
):
    """This logic optimizes data load of dataframe, df, into a target table, df_target.
    it checks whether a merge is needed.
    If it is not needed, a simple insert is executed.

    The join_type left-anti returns values from the left relation that has no match with the right.
    Utilizing left anti makes it possible to find out if merge is needed.

    See: https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-join.html#anti-join
    """

    all_cols = df.columns

    df = (
        df.alias("a")
        .join(
            df_target.alias("b"),
            on=all_cols,
            how="left_anti",
        )
        .cache()
    )

    # merge_required == True if the data contains updates or deletes,
    # False if only inserts
    # note: inserts alone happen almost always and can use "append",
    # which is much faster than merge
    merge_required = len(df.take(1)) > 0

    return df, merge_required
