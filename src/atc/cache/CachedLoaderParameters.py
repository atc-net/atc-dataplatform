# Defines input parameters for a cached Loader

from typing import List


class CachedLoaderParameters:
    def __init__(
        self,
        cache_table_name: str,
        key_cols: List[str],
        refresh_ttl: float = -1,
        refresh_row_limit: int = -1,
        max_ttl: float = -1,
        cache_id_cols: List[str] = None,
    ):
        """
        Args:
            cache_table_name: The table that holds the cache
            key_cols: the set of columns that form the primary key for a row
            refresh_ttl: Based on the cache, if the writing of a row is this
                long ago (in seconds) it will be written again, respecting the refresh
                row limit. Set to <=0 to disable re-writing of cached rows.
            refresh_row_limit: limit for how many rows a refreshing write should be
                restricted to. Set to <=0 to disable this restriction.
            max_ttl: Based on the cache, if the writing of a row is this long ago
                (in seconds) it will be written again, regardless of the refresh limit.
                Set to <=0 to disable force-re-writing of cached rows.
            cache_id_cols: These columns, added by the write operation, will be saved
                in the cache to identify e.g. the written batch.
        """

        self.cache_id_cols = cache_id_cols or []
        self.cache_table_name = cache_table_name
        self.key_cols = key_cols
        if not key_cols:
            raise ValueError("key columns must be provided")

        self.refresh_row_limit = refresh_row_limit

        self.refresh_ttl = refresh_ttl
        self.max_ttl = max_ttl

        self.rowHash = "rowHash"
        self.loadedTime = "loadedTime"
        self.deletedTime = "deletedTime"
