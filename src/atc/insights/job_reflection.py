import json
from functools import lru_cache

import atc.db_auto
from atc.atc_exceptions import AtcException
from atc.functions import init_dbutils


class UnableToDetermineRunId(AtcException):
    pass


class RestApiException(AtcException):
    pass


@lru_cache
def get_run_page_url() -> str:

    api = atc.db_auto.getDbApi()
    run_details = api.jobs.get_run(get_run_id())

    if not "run_page_url":
        raise RestApiException("Databricks API returned unexpected json document.")

    return run_details["run_page_url"]


@lru_cache
def get_run_id() -> int:
    # I have tested the following code in notebook- and in python-task jobs
    # this page mentions the access
    #
    # https://community.databricks.com/s/question
    # /0D53f00001HKHkhCAH
    # /is-it-possible-to-get-job-run-id-of-notebook-run-by-dbutilsnotbookrun
    #
    # but it says that this is an unofficial api and may change.
    # there is no more official way to get the currently executing run id afaik.
    details = json.loads(
        init_dbutils()
        .notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .toJson()
    )
    if "tags" not in details:
        raise UnableToDetermineRunId()
    tags = details["tags"]
    if "runId" not in tags:
        raise UnableToDetermineRunId()
    return int(tags["runId"])
