import logging

from opencensus.ext.azure.log_exporter import AzureLogHandler

from atc.insights.job_reflection import get_run_id, get_run_page_url


class InsightsLogHandler(AzureLogHandler):
    def __init__(self, instrumentation_key: str = None):
        if instrumentation_key is None:
            super().__init__()
        else:
            super().__init__(
                connection_string=f"InstrumentationKey={instrumentation_key}"
            )

    def emit(self, record: logging.LogRecord):
        # enrich the record with information about the job
        record.runId = get_run_id()
        record.runPageUrl = get_run_page_url()

        super().emit(record)
