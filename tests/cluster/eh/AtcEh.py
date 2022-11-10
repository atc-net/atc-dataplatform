"""
This file sets up the EventHub that is deployed as part of the atc integration pipeline
"""
from atc.eh import EventHubStream
from tests.cluster.secrets import eventHubConnection


class AtcEh(EventHubStream):
    def __init__(self):
        super().__init__(
            connection_str=eventHubConnection(),
            entity_path="atceh",
            consumer_group="$Default",
        )
