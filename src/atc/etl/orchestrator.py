from typing import List, Dict, Any

from pyspark.sql import DataFrame

from .types import EtlBase, dataset_group


class Orchestrator(EtlBase):
    """
    It is up to the user of this library that extractors,
    transformers and loaders live up to their names and are not
    used in a wrong order.
    """

    def __init__(self, args: Dict[str, Any] = None):
        super().__init__()
        self.steps: List[EtlBase] = []
        if args is None:
            args = {}
        self.args = args

    def step(self, etl: EtlBase) -> "Orchestrator":
        self.steps.append(etl)
        etl.set_orchestrator_args(self.args)
        return self

    def set_orchestrator_args(self, args: Dict[str, Any]) -> None:
        self.args = args
        for etl in self.steps:
            etl.set_orchestrator_args(args)

    # these are just synonyms for readability
    extract_from = step
    transform_with = step
    load_into = step

    def execute(self) -> DataFrame:
        datasets = self.etl({})

        if len(datasets) == 1:
            return next(iter(datasets.values()))
        raise AssertionError("Multiple datasets in play at the end of orchestration.")

    def etl(self, datasets: dataset_group) -> dataset_group:
        for step in self.steps:
            datasets = step.etl(datasets)
        return datasets
