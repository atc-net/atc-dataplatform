from abc import abstractmethod
from typing import Dict, Any, Optional

from pyspark.sql import DataFrame

dataset_group = Dict[str, DataFrame]


class EtlBase:
    orchestrator_args: Optional[Dict[str, Any]] = None

    @abstractmethod
    def etl(self, inputs: dataset_group) -> dataset_group:
        pass

    def set_orchestrator_args(self, args: Dict[str, Any]) -> None:
        self.orchestrator_args = args

    def get_arg(self, key: str) -> Any:
        if self.orchestrator_args is None:
            raise KeyError(f"Orchestrator args not set, unable to get {key}")
        return self.orchestrator_args[key]

    def set_arg(self, key: str, value: Any) -> None:
        if self.orchestrator_args is None:
            raise KeyError(f"Orchestrator args not set, unable to set {key}")
        self.orchestrator_args[key] = value
