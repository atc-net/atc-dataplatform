from abc import abstractmethod

from atc.etl.types import dataset_group


class EtlBase:
    @abstractmethod
    def etl(self, inputs: dataset_group) -> dataset_group:
        raise NotImplementedError()
