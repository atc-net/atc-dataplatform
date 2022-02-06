from typing import Any

from .DataLake import DataLake
from .EtlProxy import EtlProxy
from .NodeProxy import NodeProxy
from atc.singleton import Singleton


class EtlRegistry(metaclass=Singleton):
    def __init__(self):
        self.etls = dict()

    def get_prx(self, orc) -> NodeProxy:
        if orc not in self.etls:
            self.etls[orc] = EtlProxy(orc)
        return self.etls[orc]

    def register(self, etl: Any) -> Any:
        self.get_prx(etl)
        return etl

    def parent(self, prnt: NodeProxy):
        def attach_parent(etl):
            self.get_prx(etl).set_parent(prnt)
            return etl

        return attach_parent

    def input(self, tbl: DataLake):
        def attach_input(etl):
            self.get_prx(etl).inputs.append(tbl)
            return etl

        return attach_input

    def output(self, tbl: DataLake):
        def attach_output(etl):
            self.get_prx(etl).outputs.append(tbl)
            return etl

        return attach_output
