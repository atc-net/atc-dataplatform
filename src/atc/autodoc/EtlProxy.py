from typing import List


from .DataLake import DataLake
from .NodeProxy import NodeProxy
from .resources import etl_icon


class EtlProxy(NodeProxy):
    def __init__(self, etl):
        super().__init__(etl.__name__)
        self.etl = etl
        self.inputs: List[DataLake] = []
        self.outputs: List[DataLake] = []

    def create_node(self) -> "Node":
        from diagrams.custom import Custom

        node = Custom(self.name, etl_icon())
        for inpt in self.inputs:
            in_nd = inpt.get_node()
            in_nd >> node
        for outpt in self.outputs:
            out_nd = outpt.get_node()
            node >> out_nd
        return node
