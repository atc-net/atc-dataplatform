from diagrams import Node

from atc.autodoc.NodeProxy import NodeProxy
import diagrams.azure.database


class DataLake(NodeProxy):
    def __init__(self, name, parent: NodeProxy = None):
        super().__init__(name, parent)

    def create_node(self) -> Node:
        return diagrams.azure.database.DataLake(self.name)
