from diagrams import Cluster, Node

from .NodeProxy import NodeProxy


class Group(NodeProxy):
    def __init__(self, name, parent: NodeProxy = None):
        super().__init__(name, parent)

    def create_node(self) -> Node:
        return Cluster(self.name)
