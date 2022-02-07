from .NodeProxy import NodeProxy


class Group(NodeProxy):
    def __init__(self, name, parent: NodeProxy = None):
        super().__init__(name, parent)

    def create_node(self) -> "Node":
        from diagrams import Cluster

        return Cluster(self.name)
