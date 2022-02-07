from atc.autodoc.NodeProxy import NodeProxy


class DataLake(NodeProxy):
    def __init__(self, name, parent: NodeProxy = None):
        super().__init__(name, parent)

    def create_node(self) -> "Node":
        import diagrams.azure.database

        return diagrams.azure.database.DataLake(self.name)
