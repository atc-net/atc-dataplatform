from abc import abstractmethod

from .NodeProxyRegistry import NodeProxyRegistry
from diagrams import Node


class NodeProxy:
    """The NodeProxy is a node in a diagram that is not instantiated until it is used.
    This allows it to be described and defined before and independently of the diagram
    that it appears in.

    Subclasses must implement create_node where the node itself is created (using the .name)
    """

    def __init__(self, name: str, parent: "NodeProxy" = None):
        self._node = None
        self.name = name
        self.parent = parent
        NodeProxyRegistry().register_proxy(self)

    @abstractmethod
    def create_node(self) -> Node:
        raise NotImplementedError()

    def node_reset(self) -> None:
        """Resets the diagram Node so that a new one is created on the next call to get_node."""
        self._node = None

    def get_node(self) -> Node:
        """Create the node according to the factory.
        If the node already exists, reuse it. This allows e.g. several transformations
        to reference the same table."""
        if self._node is None:
            if self.parent:
                with self.parent.get_node():
                    self._node = self.create_node()
            else:
                self._node = self.create_node()
        return self._node

    def set_parent(self, prnt: "NodeProxy") -> None:
        self.parent = prnt
