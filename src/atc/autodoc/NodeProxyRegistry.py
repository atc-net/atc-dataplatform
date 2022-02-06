from typing import List

from atc.singleton import Singleton


class NodeProxyRegistry(metaclass=Singleton):
    """The NodeProxyRegistry holds a reference to every NodeProxy ever created.
    It has two functions:
        - reset all NodeProxies to create new nodes on next usage to support drawing multiple diagrams
        - provide a handle to all NodeProxies when the global diagram is to be drawn.
    """

    def __init__(self):
        self.proxies: List["NodeProxy"] = []

    def register_proxy(self, prxy: "NodeProxy"):
        self.proxies.append(prxy)

    def reset_proxies(self) -> None:
        for nd in self.proxies:
            nd.node_reset()
