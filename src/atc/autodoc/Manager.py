from atc.autodoc import NodeProxyRegistry, EtlRegistry
from atc.autodoc.JobRegistry import JobRegistry
from atc.singleton import Singleton


class Manager(metaclass=Singleton):
    def __init__(self):
        self.jobs = JobRegistry()
        self.nodes = NodeProxyRegistry()
        self.etls = EtlRegistry()

        import diagrams

        self.diagrams = diagrams

    def create_total_diagram(self, title: str = None):
        # create total diagram
        self.nodes.reset_proxies()
        print(f"creating diagram for all nodes")
        with self.diagrams.Diagram(title or "Total Diagram", show=False):
            for prxy in self.nodes.proxies:
                prxy.get_node()

    def create_single_etl_diagrams(self):
        # create single orchestrator diagrams
        for etl in self.etls.etls.values():
            self.nodes.reset_proxies()
            print(f"creating diagram for single etl {etl.name}")
            with self.diagrams.Diagram(etl.name, show=False):
                etl.get_node()

    def create_job_diagrams(self):
        # create job diagrams
        self.nodes.reset_proxies()
        for job in self.jobs.jobs:
            print(f"creating job diagram for {job.name}")
            with self.diagrams.Diagram(job.name, show=False):
                job.get_node()
