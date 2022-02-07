from .EtlRegistry import EtlRegistry
from .JobRegistry import JobRegistry
from .NodeProxy import NodeProxy
from .resources import job_icon


class Job(NodeProxy):
    def __init__(self, name):
        super().__init__(name)
        self.steps = []
        JobRegistry().register_job(self)

    def step(self, etl) -> "Job":
        self.steps.append(etl)
        return self

    def create_node(self) -> "Node":
        from diagrams.custom import Custom
        from diagrams import Edge

        node = Custom(self.name, job_icon())

        for step_no, etl in enumerate(self.steps):
            step_prxy = EtlRegistry().get_prx(etl)
            # step_prxy.name = f"Step {step_no+1}:"+step_prxy.name
            step_node = step_prxy.get_node()
            node - Edge(color="red", style="dotted") - step_node
            # node - Edge(color="red", style="dotted", splines='curved') - step_node

        return node
