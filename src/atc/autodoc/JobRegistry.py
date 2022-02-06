from typing import List

from atc.singleton import Singleton


class JobRegistry(metaclass=Singleton):
    """The JobRegistry holds a reference to every Job for access in the creation of job diagrams."""

    def __init__(self):
        self.jobs: List["Job"] = []

    def register_job(self, job: "Job"):
        self.jobs.append(job)
