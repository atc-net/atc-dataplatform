# job.py
from GuitarOrchestrator import GuitarOrchestrator
from CustomerOrchestrator import CustomerOrchestrator
from atc.autodoc.Job import Job

Job("order flow").step(GuitarOrchestrator).step(CustomerOrchestrator)
