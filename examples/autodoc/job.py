# job.py

from GuitarOrchestrator import GuitarOrchestrator
from CustomerOrchestrator import CustomerOrchestrator
from atc.autodoc.Job import Job

order_flow = Job("order flow").step(GuitarOrchestrator).step(CustomerOrchestrator)
