import GuitarOrchestrator
import CustomerOrchestrator
import job

from atc import autodoc

mgr = autodoc.Manager()
mgr.create_job_diagrams()
mgr.create_single_etl_diagrams()
mgr.create_total_diagram()
