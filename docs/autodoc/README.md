# Automatic Diagram Documentation

## Introduction

Create files like

```
# tables.py

from atc.autodoc.DataLake import DataLake
from atc.autodoc.Group import Group

# groupings
bronze = Group("bronze")
silver = Group("silver")
gold = Group("gold")

# tables
guitars = DataLake("guitars", parent=bronze)
customers = DataLake("customers", parent=bronze)
products = DataLake("products", parent=silver)
orders = DataLake("orders", parent=gold)

```

```
# CustomerOrchestrator.py
import atc.autodoc as doc
import tables


@doc.input(tables.products)
@doc.input(tables.customers)
@doc.output(tables.orders)
class CustomerOrchestrator:
    pass

```

```
# GuitarOrchestrator.py
import atc.autodoc as doc
import tables


@doc.input(tables.guitars)
@doc.output(tables.products)
class GuitarOrchestrator:
    pass

```

```
# job.py
from GuitarOrchestrator import GuitarOrchestrator
from CustomerOrchestrator import CustomerOrchestrator
from atc.autodoc.Job import Job

Job("order flow").step(GuitarOrchestrator).step(CustomerOrchestrator)

```

then finally execute them like this:

```
import GuitarOrchestrator
import CustomerOrchestrator
import job

from atc import autodoc

mgr = autodoc.Manager()
mgr.create_job_diagrams()
mgr.create_single_etl_diagrams()
mgr.create_total_diagram()

```
Run `python diagram.py`

You will get diagrams like this:
![diagram](./total_diagram.png)
![job](./order_flow.png)
![etl](./customerorchestrator.png)
