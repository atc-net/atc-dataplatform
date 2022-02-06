# CustomerOrchestrator.py
import atc.autodoc as doc
import tables


@doc.input(tables.products)
@doc.input(tables.customers)
@doc.output(tables.orders)
class CustomerOrchestrator:
    pass
