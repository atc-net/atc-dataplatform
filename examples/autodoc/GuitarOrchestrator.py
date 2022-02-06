# GuitarOrchestrator.py
import atc.autodoc as doc
import tables


@doc.input(tables.guitars)
@doc.output(tables.products)
class GuitarOrchestrator:
    pass
