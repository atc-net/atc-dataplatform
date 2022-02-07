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
