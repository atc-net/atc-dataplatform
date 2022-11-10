from functools import lru_cache

from atc.functions import init_dbutils


@lru_cache
def getValue(secret_name: str):
    return init_dbutils().secrets.get("values", secret_name)


def resourceName():
    return getValue("resourceName")


def cosmosEndpoint():
    return getValue("Cosmos--Endpoint")


def storageAccountUrl():
    return getValue("StorageAccount--Url")
