from functools import lru_cache

from atc.functions import init_dbutils


@lru_cache
def getValue(secret_name: str):
    return init_dbutils().secrets.get("values", secret_name)


def resourceName():
    return getValue("resourceName")


def storageAccount():
    return resourceName()


def directAccessContainer(containername: str):
    domainPart = containername + "@" + storageAccount()

    # Example: abfss://silver@atc.dfs.core.windows.net/
    return "abfss://" + domainPart + ".dfs.core.windows.net/"
