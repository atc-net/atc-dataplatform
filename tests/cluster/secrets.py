from functools import lru_cache

from atc.functions import init_dbutils


@lru_cache
def getSecret(secret_name: str):
    return init_dbutils().secrets.get("secrets", secret_name)


def cosmosAccountKey():
    return getSecret("Cosmos--AccountKey")


def eventHubConnection():
    return getSecret("EventHubConnection")


def sqlServerUser():
    return getSecret("SqlServer--DatabricksUser")


def sqlServerUserPassword():
    return getSecret("SqlServer--DatabricksUserPassword")


def dbDeployClientSecret():
    return getSecret("DbDeploy--ClientSecret")


def dbDeployClientId():
    return getSecret("DbDeploy--ClientId")
