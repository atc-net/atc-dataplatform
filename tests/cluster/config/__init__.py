from atc import Configurator
from tests.cluster import values
from tests.cluster.values import storageAccountUrl


def InitConfigurator(*, clear=False):
    """This example function is how you would use the Configurator in a project."""
    tc = Configurator()
    if clear:
        tc.clear_all_configurations()

    # This is how you would set yourself up for different environments
    # tc.register('ENV','dev')

    tc.register("resourceName", values.resourceName())
    tc.register("storageAccount", f"abfss://silver@{storageAccountUrl()}")

    return tc
