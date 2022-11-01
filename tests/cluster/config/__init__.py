from atc import Configurator
from tests.cluster import values


def InitConfigurator(*, clear=False):
    """This example function is how you would use the Configurator in a project."""
    tc = Configurator()
    if clear:
        tc.clear_all_configurations()

    # This is how you would set yourself up for differnet environments
    # tc.register('ENV','dev')

    tc.register("resourceName", values.resourceName())
    tc.register("storageAccount", "abfss://silver@{resourceName}.dfs.core.windows.net")

    return tc
