import atexit
import importlib.resources


def get_resource_icon(resource_name):
    def get_icon():
        resource = importlib.resources.path(__name__, resource_name)
        path = resource.__enter__()

        atexit.register(lambda: resource.__exit__(None, None, None))
        return str(path)

    return get_icon


etl_icon = get_resource_icon("gears.png")
job_icon = get_resource_icon("databricks.png")
