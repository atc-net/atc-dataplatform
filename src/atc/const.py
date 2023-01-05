from enum import StrEnum


class TableProperty(StrEnum):
    NAME = "name"
    PATH = "path"
    PARTITIONING = "partitioning"
    FORMAT = "format"
    SCHEMA = "schema"
    RELEASE = "release"
    DEBUG = "debug"
    ALIAS = "alias"
