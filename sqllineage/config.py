import os
from collections import namedtuple
from dataclasses import dataclass
from enum import auto
from typing import Any, Union

from pip._internal.utils.misc import enum

#
# class SQLLineageConfigKey(enum):
#     DIRECTORY=auto()
#     DEFAULT_SCHEMA=auto()
#     TSQL_NO_SEMICOLON=auto()
#
@dataclass
class SQLLineageConfigValue:
    class_type: Any
    value: Union[str, bool, None]

#
@dataclass
class SQLLineageConfigDef:
    DIRECTORY: SQLLineageConfigValue
    DEFAULT_SCHEMA: SQLLineageConfigValue
    TSQL_NO_SEMICOLON: SQLLineageConfigValue


class _SQLLineageConfigLoader:
    """
    Load all configurable items from config variable, otherwise fallback to default
    """
    def __init__(self) -> None:
        # SQLLineageConfigDef = namedtuple('SQLLineageConfigDef', ['class', 'value'])
        self._config = {
            "DIRECTORY": {
                "class_type": str,
                "value": {
                    "additional": None,
                    "os_env": None,
                    "default": os.path.join(os.path.dirname(__file__), "data"),
                },
            },
            "DEFAULT_SCHEMA": {
                "class_type": str,
                "value": {
                    "additional": None,
                    "os_env": None,
                    "default": None,
                },
            },
            "TSQL_NO_SEMICOLON": {
                "class_type": bool,
                "value": {"additional": None, "os_env": None, "default": False},
            },
        }

        self._prefix = "SQLLINEAGE_"
        self._len_prefix = len(self._prefix)

        for item in set(
            [self._prefix + key for key in self._config.keys()]
        ).intersection(set(os.environ.keys())):
            self._config[item[self._len_prefix :]] = os.environ[item]

    @property
    def DEFAULT_SCHEMA(self):
        for key, value in self._config["DEFAULT_SCHEMA"]["value"]:
            if value or key == "default":
                return value

    @DEFAULT_SCHEMA.setter
    def DEFAULT_SCHEMA(self, value: str):
        if (
            value and isinstance(value, self._config["DEFAULT_SCHEMA"]["class_type"])
        ) or value is None:
            self._config["DEFAULT_SCHEMA"]["value"]["additional"] = value
        else:
            raise ValueError(
                f"DEFAULT_SCHEMA should be {self._config['DEFAULT_SCHEMA']['class_type']}"
            )

    @property
    def DIRECTORY(self):
        for key, value in self._config["DIRECTORY"]["value"]:
            if value or key == "default":
                return value

    @DIRECTORY.setter
    def DIRECTORY(self, value: str):
        if value is None:
            self._config["DIRECTORY"]["value"]["additional"] = value
        else:
            assert isinstance(
                value, self._config["DIRECTORY"]["class_type"]
            ), ValueError(
                f"DIRECTORY should be {self._config['DIRECTORY']['class_type']}"
            )
            assert os.path.isdir(value), NotADirectoryError(f"DIRECTORY does not exits")
            self._config["DIRECTORY"]["value"]["additional"] = value

    @property
    def TSQL_NO_SEMICOLON(self):
        for key, value in self._config["TSQL_NO_SEMICOLON"]["value"]:
            if value or key == "default":
                return value

    @TSQL_NO_SEMICOLON.setter
    def TSQL_NO_SEMICOLON(self, value: bool):
        if (
            value and isinstance(value, self._config["TSQL_NO_SEMICOLON"]["class_type"])
        ) or value is None:
            self._config["TSQL_NO_SEMICOLON"]["value"]["additional"] = value
        else:
            raise ValueError(
                f"TSQL_NO_SEMICOLON should be {self._config['TSQL_NO_SEMICOLON']['class_type']}"
            )


SQLLineageConfig = _SQLLineageConfigLoader()
