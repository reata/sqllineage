import os
from dataclasses import dataclass
from typing import Any, Union


@dataclass
class SQLLineageConfigValue:
    class_type: Any
    value: Union[str, bool, None]


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
        self._config = SQLLineageConfigDef(
            DIRECTORY=SQLLineageConfigValue(
                str, os.path.join(os.path.dirname(__file__), "data")
            ),
            DEFAULT_SCHEMA=SQLLineageConfigValue(str, None),
            TSQL_NO_SEMICOLON=SQLLineageConfigValue(bool, False),
        )

    @property
    def DEFAULT_SCHEMA(self):
        return self._config.DEFAULT_SCHEMA.value

    @DEFAULT_SCHEMA.setter
    def DEFAULT_SCHEMA(self, value: str):
        if (
            value and isinstance(value, self._config.DEFAULT_SCHEMA.class_type)
        ) or value is None:
            self._config.DEFAULT_SCHEMA.value = value
        else:
            raise ValueError(
                f"DEFAULT_SCHEMA should be {self._config.DEFAULT_SCHEMA.class_type}"
            )

    @property
    def DIRECTORY(self):
        return self._config.DIRECTORY.value

    @DIRECTORY.setter
    def DIRECTORY(self, value: str):
        if value is None:
            value = os.path.join(os.path.dirname(__file__), "data")
        if isinstance(value, self._config.DEFAULT_SCHEMA.class_type):
            self._config.DIRECTORY.value = value
        else:
            raise ValueError(f"DIRECTORY should be {self._config.DIRECTORY.class_type}")

    @property
    def TSQL_NO_SEMICOLON(self):
        return self._config.TSQL_NO_SEMICOLON.value

    @TSQL_NO_SEMICOLON.setter
    def TSQL_NO_SEMICOLON(self, value: bool):
        if value is None:
            value = False
        if isinstance(value, self._config.TSQL_NO_SEMICOLON.class_type):
            self._config.TSQL_NO_SEMICOLON.value = value
        else:
            raise ValueError(
                f"TSQL_NO_SEMICOLON should be {self._config.TSQL_NO_SEMICOLON.class_type}"
            )


SQLLineageConfig = _SQLLineageConfigLoader()
