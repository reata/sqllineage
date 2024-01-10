import dataclasses
import os
from dataclasses import dataclass
from typing import Union, Any


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
    Load all configurable items from environment variable, otherwise fallback to default
    """
    def __init__(self):
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
    def DEFAULT_SCHEMA(self, value):
        if isinstance(value, self._config.DEFAULT_SCHEMA.class_type):
            self._config.DEFAULT_SCHEMA.value = value
        else:
            raise

    @property
    def DIRECTORY(self):
        return self._config.DIRECTORY.value

    @DIRECTORY.setter
    def DIRECTORY(self, value):
        if isinstance(value, self._config.DIRECTORY.class_type):
            self._config.DIRECTORY.value = value
        else:
            raise

    @property
    def TSQL_NO_SEMICOLON(self):
        return self._config.TSQL_NO_SEMICOLON.value

    @TSQL_NO_SEMICOLON.setter
    def TSQL_NO_SEMICOLON(self, value):
        if isinstance(value, self._config.TSQL_NO_SEMICOLON.class_type):
            self._config.TSQL_NO_SEMICOLON.value = value
        else:
            raise

    # def __getattr__(self, item):
    #     self._config: SQLLineageConfigDef
    #     a = dataclasses.fields(self._config)
    #     print(a)
        # if self._config.
        #     if item in self.config:
        #         return
        #         # type_, default = self.config[item]
        #         # # require SQLLINEAGE_ prefix from environment variable
        #         # return type_(os.environ.get("SQLLINEAGE_" + item, default))
        #     else:
        #         return super().__getattribute__(item)


SQLLineageConfig = _SQLLineageConfigLoader()

if __name__ == "__main__":
    SQLLineageConfig.DIRECTORY = "xxx"
    SQLLineageConfig.DEFAULT_SCHEMA = "ods"
    SQLLineageConfig.TSQL_NO_SEMICOLON = True
    print(SQLLineageConfig.DIRECTORY)
    print(SQLLineageConfig.DEFAULT_SCHEMA)
    print(SQLLineageConfig.TSQL_NO_SEMICOLON)
