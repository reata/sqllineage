import os
import threading
from typing import Any, Dict

from sqllineage.exceptions import ConfigException


class _SQLLineageConfigLoader:
    """
    Load all configurable items from environment variable, otherwise fallback to default
    """

    _thread_config: Dict[int, Dict[str, Any]] = {}

    # inspired by https://github.com/joke2k/django-environ
    config = {
        # for frontend directory drawer
        "DIRECTORY": (str, os.path.join(os.path.dirname(__file__), "data")),
        # set default schema/database
        "DEFAULT_SCHEMA": (str, ""),
        # to enable tsql no semicolon splitter mode
        "TSQL_NO_SEMICOLON": (bool, False),
        # lateral column alias reference supported by some dialect (redshift, spark 3.4+, etc)
        "LATERAL_COLUMN_ALIAS_REFERENCE": (bool, False),
    }
    BOOLEAN_TRUE_STRINGS = ("true", "on", "ok", "y", "yes", "1")

    def __getattr__(self, item: str):
        if item in self.config.keys():
            if value := self._thread_config.get(self.get_ident(), {}).get(item, None):
                return value

            type_, default = self.config[item]
            # require SQLLINEAGE_ prefix from environment variable
            return self.parse_value(
                os.environ.get("SQLLINEAGE_" + item, default), type_
            )
        else:
            return super().__getattribute__(item)

    def __setattr__(self, key, value) -> None:
        if key in self.config:
            raise ConfigException(
                "SQLLineageConfig is read-only. Use context manager to update thread level config."
            )
        else:
            super().__setattr__(key, value)

    @classmethod
    def get_ident(cls) -> int:
        return threading.get_ident()

    @classmethod
    def parse_value(cls, value, cast) -> Any:
        """Parse and cast provided value

        :param value: Stringed value.
        :param cast: Type to cast return value as.

        :returns: Casted value
        """
        if cast is bool:
            try:
                value = int(value) != 0
            except ValueError:
                value = value.lower().strip() in cls.BOOLEAN_TRUE_STRINGS
        else:
            value = cast(value)

        return value

    def __call__(self, *args, **kwargs):
        self.__enter__()
        for key, value in kwargs.items():
            if key in self.config.keys():
                self._thread_config[self.get_ident()][key] = self.parse_value(
                    value, self.config[key][0]
                )
            else:
                super().__setattr__(key, value)
        return self

    def __enter__(self):
        if self.get_ident() not in self._thread_config.keys():
            self._thread_config[self.get_ident()] = {}
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.get_ident() in self._thread_config.keys():
            self._thread_config.pop(self.get_ident())


SQLLineageConfig = _SQLLineageConfigLoader()
