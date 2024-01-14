import os


class _SQLLineageConfigLoader:
    """
    Load all configurable items from config variable, otherwise fallback to default
    """

    def __init__(self) -> None:
        self._init = False

        self._config = {
            "DIRECTORY": {
                "class_type": str,
                "additional": None,
                "default": os.path.join(os.path.dirname(__file__), "data"),
            },
            "DEFAULT_SCHEMA": {
                "class_type": str,
                "additional": None,
                "default": None,
            },
            "TSQL_NO_SEMICOLON": {
                "class_type": bool,
                "additional": None,
                "default": False,
            },
        }

        self._PREFIX = "SQLLINEAGE_"
        self._BOOLEAN_TRUE_STRINGS = ("true", "on", "ok", "y", "yes", "1")
        self._len_prefix = len(self._PREFIX)
        self._init = True

    def parse_value(self, value, cast):
        """Parse and cast provided value

        :param value: Stringed value.
        :param cast: Type to cast return value as.

        :returns: Casted value
        """
        if cast is bool:
            try:
                value = int(value) != 0
            except ValueError:
                value = value.lower().strip() in self._BOOLEAN_TRUE_STRINGS
        else:
            value = cast(value)

        return value

    def __getattr__(self, item):
        if self._config[item]["additional"] is not None:
            return self._config[item]["additional"]
        elif (os_env_item := self._PREFIX + item) in os.environ.keys():
            return self.parse_value(
                os.environ[os_env_item], self._config[item]["class_type"]
            )
        else:
            return self._config[item]["default"]

    def __setattr__(self, key, value):
        if key == "_init" or not self._init:
            super().__setattr__(key, value)
        else:
            if key not in [field for field in self._config.keys()]:
                raise ValueError(f"config {key} is not support")

            if value is None:
                self._config[key]["additional"] = value
            else:
                value = self.parse_value(value, self._config[key]["class_type"])
                if key == "DIRECTORY" and not os.path.isdir(value):
                    raise NotADirectoryError(f"{value} does not exist")
                self._config[key]["additional"] = value


SQLLineageConfig = _SQLLineageConfigLoader()
