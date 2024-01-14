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
                "os_env": None,
                "default": os.path.join(os.path.dirname(__file__), "data"),
            },
            "DEFAULT_SCHEMA": {
                "class_type": str,
                "additional": None,
                "os_env": None,
                "default": None,
            },
            "TSQL_NO_SEMICOLON": {
                "class_type": bool,
                "additional": None,
                "os_env": None,
                "default": False,
            },
        }

        self._prefix = "SQLLINEAGE_"
        self._len_prefix = len(self._prefix)
        self._init = True

        for item in set(
            [self._prefix + key for key in self._config.keys()]
        ).intersection(set(os.environ.keys())):
            self._config[item[self._len_prefix :]]["os_env"] = os.environ[item]

    def __getattr__(self, item):
        if not self._init:
            super().__getattribute__(item)
        else:
            for field in self._config[item].keys():
                if field == "class_type":
                    continue
                if (value := self._config[item][field]) or field == "default":
                    return value

    def __setattr__(self, key, value):
        if key == "_init" or not self._init:
            super().__setattr__(key, value)
        else:
            if key not in [field for field in self._config.keys()]:
                raise ValueError(f"{key} is not support")

            if value is None or isinstance(value, self._config[key]["class_type"]):
                if key == "DIRECTORY" and value and not os.path.isdir(value):
                    raise NotADirectoryError(f"{value} does not exist")
                self._config[key]["additional"] = value
            else:
                raise ValueError(f"{key}:{value} class type incorrect")


SQLLineageConfig = _SQLLineageConfigLoader()
