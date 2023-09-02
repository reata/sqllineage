import os


class _SQLLineageConfigLoader:
    """
    Load all configurable items from environment variable, otherwise fallback to default
    """

    # inspired by https://github.com/joke2k/django-environ
    config = {
        # for frontend directory drawer
        "DIRECTORY": (str, os.path.join(os.path.dirname(__file__), "data")),
        # to enable tsql no semicolon splitter mode
        "TSQL_NO_SEMICOLON": (bool, False),
    }

    def __getattr__(self, item):
        if item in self.config:
            type_, default = self.config[item]
            # require SQLLINEAGE_ prefix from environment variable
            return type_(os.environ.get("SQLLINEAGE_" + item, default))
        else:
            return super().__getattribute__(item)


SQLLineageConfig = _SQLLineageConfigLoader()
