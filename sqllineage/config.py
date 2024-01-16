import os


class _SQLLineageConfigLoader:
    """
    Load all configurable items from environment variable, otherwise fallback to default
    """

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

    def __getattr__(self, item):
        if item in self.config:
            type_, default = self.config[item]
            # require SQLLINEAGE_ prefix from environment variable
            return self.parse_value(
                os.environ.get("SQLLINEAGE_" + item, default), type_
            )
        else:
            return super().__getattribute__(item)

    @classmethod
    def parse_value(cls, value, cast):
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


SQLLineageConfig = _SQLLineageConfigLoader()
