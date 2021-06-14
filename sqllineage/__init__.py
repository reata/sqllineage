import logging.config
import os

NAME = "sqllineage"
VERSION = "1.2.4"
DEFAULT_LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {"default": {"format": "%(levelname)s: %(message)s"}},
    "handlers": {
        "console": {
            "level": "WARNING",
            "class": "logging.StreamHandler",
            "formatter": "default",
        }
    },
    "loggers": {
        "": {
            "handlers": ["console"],
            "level": "WARNING",
            "propagate": False,
            "filters": [],
        },
        "werkzeug": {
            "handlers": ["console"],
            "level": "ERROR",
            "propagate": False,
            "filters": [],
        },
    },
}
logging.config.dictConfig(DEFAULT_LOGGING)

STATIC_FOLDER = "build"
DATA_FOLDER = os.environ.get(
    "SQLLINEAGE_DIRECTORY", os.path.join(os.path.dirname(__file__), "data")
)
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 5000
