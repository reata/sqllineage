import logging.config
import os
import sys
from argparse import Namespace

from flask import Flask, jsonify, request
from flask_cors import CORS

from sqllineage.helpers import extract_sql_from_args
from sqllineage.runner import LineageRunner

NAME = "sqllineage"
VERSION = "1.1.1"
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

STATIC_FOLDRE = "build"
DEFAULT_PORT = 5000

app = Flask(
    __name__,
    static_url_path="",
    static_folder=os.path.join(os.path.dirname(__file__), STATIC_FOLDRE),
)
CORS(app)


@app.route("/")
def index():
    return app.send_static_file("index.html")


@app.route("/lineage", methods=["POST"])
def lineage():
    req_args = Namespace(**request.get_json())
    sql = extract_sql_from_args(req_args)
    resp = LineageRunner(sql).to_cytoscape()
    return jsonify(resp)


cli = sys.modules["flask.cli"]
cli.show_server_banner = lambda *x: None  # type: ignore
