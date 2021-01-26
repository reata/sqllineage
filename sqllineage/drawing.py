import logging
import os
import sys
from argparse import Namespace
from urllib.parse import urlencode

from flask import Flask, jsonify, request
from flask_cors import CORS

from sqllineage import DEFAULT_PORT
from sqllineage import STATIC_FOLDRE
from sqllineage.helpers import extract_sql_from_args

logger = logging.getLogger(__name__)

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
    # this is to avoid circular import
    from sqllineage.runner import LineageRunner

    req_args = Namespace(**request.get_json())
    sql = extract_sql_from_args(req_args)
    resp = LineageRunner(sql).to_cytoscape()
    return jsonify(resp)


cli = sys.modules["flask.cli"]
cli.show_server_banner = lambda *x: None  # type: ignore


def draw_lineage_graph(**kwargs) -> None:
    port = kwargs.pop("p", DEFAULT_PORT)
    querystring = urlencode({k: v for k, v in kwargs.items() if v})
    print(f" * SQLLineage Running on http://localhost:{port}/?{querystring}")
    app.run(port=port)
