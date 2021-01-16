import logging
import os
import sys
from argparse import Namespace
from urllib.parse import urlencode

from flask import Flask, jsonify, request
from flask_cors import CORS

from sqllineage import STATIC_FOLDRE, __name__ as name
from sqllineage.helpers import extract_sql_from_args
from sqllineage.runner import LineageRunner

logger = logging.getLogger(__name__)


def draw_lineage_graph(**kwargs) -> Flask:
    app = Flask(
        name,
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
    port = kwargs.pop("p")
    querystring = urlencode({k: v for k, v in kwargs.items() if v})
    print(f" * SQLLineage Running on http://localhost:{port}/?{querystring}")
    app.run(port=port)
    # return here is for testing purpose only
    return app
