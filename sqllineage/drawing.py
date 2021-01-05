import logging
import os
import sys
from argparse import Namespace
from urllib.parse import urlencode

from sqllineage import __name__ as name
from sqllineage.helpers import extract_sql_from_args
from sqllineage.runner import LineageRunner

logger = logging.getLogger(__name__)


def draw_lineage_graph(args: Namespace):
    try:
        from flask import Flask, jsonify, request
        from flask_cors import CORS
    except ImportError as e:
        raise ImportError("flask and flask_cors required for visualization") from e
    app = Flask(
        name,
        static_url_path="",
        static_folder=os.path.join(os.path.dirname(__file__), "build"),
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
    param = {}
    if args.f:
        param["f"] = args.f
    elif args.e:
        param["e"] = args.e
    querystring = urlencode(param)
    print(f" * SQLLineage Running on http://127.0.0.1:5000/?{querystring}")
    app.run()
    # return here is for testing purpose only
    return app
