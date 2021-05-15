import logging
import os
import sys
from argparse import Namespace
from pathlib import Path
from urllib.parse import urlencode

from flask import Flask, jsonify, request
from flask_cors import CORS
from werkzeug.exceptions import InternalServerError

from sqllineage import DATA_FOLDER, DEFAULT_HOST, DEFAULT_PORT
from sqllineage import STATIC_FOLDER
from sqllineage.helpers import extract_sql_from_args

logger = logging.getLogger(__name__)

app = Flask(
    __name__,
    static_url_path="",
    static_folder=os.path.join(os.path.dirname(__file__), STATIC_FOLDER),
)
CORS(app)


@app.errorhandler(InternalServerError)
def handle_500(e):
    original = getattr(e, "original_exception", None)
    return jsonify({"message": str(original)}), 400


@app.route("/")
def index():
    return app.send_static_file("index.html")


@app.route("/lineage", methods=["POST"])
def lineage():
    # this is to avoid circular import
    from sqllineage.runner import LineageRunner

    req_args = Namespace(**request.get_json())
    sql = extract_sql_from_args(req_args)
    lr = LineageRunner(sql, verbose=True)
    resp = {"verbose": str(lr), "dag": lr.to_cytoscape()}
    return jsonify(resp)


@app.route("/script", methods=["POST"])
def script():
    req_args = Namespace(**request.get_json())
    sql = extract_sql_from_args(req_args)
    return jsonify({"content": sql})


@app.route("/directory", methods=["POST"])
def directory():
    payload = request.get_json()
    if payload.get("f"):
        root = Path(payload["f"]).parent
    elif payload.get("d"):
        root = Path(payload["d"])
    else:
        root = Path(DATA_FOLDER)
    data = {
        "id": str(root),
        "name": root.name,
        "is_dir": True,
        "children": [
            {"id": str(p), "name": p.name, "is_dir": p.is_dir()}
            for p in sorted(root.iterdir(), key=lambda _: (not _.is_dir(), _.name))
        ],
    }
    return jsonify(data)


cli = sys.modules["flask.cli"]
cli.show_server_banner = lambda *x: None  # type: ignore


def draw_lineage_graph(**kwargs) -> None:
    host = kwargs.pop("host", DEFAULT_HOST)
    port = kwargs.pop("port", DEFAULT_PORT)
    querystring = urlencode({k: v for k, v in kwargs.items() if v})
    path = f"/?{querystring}" if querystring else "/"
    print(f" * SQLLineage Running on http://{host}:{port}{path}")
    app.run(host=host, port=port)
