import logging
import os
import sys
from argparse import Namespace
from pathlib import Path
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
    def find_children(folder: Path):
        children = []
        # sort with folder before file, and each in alphanumeric order
        for p in sorted(folder.iterdir(), key=lambda _: (not _.is_dir(), _.name)):
            if p.is_dir():
                children.append(
                    {"id": str(p), "name": p.name, "children": find_children(p)}
                )
            else:
                children.append({"id": str(p), "name": p.name})
        return children

    dir_conf_key = "SQLLINEAGE_DIRECTORY"
    root = (
        Path(os.environ[dir_conf_key])
        if dir_conf_key in os.environ
        else Path(
            request.get_json().get(
                "f", os.path.join(os.path.dirname(__file__), "data/tpcds/query01.sql")
            )
        ).parent
    )
    data = {"id": str(root), "name": root.name, "children": find_children(root)}
    return jsonify(data)


cli = sys.modules["flask.cli"]
cli.show_server_banner = lambda *x: None  # type: ignore


def draw_lineage_graph(**kwargs) -> None:
    port = kwargs.pop("p", DEFAULT_PORT)
    querystring = urlencode({k: v for k, v in kwargs.items() if v})
    print(f" * SQLLineage Running on http://localhost:{port}/?{querystring}")
    app.run(port=port)
