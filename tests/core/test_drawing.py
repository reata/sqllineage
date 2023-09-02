import json
import os.path
from collections import namedtuple
from http import HTTPStatus
from io import StringIO

from sqllineage.config import SQLLineageConfig
from sqllineage.drawing import app


def test_handler():
    container = namedtuple("response", ["status", "header"])

    def start_response(status, header):
        container.status = status
        container.header = header

    def mock_request(method, path, body=None):
        if isinstance(body, dict):
            body = json.dumps(body)
        environ = {"REQUEST_METHOD": method, "PATH_INFO": path}
        if body:
            with StringIO() as f:
                length = f.write(body)
                f.seek(0)
                environ["CONTENT_LENGTH"] = length
                environ["wsgi.input"] = f
                app(environ, start_response)
        else:
            app(environ, start_response)

    # 200
    mock_request("GET", "/")
    assert container.status.startswith(str(HTTPStatus.OK.value))
    mock_request("GET", "/manifest.json")
    assert container.status.startswith(str(HTTPStatus.OK.value))
    mock_request("POST", "/lineage", {"e": "SELECT * FROM dual", "p": 5000})
    assert container.status.startswith(str(HTTPStatus.OK.value))
    mock_request("POST", "/script", {"e": "SELECT * FROM dual", "p": 5000})
    assert container.status.startswith(str(HTTPStatus.OK.value))
    mock_request(
        "POST",
        "/directory",
        {"f": os.path.join(SQLLineageConfig.DIRECTORY, "tpcds/query01.sql")},
    )
    assert container.status.startswith(str(HTTPStatus.OK.value))
    mock_request(
        "POST",
        "/directory",
        {"d": os.path.join(SQLLineageConfig.DIRECTORY, "tpcds/")},
    )
    assert container.status.startswith(str(HTTPStatus.OK.value))
    mock_request("POST", "/directory", {})
    assert container.status.startswith(str(HTTPStatus.OK.value))
    mock_request("OPTIONS", "/directory", {})
    assert container.status.startswith(str(HTTPStatus.OK.value))
    # 400
    mock_request("POST", "/lineage", {"e": "SELECT * FROM where foo='bar'"})
    assert container.status.startswith(str(HTTPStatus.BAD_REQUEST.value))
    # 403
    mock_request("POST", "/directory", {"f": "/etc/passwd"})
    assert container.status.startswith(str(HTTPStatus.FORBIDDEN.value))
    mock_request("POST", "/directory", {"d": "/"})
    assert container.status.startswith(str(HTTPStatus.FORBIDDEN.value))
    # 404
    mock_request("GET", "/non-exist-resource")
    assert container.status.startswith(str(HTTPStatus.NOT_FOUND.value))
    mock_request("GET", "/static")
    assert container.status.startswith(str(HTTPStatus.NOT_FOUND.value))
    mock_request(
        "POST",
        "/script",
        {"f": os.path.join(SQLLineageConfig.DIRECTORY, "tpcds/query100.sql")},
    )
    assert container.status.startswith(str(HTTPStatus.NOT_FOUND.value))
    mock_request("POST", "/non-exist-resource", {"e": "SELECT * FROM where foo='bar'"})
    assert container.status.startswith(str(HTTPStatus.NOT_FOUND.value))
    mock_request("GET", "/../cli.py")
    assert container.status.startswith(str(HTTPStatus.NOT_FOUND.value))
    mock_request("OPTIONS", "/non-exist-resource")
    assert container.status.startswith(str(HTTPStatus.NOT_FOUND.value))
    # 405
    mock_request("PUT", "/")
    assert container.status.startswith(str(HTTPStatus.METHOD_NOT_ALLOWED.value))
    mock_request("PATCH", "/")
    assert container.status.startswith(str(HTTPStatus.METHOD_NOT_ALLOWED.value))
    mock_request("DELETE", "/")
    assert container.status.startswith(str(HTTPStatus.METHOD_NOT_ALLOWED.value))
