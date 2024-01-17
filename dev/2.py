import json
from collections import namedtuple
from http import HTTPStatus
from io import StringIO
from sqllineage.drawing import app

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
print(container.status)
assert container.status.startswith(str(HTTPStatus.OK.value))
