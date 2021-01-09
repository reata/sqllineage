from http import HTTPStatus
from unittest.mock import patch

from sqllineage.drawing import draw_lineage_graph


@patch("flask.Flask.run")
def test_flask_handler(_):
    option = {"e": "select * from dual", "p": 5000}
    app = draw_lineage_graph(**option)
    with app.test_client() as c:
        resp = c.get("/")
        assert resp.status_code == HTTPStatus.OK
        resp = c.post("/lineage", json=option)
        assert resp.status_code == HTTPStatus.OK
