import logging
from urllib.parse import urlencode

logger = logging.getLogger(__name__)


def draw_lineage_graph(**kwargs) -> None:
    from sqllineage import app, DEFAULT_PORT

    port = kwargs.pop("p", DEFAULT_PORT)
    querystring = urlencode({k: v for k, v in kwargs.items() if v})
    print(f" * SQLLineage Running on http://localhost:{port}/?{querystring}")
    app.run(port=port)
