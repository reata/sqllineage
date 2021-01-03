import sys
from argparse import Namespace
from unittest.mock import patch

import pytest

from sqllineage.drawing import draw_lineage_graph


@patch.dict(sys.modules, {"flask": None})
def test_no_flask():
    with pytest.raises(ImportError):
        draw_lineage_graph(Namespace())


@patch.dict(sys.modules, {"flask_cors": None})
def test_no_flask_cors():
    with pytest.raises(ImportError):
        draw_lineage_graph(Namespace())
