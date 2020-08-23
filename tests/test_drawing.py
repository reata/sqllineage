import sys
from unittest.mock import patch

import pytest
from networkx import DiGraph

from sqllineage.drawing import draw_lineage_graph


@patch.dict(sys.modules, {"matplotlib": None})
def test_no_matplotlib():
    with pytest.raises(ImportError):
        draw_lineage_graph(DiGraph())


@patch.dict(sys.modules, {"pygraphviz": None})
def test_no_pygraphviz():
    with pytest.raises(ImportError):
        draw_lineage_graph(DiGraph())
