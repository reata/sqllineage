from unittest.mock import Mock

import pytest

from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.core.parser.sqlfluff.handlers.base import ConditionalSegmentBaseHandler
from sqllineage.core.parser.sqlfluff.models import SqlFluffColumn


def test_column_extract_source_columns():
    segment_mock = Mock()
    segment_mock.type = ""
    assert [] == SqlFluffColumn._extract_source_columns(segment_mock)


def test_handler_dummy():
    segment_mock = Mock()
    holder = SubQueryLineageHolder()
    c_handler = ConditionalSegmentBaseHandler()
    with pytest.raises(NotImplementedError):
        c_handler.handle(segment_mock, holder)
    with pytest.raises(NotImplementedError):
        c_handler.indicate(segment_mock)
