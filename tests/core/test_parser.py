from unittest.mock import Mock

from sqllineage.core.parser.sqlfluff.models import SqlFluffColumn


def test_column_extract_source_columns():
    segment_mock = Mock()
    segment_mock.type = ""
    assert [] == SqlFluffColumn._extract_source_columns(segment_mock)
