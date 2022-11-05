import pytest

from sqllineage.exceptions import SQLLineageException
from sqllineage.runner import LineageRunner


def test_select_without_table():
    with pytest.raises(SQLLineageException):
        LineageRunner("select * from where foo='bar'")._eval()
