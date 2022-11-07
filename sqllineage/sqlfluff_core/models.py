import warnings
from typing import Optional

from sqlfluff.core.parser import BaseSegment

from sqllineage.core.models import SubQuery, Schema, Table
from sqllineage.exceptions import SQLLineageException
from sqllineage.utils.helpers import escape_identifier_name


class SqlFluffTable(Table):
    def __init__(self, name: str, schema: Schema = Schema(), **kwargs):
        """
        Data Class for Table

        :param name: table name
        :param schema: schema as defined by :class:`Schema`
        """
        if "." not in name:
            self.schema = schema
            self.raw_name = escape_identifier_name(name)
        else:
            schema_name, table_name = name.rsplit(".", 1)
            if len(schema_name.split(".")) > 2:
                # allow db.schema as schema_name, but a.b.c as schema_name is forbidden
                raise SQLLineageException("Invalid format for table name: %s.", name)
            self.schema = Schema(schema_name)
            self.raw_name = escape_identifier_name(table_name)
            if schema:
                warnings.warn("Name is in schema.table format, schema param is ignored")
        self.alias = kwargs.pop("alias", self.raw_name)

    @staticmethod
    def of(segment: BaseSegment, alias: str = None) -> "Table":
        # rewrite identifier's get_real_name method, by matching the last dot instead of the first dot, so that the
        # real name for a.b.c will be c instead of b
        dot_idx, _ = _token_matching(
            segment,
            lambda segment: segment.type == "symbol",
            start=len(segment.segments),
            reverse=True,
        )
        real_name = (
            segment.segments[dot_idx + 1].raw if dot_idx else segment.segments[0].raw
        )
        # rewrite identifier's get_parent_name accordingly
        parent_name = (
            "".join(
                [
                    escape_identifier_name(segment.raw)
                    for segment in segment.segments[:dot_idx]
                ]
            )
            if dot_idx
            else None
        )
        schema = Schema(parent_name) if parent_name is not None else Schema()
        kwargs = {"alias": alias} if alias else {}
        return SqlFluffTable(real_name, schema, **kwargs)


class SqlFluffSubQuery(SubQuery):
    def __init__(self, bracketed_segment: BaseSegment, alias: Optional[str]):
        """
        Data Class for SqlFluffSubQuery

        :param bracketed_segment: subquery segment
        :param alias: subquery alias
        """
        self.segment = bracketed_segment
        self._query = bracketed_segment.raw
        self.alias = alias if alias is not None else f"subquery_{hash(self)}"

    @staticmethod
    def of(bracketed_segment: BaseSegment, alias: Optional[str]) -> "SqlFluffSubQuery":
        return SqlFluffSubQuery(bracketed_segment, alias)


def _token_matching(segment: BaseSegment, funcs, start=0, end=None, reverse=False):
    """next token that match functions"""
    if start is None:
        return None

    if not isinstance(funcs, (list, tuple)):
        funcs = (funcs,)

    if reverse:
        assert end is None
        indexes = range(start - 2, -1, -1)
    else:
        if end is None:
            end = len(segment.segments)
        indexes = range(start, end)
    for idx in indexes:
        token = segment.segments[idx]
        for func in funcs:
            if func(token):
                return idx, token
    return None, None
