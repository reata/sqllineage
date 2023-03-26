from typing import List, Union

from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.core.models import Path, SubQuery, Table
from sqllineage.core.parser.sqlfluff.models import SqlFluffSubQuery, SqlFluffTable
from sqllineage.core.parser.sqlfluff.utils import get_table_alias
from sqllineage.utils.helpers import escape_identifier_name


def retrieve_holder_data_from(
    segments: List[BaseSegment],
    holder: SubQueryLineageHolder,
    table_identifier: BaseSegment,
) -> Union[Path, SubQuery, Table]:
    """
    Build a 'SqlFluffSubquery' or 'SqlFluffTable' for a given list of segments and a table identifier segment.
    It will use the list of segments to find an alias and the holder CTE set of 'SqlFluffSubQuery'.
    :param segments: list of segments to search for an alias
    :param holder: 'SqlFluffSubQueryLineageHolder' to use the CTE set of 'SqlFluffSubQuery'
    :param table_identifier: a table identifier segment
    :return: 'Path' or 'SqlFluffSubQuery' or 'SqlFluffTable' object
    """
    data = None
    alias = get_table_alias(segments)
    if "." not in table_identifier.raw:
        cte_dict = {s.alias: s for s in holder.cte}
        cte = cte_dict.get(table_identifier.raw)
        if cte is not None:
            # could reference CTE with or without alias
            data = SqlFluffSubQuery.of(
                cte.query,
                alias or table_identifier.raw,
            )
    if data is None:
        if table_identifier.type == "file_reference":
            return Path(escape_identifier_name(table_identifier.segments[-1].raw))
        else:
            return SqlFluffTable.of(table_identifier, alias=alias)
    return data
