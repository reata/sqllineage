from typing import List, Union

from sqlfluff.core.parser import BaseSegment

from sqllineage.sqlfluff_core.holders import SqlFluffSubQueryLineageHolder
from sqllineage.sqlfluff_core.models import SqlFluffSubQuery, SqlFluffTable
from sqllineage.sqlfluff_core.utils.sqlfluff import get_table_alias


def retrieve_holder_data_from(
    segments: List[BaseSegment],
    holder: SqlFluffSubQueryLineageHolder,
    table_identifier: BaseSegment,
) -> Union[SqlFluffTable, SqlFluffSubQuery]:
    """
    Build a 'SqlFluffSubquery' or 'SqlFluffTable' for a given list of segments and a table identifier segment.
    It will use the list of segments to find an alias and the holder CTE set of 'SqlFluffSubQuery'.
    :param segments: list of segments to search for an alias
    :param holder: 'SqlFluffSubQueryLineageHolder' to use the CTE set of 'SqlFluffSubQuery'
    :param table_identifier: a table identifier segment
    :return: 'SqlFluffSubQuery' or 'SqlFluffTable' object
    """
    data = None
    alias = get_table_alias(segments)
    if "." not in table_identifier.raw:
        cte_dict = {s.alias: s for s in holder.cte}
        cte = cte_dict.get(table_identifier.raw)
        if cte is not None:
            # could reference CTE with or without alias
            data = SqlFluffSubQuery.of(
                cte.segment,
                alias or table_identifier.raw,
            )
    if data is None:
        return SqlFluffTable.of(table_identifier, alias=alias)
    return data
