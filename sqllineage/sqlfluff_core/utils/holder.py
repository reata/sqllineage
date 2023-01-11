from typing import List, Union

from sqlfluff.core.parser import BaseSegment
from sqllineage.core.holders import SubQueryLineageHolder

from sqllineage.sqlfluff_core.models import SqlFluffSubQuery, SqlFluffTable
from sqllineage.sqlfluff_core.utils.sqlfluff import get_table_alias


def retrieve_holder_data_from(
    segments: List[BaseSegment],
    holder: SubQueryLineageHolder,
    table_identifier: BaseSegment,
) -> Union[SqlFluffTable, SqlFluffSubQuery]:
    data = None
    if "." not in table_identifier.raw:
        cte_dict = {s.alias: s for s in holder.cte}
        cte = cte_dict.get(table_identifier.raw)
        if cte is not None:
            # could reference CTE with or without alias
            data = SqlFluffSubQuery.of(
                cte.segment,
                table_identifier.raw,
            )
    if data is None:
        data = SqlFluffTable.of(table_identifier, alias=get_table_alias(segments))
    return data
