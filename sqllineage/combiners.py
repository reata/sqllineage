from functools import reduce
from operator import add, or_
from typing import Set

from sqllineage.core import LineageResult
from sqllineage.models import Table


class LineageCombiner:
    @staticmethod
    def combine(*args: LineageResult) -> LineageResult:
        raise NotImplementedError


class NaiveLineageCombiner(LineageCombiner):
    @staticmethod
    def combine(*args: LineageResult) -> LineageResult:
        return reduce(add, args, LineageResult())


class DefaultLineageCombiner(LineageCombiner):
    @staticmethod
    def combine(*args: LineageResult) -> LineageResult:
        combined_result = LineageResult()
        for lineage_result in args:
            if lineage_result.drop:
                for st_tables in (combined_result.read, combined_result.write):
                    st_tables -= lineage_result.drop
            elif lineage_result.rename:
                for (table_old, table_new) in lineage_result.rename:
                    for st_tables in (combined_result.read, combined_result.write):
                        if table_old in st_tables:
                            st_tables.remove(table_old)
                            st_tables.add(table_new)
            elif lineage_result.intermediate:
                combined_result.read |= (
                    lineage_result.read - lineage_result.intermediate
                )
                combined_result.write |= lineage_result.write
            else:
                combined_result.read |= lineage_result.read
                combined_result.write |= lineage_result.write
        combined_result.intermediate = combined_result.read.intersection(
            combined_result.write
        )
        self_depend_tables = reduce(
            or_,
            (
                lineage_result.read.intersection(lineage_result.write)
                for lineage_result in args
            ),
            set(),
        )  # type: Set[Table]
        combined_result.intermediate -= self_depend_tables
        combined_result.read -= combined_result.intermediate
        combined_result.write -= combined_result.intermediate
        return combined_result
