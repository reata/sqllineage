from functools import reduce
from operator import add

from sqllineage.core import LineageResult


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
            else:
                combined_result.read |= lineage_result.read
                combined_result.write |= lineage_result.write
        tmp_tables = combined_result.read.intersection(combined_result.write)
        combined_result.read -= tmp_tables
        combined_result.write -= tmp_tables
        return combined_result
