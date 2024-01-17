import sys
from pathlib import Path
from sqllineage.runner import LineageRunner


def go1():
    v_sql = Path(sys.argv[0]).parent.joinpath("test.sql").read_text()

    a = LineageRunner(sql=v_sql, dialect="ansi")
    a.print_column_lineage()


if __name__ == "__main__":
    go1()
