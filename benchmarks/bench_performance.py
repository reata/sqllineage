"""
Performance benchmarks for sqllineage hot paths.

Micro-benchmarks:
  - Table.__str__ / Table.__hash__
  - Column.__str__ / Column.__hash__ (with parent set)

Integration benchmark:
  - LineageRunner on a synthetic workload with many column lineage statements,
    write_columns (INSERT INTO tab (cols) SELECT ...), and alias mappings.

Usage:
  python benchmarks/bench_performance.py

Results are printed to stdout.  Run before and after applying optimisations
to compare.
"""

import pathlib
import sys
import timeit

# Make sure the project root is on the path when running from any directory.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from sqllineage.core.graph.networkx import NetworkXGraphOperator
from sqllineage.core.graph.rustworkx import RustworkXGraphOperator
from sqllineage.core.models import Column, Path, Table
from sqllineage.core.parser.sqlfluff.analyzer import SqlFluffLineageAnalyzer
from sqllineage.runner import LineageRunner
from sqllineage.utils.constant import NodeTag

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _hr(label: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {label}")
    print("=" * 60)


def _report(name: str, seconds: float, iterations: int) -> None:
    per_call_ns = seconds / iterations * 1e9
    print(
        f"  {name:<45}  {per_call_ns:>8.1f} ns/call   ({iterations:,} iters, {seconds:.3f}s total)"
    )


def _report_comparison(name: str, old_s: float, new_s: float, n: int) -> None:
    old_ns = old_s / n * 1e9
    new_ns = new_s / n * 1e9
    print(f"  {name:<42}  {old_ns:9.1f} ns  {new_ns:9.1f} ns  {old_s / new_s:7.1f}x")


# ---------------------------------------------------------------------------
# Old implementations for Table micro-benchmark
# ---------------------------------------------------------------------------


def _table_str_old(t: Table) -> str:
    return f"{t.schema}.{t.raw_name}"


def _table_hash_old(t: Table) -> int:
    return hash(f"{t.schema}.{t.raw_name}")


# ---------------------------------------------------------------------------
# Old / proposed implementations for Column micro-benchmark
# ---------------------------------------------------------------------------


def _col_str_old(col: Column) -> str:
    p = col.parent
    return (
        f"{p}.{col.raw_name}"
        if p is not None and not isinstance(p, Path)
        else col.raw_name
    )


def _col_hash_old(col: Column) -> int:
    return hash(_col_str_old(col))


def _col_parent_proposed(col: Column):
    for p in col._parent:
        return p
    return None


# ---------------------------------------------------------------------------
# Micro-benchmark: Table
# ---------------------------------------------------------------------------


def bench_table() -> None:
    _hr("Micro: Table.__str__ and Table.__hash__")

    t = Table("schema1.orders")
    n = 1_000_000

    header = f"{'Operation':<42}  {'Old':>12}  {'New':>12}  {'Speedup':>8}"
    print("  " + header)
    print("  " + "-" * len(header))

    old_t = timeit.timeit(lambda: _table_str_old(t), number=n)
    new_t = timeit.timeit(lambda: str(t), number=n)
    _report_comparison("Table.__str__", old_t, new_t, n)

    old_t = timeit.timeit(lambda: _table_hash_old(t), number=n)
    new_t = timeit.timeit(lambda: hash(t), number=n)
    _report_comparison("Table.__hash__", old_t, new_t, n)

    n_construct = 200_000
    elapsed = timeit.timeit(lambda: Table("schema1.orders"), number=n_construct)
    print(
        f"  {'Table() construction (cache built at init)':<42}  {'n/a':>12}  {elapsed / n_construct * 1e9:9.1f} ns"
    )


# ---------------------------------------------------------------------------
# Micro-benchmark: Column
# ---------------------------------------------------------------------------


def bench_column() -> None:
    _hr("Micro: Column.__str__ and Column.__hash__")

    col = Column("order_id")
    col.parent = Table("schema1.orders")
    n = 1_000_000

    header = f"{'Operation':<42}  {'Old':>12}  {'New':>12}  {'Speedup':>8}"
    print("  " + header)
    print("  " + "-" * len(header))

    old_t = timeit.timeit(lambda: _col_str_old(col), number=n)
    new_t = timeit.timeit(lambda: str(col), number=n)
    _report_comparison("Column.__str__ (with parent)", old_t, new_t, n)

    old_t = timeit.timeit(lambda: _col_hash_old(col), number=n)
    new_t = timeit.timeit(lambda: hash(col), number=n)
    _report_comparison("Column.__hash__ (with parent)", old_t, new_t, n)

    col_no_parent = Column("order_id")
    old_t = timeit.timeit(lambda: _col_str_old(col_no_parent), number=n)
    new_t = timeit.timeit(lambda: str(col_no_parent), number=n)
    _report_comparison("Column.__str__ (no parent)", old_t, new_t, n)

    old_t = timeit.timeit(lambda: col.parent, number=n)
    new_t = timeit.timeit(lambda: _col_parent_proposed(col), number=n)
    _report_comparison("Column.parent get (proposed for-loop)", old_t, new_t, n)

    t1 = Table("schema1.orders")
    n_set = 100_000
    col2 = Column("x")
    elapsed = timeit.timeit(lambda: col2.__class__.parent.fset(col2, t1), number=n_set)
    print(
        f"  {'Column.parent setter (unchanged)':<42}  {'n/a':>12}  {elapsed / n_set * 1e9:9.1f} ns"
    )


# ---------------------------------------------------------------------------
# Integration benchmark: LineageRunner
# ---------------------------------------------------------------------------


def _build_sql_workload(n_stmts: int) -> list[str]:
    """
    Build a list of n_stmts individual SQL statements that exercise:
      - write_columns path (INSERT INTO tab (cols) SELECT ...)
      - alias_mapping path (tables with aliases and joins)
      - multi-column lineage (wide tables)
      - UNION queries (multiple union barriers)
    """
    stmts = []

    cols_wide = ", ".join(f"col{i}" for i in range(1, 21))
    cols_wide_src = ", ".join(f"s.col{i}" for i in range(1, 21))
    cols_wide_tgt = ", ".join(f"col{i}" for i in range(1, 21))

    for i in range(n_stmts):
        schema = f"db.s{i % 5}"
        src = f"{schema}.source_{i % 10}"
        tgt = f"{schema}.target_{i % 10}"
        alias = f"s{i % 4}"

        pattern = i % 4
        if pattern == 0:
            # wide INSERT with explicit column list (exercises write_columns)
            stmts.append(
                f"INSERT INTO {tgt} ({cols_wide_tgt}) SELECT {cols_wide_src} FROM {src} AS {alias}"
            )
        elif pattern == 1:
            # join with aliases (exercises alias_mapping)
            src2 = f"{schema}.dim_{i % 8}"
            stmts.append(
                f"INSERT INTO {tgt} SELECT a.col1, b.col2, a.col3 FROM {src} AS a JOIN {src2} AS b ON a.id = b.id"
            )
        elif pattern == 2:
            # UNION (multiple union barriers, exercises alias_mapping per barrier)
            stmts.append(
                f"INSERT INTO {tgt} "
                f"SELECT col1, col2 FROM {src} "
                f"UNION ALL "
                f"SELECT col1, col2 FROM {schema}.fallback_{i % 6}"
            )
        else:
            # plain wide SELECT without column list
            stmts.append(f"INSERT INTO {tgt} SELECT {cols_wide} FROM {src}")

    return stmts


def bench_lineage_runner(n_stmts: int = 120, n_repeats: int = 3) -> None:
    _hr(f"Integration: LineageRunner ({n_stmts} statements, {n_repeats} repeats)")

    sql = ";\n".join(_build_sql_workload(n_stmts))

    times = []
    for rep in range(n_repeats):
        start = timeit.default_timer()
        lr = LineageRunner(sql, dialect="ansi")
        _ = lr.get_column_lineage()
        elapsed = timeit.default_timer() - start
        times.append(elapsed)
        print(f"  run {rep + 1}: {elapsed:.3f}s")

    best = min(times)
    avg = sum(times) / len(times)
    print(f"  best={best:.3f}s  avg={avg:.3f}s")


# ---------------------------------------------------------------------------
# Micro-benchmark: retrieve_vertices_by_props tag index vs O(N) scan
# ---------------------------------------------------------------------------


def _build_tagged_graph(cls, size: int):
    g = cls()
    for i in range(size):
        t = Table(f"schema.table_{i}")
        props = {}
        if i % 5 == 0:
            props[NodeTag.READ] = True
        if i % 5 == 1:
            props[NodeTag.WRITE] = True
        g.add_vertex_if_not_exist(t, **props)
    return g


def _old_scan_nx(g, prop: str) -> list:
    return [v for v, attr in g.graph.nodes(data=True) if attr.get(prop) is True]


def _old_scan_rx(g, prop: str) -> list:
    return [
        g.graph[i]["vertex"]
        for i in g.graph.node_indices()
        if g.graph[i].get(prop) is True
    ]


def bench_tag_index(n: int = 50_000) -> None:
    _hr("Micro: retrieve_vertices_by_props  O(N) scan vs O(1) tag index")

    header = f"{'Size':<6}  {'Impl':<10}  {'Old scan':>14}  {'New index':>14}  {'Speedup':>8}"
    print("  " + header)
    print("  " + "-" * len(header))

    for size in (500, 2000):
        for cls, old_fn, label in [
            (NetworkXGraphOperator, _old_scan_nx, "NetworkX"),
            (RustworkXGraphOperator, _old_scan_rx, "RustworkX"),
        ]:
            g = _build_tagged_graph(cls, size)
            old_t = timeit.timeit(lambda: old_fn(g, NodeTag.READ), number=n)
            new_t = timeit.timeit(
                lambda: g.retrieve_vertices_by_props(**{NodeTag.READ: True}),
                number=n,
            )
            speedup = old_t / new_t
            print(
                f"  {size:<6}  {label:<10}  {old_t / n * 1e6:11.2f} us  {new_t / n * 1e6:11.2f} us  {speedup:7.1f}x"
            )


# ---------------------------------------------------------------------------
# Integration benchmark: batch parse vs sequential per-statement parse
# ---------------------------------------------------------------------------


def bench_batch_parse(n_repeats: int = 3) -> None:
    _hr("Parse: batch (preparse) vs sequential per-statement")

    header = (
        f"{'Stmts':<8}  {'Mode':<12}  {'Best (s)':>10}  {'Avg (s)':>10}  {'Speedup':>8}"
    )
    print("  " + header)
    print("  " + "-" * len(header))

    for n_stmts in (60, 120, 480):
        stmts = _build_sql_workload(n_stmts)

        # Sequential: N separate Linter calls (simulates old per-statement behaviour)
        seq_times = []
        for _ in range(n_repeats):
            a = SqlFluffLineageAnalyzer("", ".", "ansi")
            t = timeit.default_timer()
            for stmt in stmts:
                a._list_specific_statement_segment(stmt)
            seq_times.append(timeit.default_timer() - t)

        # Batch: constructor triggers batch parse of all statements at once
        batch_times = []
        for _ in range(n_repeats):
            combined = ";\n".join(stmts)
            t = timeit.default_timer()
            a = SqlFluffLineageAnalyzer(combined, ".", "ansi")
            batch_times.append(timeit.default_timer() - t)

        seq_best, seq_avg = min(seq_times), sum(seq_times) / n_repeats
        batch_best, batch_avg = min(batch_times), sum(batch_times) / n_repeats
        speedup = seq_best / batch_best

        print(f"  {n_stmts:<8}  {'sequential':<12}  {seq_best:10.3f}  {seq_avg:10.3f}")
        print(
            f"  {n_stmts:<8}  {'batch':<12}  {batch_best:10.3f}  {batch_avg:10.3f}  {speedup:7.2f}x"
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("sqllineage performance benchmark")
    print(f"Python {sys.version}")

    bench_table()
    bench_column()
    bench_tag_index()
    bench_batch_parse()
    bench_lineage_runner()

    print("\nDone.")
