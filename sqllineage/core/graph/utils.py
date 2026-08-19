import itertools
from typing import Any

from sqllineage.core.graph_operator import GraphOperator


def list_lineage_paths_between(
    graph: GraphOperator, sources: set[Any], targets: set[Any]
) -> list[list[Any]]:
    """Naive path enumeration: every source/target pair, no filtering."""
    return [
        path
        for source, target in itertools.product(sources, targets)
        for path in graph.list_lineage_paths(source, target)
    ]


def cone_lineage_paths(
    graph: GraphOperator, seed: Any, sources: set[Any], targets: set[Any]
) -> list[list[Any]]:
    """
    Path enumeration restricted to the subgraph around ``seed``: only
    ancestors/descendants of ``seed`` are considered, so path enumeration
    runs on a much smaller subgraph than the full lineage graph.
    """
    ancestors = graph.ancestors(seed)
    descendants = graph.descendants(seed)
    true_sources = ancestors & sources
    true_targets = descendants & targets
    if seed in sources:
        true_sources.add(seed)
    if seed in targets:
        true_targets.add(seed)
    if not true_sources or not true_targets:
        return []

    up_graph = graph.get_sub_graph(*(ancestors | {seed}))
    down_graph = graph.get_sub_graph(*(descendants | {seed}))
    up_paths = [[seed]] if seed in true_sources else []
    up_paths += [
        path
        for source in true_sources - {seed}
        for path in up_graph.list_lineage_paths(source, seed)
    ]
    down_paths = [[seed]] if seed in true_targets else []
    down_paths += [
        path
        for target in true_targets - {seed}
        for path in down_graph.list_lineage_paths(seed, target)
    ]
    return [up + down[1:] for up in up_paths for down in down_paths]
