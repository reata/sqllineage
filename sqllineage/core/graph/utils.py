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

    # seed reaching itself directly (a real self-loop edge) is a complete path
    # on its own, not a "half" to be stitched together with anything else.
    self_paths = (
        up_graph.list_lineage_paths(seed, seed)
        if seed in true_sources and seed in true_targets
        else []
    )

    up_paths = [[seed]] if seed in true_sources else []
    up_paths += list_lineage_paths_between(up_graph, true_sources - {seed}, {seed})
    down_paths = [[seed]] if seed in true_targets else []
    down_paths += list_lineage_paths_between(down_graph, {seed}, true_targets - {seed})
    joined = [
        up + down[1:]
        for up in up_paths
        for down in down_paths
        # the [seed] x [seed] pairing is the same relationship self_paths
        # already represents (correctly, as a self-loop edge or not at all);
        # skip it here so it isn't double-counted or wrongly trivialized.
        if not (up == [seed] and down == [seed])
    ]
    return self_paths + joined
