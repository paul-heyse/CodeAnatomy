"""Graph utilities using rustworkx for efficient graph algorithms.

Provides SCC detection, cycle finding, ancestor/descendant traversal.
"""

from __future__ import annotations

import rustworkx as rx


def _build_digraph(graph: dict[str, set[str]]) -> tuple[rx.PyDiGraph, list[str], dict[str, int]]:
    """Build rustworkx digraph from adjacency dict.

    Parameters
    ----------
    graph : dict[str, set[str]]
        Adjacency dict: source -> set of targets.

    Returns
    -------
    tuple[rx.PyDiGraph, list[str], dict[str, int]]
        Graph, node list, and node-to-index mapping.
    """
    # Collect all nodes (sources and targets)
    nodes: list[str] = list(graph.keys())
    for targets in graph.values():
        nodes.extend(t for t in targets if t not in nodes)
    nodes = list(dict.fromkeys(nodes))  # dedupe preserving order

    node_to_idx = {n: i for i, n in enumerate(nodes)}

    # Build digraph
    g: rx.PyDiGraph = rx.PyDiGraph()
    g.add_nodes_from(range(len(nodes)))

    edges: list[tuple[int, int]] = []
    for src, targets in graph.items():
        src_idx = node_to_idx[src]
        edges.extend(
            (src_idx, node_to_idx[tgt])
            for tgt in targets
            if tgt in node_to_idx
        )
    g.add_edges_from_no_data(edges)

    return g, nodes, node_to_idx


def find_sccs(graph: dict[str, set[str]]) -> list[list[str]]:
    """Find strongly connected components with >1 node (cycles).

    Parameters
    ----------
    graph : dict[str, set[str]]
        Adjacency dict: source -> set of targets.

    Returns
    -------
    list[list[str]]
        List of SCCs, each SCC is a list of nodes forming a cycle.
    """
    if not graph:
        return []

    g, nodes, _ = _build_digraph(graph)

    # Find SCCs - returns list of node index lists
    sccs = rx.strongly_connected_components(g)

    # Convert back to node names, filter to cycles (>1 node)
    return [[nodes[idx] for idx in scc] for scc in sccs if len(scc) > 1]


def find_simple_cycles(graph: dict[str, set[str]], *, max_cycles: int = 100) -> list[list[str]]:
    """Find all simple cycles in the graph.

    Parameters
    ----------
    graph : dict[str, set[str]]
        Adjacency dict: source -> set of targets.
    max_cycles : int
        Maximum number of cycles to return.

    Returns
    -------
    list[list[str]]
        List of cycles, each cycle is a list of nodes.
    """
    if not graph:
        return []

    g, nodes, _ = _build_digraph(graph)

    cycles: list[list[str]] = []
    for cycle in rx.simple_cycles(g):
        if len(cycles) >= max_cycles:
            break
        cycles.append([nodes[idx] for idx in cycle])

    return cycles


def get_ancestors(graph: dict[str, set[str]], node: str) -> set[str]:
    """Get all transitive dependencies of a node.

    Parameters
    ----------
    graph : dict[str, set[str]]
        Adjacency dict: source -> set of targets.
    node : str
        Node to find ancestors for.

    Returns
    -------
    set[str]
        All nodes that can reach the given node.
    """
    if not graph or (node not in graph and not any(node in targets for targets in graph.values())):
        return set()

    g, nodes, node_to_idx = _build_digraph(graph)

    idx = node_to_idx.get(node)
    if idx is None:
        return set()

    anc: set[int] = rx.ancestors(g, idx)
    return {nodes[i] for i in anc}


def get_descendants(graph: dict[str, set[str]], node: str) -> set[str]:
    """Get all modules that depend on a node (reverse reachability).

    Parameters
    ----------
    graph : dict[str, set[str]]
        Adjacency dict: source -> set of targets.
    node : str
        Node to find descendants for.

    Returns
    -------
    set[str]
        All nodes reachable from the given node.
    """
    if not graph or node not in graph:
        return set()

    g, nodes, node_to_idx = _build_digraph(graph)

    idx = node_to_idx.get(node)
    if idx is None:
        return set()

    desc: set[int] = rx.descendants(g, idx)
    return {nodes[i] for i in desc}


def topological_sort(graph: dict[str, set[str]]) -> list[str] | None:
    """Return topological ordering if graph is a DAG, else None.

    Parameters
    ----------
    graph : dict[str, set[str]]
        Adjacency dict: source -> set of targets.

    Returns
    -------
    list[str] | None
        Topological ordering or None if graph has cycles.
    """
    if not graph:
        return []

    g, nodes, _ = _build_digraph(graph)

    try:
        order = rx.topological_sort(g)
        return [nodes[idx] for idx in order]
    except rx.DAGHasCycle:
        return None
