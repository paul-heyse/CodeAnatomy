Style target reference (your attached PyArrow doc): 

# rustworkx (Python) — comprehensive feature catalog (clustered)

rustworkx is a **high-performance, general-purpose graph library for Python written in Rust**. Its docs summarize the package as providing graph data structures (directed/undirected + multigraphs), a library of standard graph algorithms, graph generators (incl. random graphs), and visualization functions. ([PyPI][1])

---

## 0) Mental model: what rustworkx “is”

### Core object model

* **Graphs are index-addressed.** Nodes (and edges) are identified by **integer indices**; indices are stable for the lifetime of the graph object, can become sparse after deletions, and may be reused on later insertions. ([rustworkx.org][2])
* **Payloads are Python objects.** Each node (and edges, when you supply them) carries an arbitrary Python object as its “weight/data payload.” ([rustworkx.org][2])
* **Graph-level metadata**: each graph can carry an `attrs` payload (any Python object). ([rustworkx.org][2])

### “One import” surface

* The project positions itself so that **graph classes + top-level functions** are accessible via `import rustworkx`. ([PyPI][1])

---

## 1) Graph classes and core data structures

Rustworkx’s API reference organizes the library into **Graph Classes**, **Algorithm Functions**, **Generators**, **Layout**, **Serialization**, **Converters**, **Exceptions**, **Custom Return Types**, and **Visualization**. ([rustworkx.org][3])

### 1.1 `rustworkx.PyGraph` (undirected; optional multigraph)

**Constructor**

* `PyGraph(multigraph=True, attrs=None, *, node_count_hint=None, edge_count_hint=None)` ([rustworkx.org][2])

**Key behaviors / knobs**

* **Multigraph toggle**: `multigraph=False` disallows parallel edges; attempts to add a parallel edge update the existing edge’s payload instead. ([rustworkx.org][2])
* **Node payload mapping protocol**: `graph[node_index]` gets/sets node payloads directly. ([rustworkx.org][2])
* **Capacity hints**: `node_count_hint` / `edge_count_hint` can preallocate capacity for performance. ([rustworkx.org][2])
* **Max size**: max nodes and edges are each `2^32 - 1`. ([rustworkx.org][2])

**Method surface area (clustered)**

* **Construction / mutation**

  * `add_node`, `add_nodes_from`
  * `add_edge`, `add_edges_from`, `add_edges_from_no_data`
  * `remove_node`, `remove_nodes_from`
  * `remove_edge`, `remove_edge_from_index`, `remove_edges_from`
  * `clear`, `clear_edges` ([rustworkx.org][2])
* **Adjacency / neighborhood**

  * `adj`, `neighbors` ([rustworkx.org][2])
* **Node/edge enumeration & counts**

  * `node_indices` / `node_indexes`, `nodes`, `num_nodes`
  * `edge_indices`, `edges`, `num_edges`
  * `edge_list`, `weighted_edge_list` ([rustworkx.org][2])
* **Edge inspection**

  * `get_edge_data`, `get_all_edge_data`, `get_edge_data_by_index`, `get_edge_endpoints_by_index`
  * `has_edge`, `has_parallel_edges` ([rustworkx.org][2])
* **Subgraphs / composition / rewrites**

  * `copy`, `compose`
  * `subgraph`, `subgraph_with_nodemap`, `edge_subgraph`
  * `contract_nodes`, `substitute_node_with_subgraph` ([rustworkx.org][2])
* **Conversion / IO**

  * `to_directed`, `to_dot`
  * `extend_from_edge_list`, `extend_from_weighted_edge_list`
  * `read_edge_list`, `write_edge_list`
  * `from_adjacency_matrix`, `from_complex_adjacency_matrix` ([rustworkx.org][2])
* **Filtering / search**

  * `filter_nodes`, `filter_edges`, `find_node_by_weight` ([rustworkx.org][2])
* **In/Out edge APIs**

  * `in_edge_indices`, `in_edges`, `out_edge_indices`, `out_edges`
  * `incident_edges`, `incident_edge_index_map`, `edge_index_map` ([rustworkx.org][2])
* **Update**

  * `update_edge`, `update_edge_by_index` ([rustworkx.org][2])

---

### 1.2 `rustworkx.PyDiGraph` (directed; optional multigraph; optional cycle checking)

**Constructor**

* `PyDiGraph(check_cycle=False, multigraph=True, attrs=None, *, node_count_hint=None, edge_count_hint=None)` ([rustworkx.org][4])

**Key behaviors / knobs**

* Everything from `PyGraph` applies (integer indices, node mapping protocol, `attrs`, multigraph toggle). ([rustworkx.org][4])
* **Runtime cycle checking**:

  * Enable via `PyDiGraph(check_cycle=True)` or `graph.check_cycle = True`.
  * When enabled, edge-add operations ensure you don’t introduce cycles; this adds overhead (grows with graph size), and the docs recommend `add_child()` / `add_parent()` when adding a node+edge together to avoid some overhead. ([rustworkx.org][4])

**Method surface area (clustered)**

* **Directed construction primitives**

  * `add_child`, `add_parent` (node+edge in one step) ([rustworkx.org][4])
* **Direction-aware adjacency**

  * `adj_direction` (parents vs children), plus `adj` ([rustworkx.org][4])
* **Predecessor/successor APIs**

  * `predecessor_indices`, `predecessors`
  * `successor_indices`, `successors` ([rustworkx.org][4])
* **Degree / edge direction**

  * `in_degree`, `out_degree`
  * `in_edge_indices`, `out_edge_indices`
  * `in_edges`, `out_edges` ([rustworkx.org][4])
* **Graph rewrites that matter for DAG-ish work**

  * `remove_node_retain_edges`, `remove_node_retain_edges_by_id`, `remove_node_retain_edges_by_key`
  * `insert_node_on_in_edges`, `insert_node_on_in_edges_multiple`
  * `insert_node_on_out_edges`, `insert_node_on_out_edges_multiple`
  * `merge_nodes` ([rustworkx.org][4])
* **Symmetry helpers**

  * `is_symmetric`, `make_symmetric`, plus `neighbors_undirected` for direction-agnostic neighbor queries ([rustworkx.org][4])
* **Reversal**

  * `reverse` (in-place edge reversal), `to_undirected` (convert) ([rustworkx.org][4])
* **Cycle-safe contraction helper**

  * `can_contract_without_cycle` ([rustworkx.org][4])

---

### 1.3 `rustworkx.PyDAG` (DAG convenience / backward-compat alias)

* `PyDAG` is a subclass of `PyDiGraph` and **behaves identically**; it exists as a **backwards compatibility alias** (from before `PyDiGraph` existed). ([rustworkx.org][5])
* It keeps the same `check_cycle` option and method surface. ([rustworkx.org][5])

---

## 2) Universal algorithm functions (graph-type agnostic)

The Rustworkx API groups “Algorithm Functions” into these clusters and lists the public entry points below. ([rustworkx.org][6])

### 2.1 Centrality

* `betweenness_centrality`
* `degree_centrality`
* `edge_betweenness_centrality`
* `eigenvector_centrality`
* `katz_centrality`
* `closeness_centrality`
* `newman_weighted_closeness_centrality`
* `in_degree_centrality`, `out_degree_centrality` ([rustworkx.org][6])

### 2.2 Coloring

* `ColoringStrategy`
* `graph_greedy_color`
* `graph_bipartite_edge_color`
* `graph_greedy_edge_color`
* `graph_misra_gries_edge_color`
* `two_color` ([rustworkx.org][6])

### 2.3 Connectivity and cycles

* Connectedness:

  * `number_connected_components`, `connected_components`, `node_connected_component`, `is_connected` ([rustworkx.org][6])
* Strong/weak connectivity (directed):

  * `number_strongly_connected_components`, `strongly_connected_components`, `is_strongly_connected`
  * `number_weakly_connected_components`, `weakly_connected_components`, `is_weakly_connected` ([rustworkx.org][6])
* Cycles / paths / structure:

  * `cycle_basis`, `simple_cycles`, `digraph_find_cycle`
  * `articulation_points`, `bridges`, `biconnected_components`, `chain_decomposition`
  * `all_simple_paths`, `all_pairs_all_simple_paths`, `has_path`, `connected_subgraphs`
  * Cuts / longest path: `stoer_wagner_min_cut`, `longest_simple_path`
  * Bipartite helpers: `is_bipartite`, `isolates` ([rustworkx.org][6])

### 2.4 DAG algorithms

* Longest paths:

  * `dag_longest_path`, `dag_longest_path_length`
  * `dag_weighted_longest_path`, `dag_weighted_longest_path_length`
* DAG checks / transforms:

  * `is_directed_acyclic_graph`, `transitive_reduction`
* Layering / ordering helpers:

  * `layers`, `topological_generations` ([rustworkx.org][6])

### 2.5 Dominance

* `immediate_dominators`
* `dominance_frontiers` ([rustworkx.org][6])

### 2.6 Graph operations

* `complement`
* `union`
* `cartesian_product` ([rustworkx.org][6])

### 2.7 Isomorphism

* `is_isomorphic`
* `is_subgraph_isomorphic`
* `is_isomorphic_node_match`
* `vf2_mapping` ([rustworkx.org][6])

### 2.8 Link analysis

* `pagerank`
* `hits` ([rustworkx.org][6])

### 2.9 Matching

* `max_weight_matching`
* `is_matching`
* `is_maximal_matching` ([rustworkx.org][6])

### 2.10 Other algorithms

* `adjacency_matrix`
* `transitivity`
* `core_number`
* `graph_line_graph`
* `metric_closure`
* `is_planar`
* `digraph_maximum_bisimulation` ([rustworkx.org][6])

### 2.11 Shortest paths

* Dijkstra:

  * `dijkstra_shortest_paths`, `dijkstra_shortest_path_lengths`
  * `all_pairs_dijkstra_shortest_paths`, `all_pairs_dijkstra_path_lengths`
* Bellman–Ford:

  * `bellman_ford_shortest_paths`, `bellman_ford_shortest_path_lengths`
  * `all_pairs_bellman_ford_shortest_paths`, `all_pairs_bellman_ford_path_lengths`
* Negative cycles:

  * `negative_edge_cycle`, `find_negative_cycle`
* Floyd–Warshall:

  * `distance_matrix`, `floyd_warshall`, `floyd_warshall_numpy`, `floyd_warshall_successor_and_distance`
* A*/k-shortest:

  * `astar_shortest_path`, `k_shortest_path_lengths`
* Counting / averaging / enumeration:

  * `num_shortest_paths_unweighted`
  * `unweighted_average_shortest_path_length`
  * `all_shortest_paths`, `digraph_all_shortest_paths`, `single_source_all_shortest_paths` ([rustworkx.org][6])

### 2.12 Traversal (plus visitor hooks)

* Traversal primitives:

  * `dfs_edges`, `dfs_search`
  * `bfs_successors`, `bfs_predecessors`, `bfs_search`
  * `dijkstra_search`
* Topological / ancestry:

  * `topological_sort`, `lexicographical_topological_sort`
  * `descendants`, `ancestors`
* Run detection helpers:

  * `collect_runs`, `collect_bicolor_runs`
* Visitor classes:

  * `DFSVisitor`, `BFSVisitor`, `DijkstraVisitor`
  * `TopologicalSorter` (with `done`, `get_ready`, `is_active`) ([rustworkx.org][6])

### 2.13 Tree algorithms

* `minimum_spanning_edges`
* `minimum_spanning_tree`
* `steiner_tree` ([rustworkx.org][6])

---

## 3) Graph generation (deterministic + random)

### 3.1 Deterministic generators (`rustworkx.generators.*`)

* `cycle_graph`, `directed_cycle_graph`
* `path_graph`, `directed_path_graph`
* `star_graph`, `directed_star_graph`
* `mesh_graph`, `directed_mesh_graph`
* `grid_graph`, `directed_grid_graph`
* `binomial_tree_graph`, `directed_binomial_tree_graph`
* `hexagonal_lattice_graph`, `directed_hexagonal_lattice_graph`
* `heavy_square_graph`, `directed_heavy_square_graph`
* `heavy_hex_graph`, `directed_heavy_hex_graph`
* `lollipop_graph`
* `generalized_petersen_graph`
* `barbell_graph`
* `full_rary_tree`
* `empty_graph`, `directed_empty_graph`
* `complete_graph`, `directed_complete_graph`
* `dorogovtsev_goltsev_mendes_graph`
* `karate_club_graph` ([rustworkx.org][3])

### 3.2 Random graph generators

* Erdős–Rényi:

  * `directed_gnp_random_graph`, `undirected_gnp_random_graph`
  * `directed_gnm_random_graph`, `undirected_gnm_random_graph`
* Stochastic block model:

  * `directed_sbm_random_graph`, `undirected_sbm_random_graph`
* Geometric/hyperbolic:

  * `random_geometric_graph`, `hyperbolic_random_graph`
* Preferential attachment:

  * `barabasi_albert_graph`, `directed_barabasi_albert_graph`
* Random bipartite:

  * `directed_random_bipartite_graph`, `undirected_random_bipartite_graph` ([rustworkx.org][3])

---

## 4) Layout + visualization

### 4.1 Layout functions

* `random_layout`
* `spring_layout`
* `bipartite_layout`
* `circular_layout`
* `shell_layout`
* `spiral_layout` ([rustworkx.org][3])

### 4.2 Visualization module

* `rustworkx.visualization.mpl_draw`
* `rustworkx.visualization.graphviz_draw` ([rustworkx.org][6])

### 4.3 DOT export (graph methods)

* `PyGraph.to_dot()`, `PyDiGraph.to_dot()`, `PyDAG.to_dot()` ([rustworkx.org][2])

---

## 5) Serialization + interchange

### 5.1 Node-link JSON (in-memory + parse/read helpers)

* `node_link_json`
* `parse_node_link_json`
* `from_node_link_json_file` ([rustworkx.org][3])

Type-specific variants also exist:

* `digraph_node_link_json`
* `graph_node_link_json` ([rustworkx.org][6])

### 5.2 GraphML

* `read_graphml`
* `write_graphml` ([rustworkx.org][3])

---

## 6) Converters (NetworkX bridge)

* `networkx_converter` (converter surface for NetworkX interop) ([rustworkx.org][3])

*(If your goal is “ingest a NetworkX graph then run rustworkx algorithms,” this is the official bridge point.)* ([rustworkx.org][3])

---

## 7) Type-specific algorithm functions (`graph_*` and `digraph_*`)

rustworkx provides **type-specific algorithm functions** for `PyGraph` and for `PyDiGraph`/`PyDAG`. The docs explicitly note that **universal functions dispatch internally to these typed APIs based on graph type.** ([rustworkx.org][7])

### 7.1 PyDiGraph / PyDAG typed API (`digraph_*`)

* Isomorphism: `digraph_is_isomorphic`, `digraph_is_subgraph_isomorphic`, `digraph_vf2_mapping`
* Shortest paths / distance matrices: `digraph_distance_matrix`, `digraph_floyd_warshall`, `digraph_floyd_warshall_numpy`, `digraph_floyd_warshall_successor_and_distance`, `digraph_astar_shortest_path`, `digraph_dijkstra_shortest_paths`, `digraph_all_pairs_dijkstra_shortest_paths`, `digraph_dijkstra_shortest_path_lengths`, `digraph_all_pairs_dijkstra_path_lengths`, `digraph_bellman_ford_shortest_path_lengths`, `digraph_all_pairs_bellman_ford_shortest_paths`, `digraph_all_pairs_bellman_ford_path_lengths`, `digraph_k_shortest_path_lengths`, `digraph_all_shortest_paths`, `digraph_single_source_all_shortest_paths`
* Matrices / paths: `digraph_adjacency_matrix`, `digraph_all_simple_paths`, `digraph_all_pairs_all_simple_paths`
* Traversal: `digraph_dfs_edges`, `digraph_dfs_search`, `digraph_bfs_search`, `digraph_dijkstra_search`, `digraph_find_cycle`
* Graph measures/ops/layout: `digraph_transitivity`, `digraph_core_number`, `digraph_complement`, `digraph_union`, `digraph_tensor_product`, `digraph_cartesian_product`, `digraph_num_shortest_paths_unweighted`, `digraph_unweighted_average_shortest_path_length`, `digraph_longest_simple_path`
* Centralities/layout/json: `digraph_betweenness_centrality`, `digraph_edge_betweenness_centrality`, `digraph_closeness_centrality`, `digraph_newman_weighted_closeness_centrality`, `digraph_eigenvector_centrality`, `digraph_katz_centrality`, `digraph_random_layout`, `digraph_bipartite_layout`, `digraph_circular_layout`, `digraph_shell_layout`, `digraph_spiral_layout`, `digraph_spring_layout`, `digraph_node_link_json` ([rustworkx.org][3])

### 7.2 PyGraph typed API (`graph_*`)

* Isomorphism: `graph_is_isomorphic`, `graph_is_subgraph_isomorphic`, `graph_vf2_mapping`
* Shortest paths / distances: `graph_distance_matrix`, `graph_floyd_warshall`, `graph_floyd_warshall_numpy`, `graph_floyd_warshall_successor_and_distance`, `graph_astar_shortest_path`, `graph_dijkstra_shortest_paths`, `graph_dijkstra_shortest_path_lengths`, `graph_all_pairs_dijkstra_shortest_paths`, `graph_all_pairs_dijkstra_path_lengths`, `graph_bellman_ford_shortest_path_lengths`, `graph_all_pairs_bellman_ford_shortest_paths`, `graph_all_pairs_bellman_ford_path_lengths`, `graph_k_shortest_path_lengths`, `graph_all_shortest_paths`, `graph_single_source_all_shortest_paths`
* Matrices / paths / traversal: `graph_adjacency_matrix`, `graph_all_simple_paths`, `graph_all_pairs_all_simple_paths`, `graph_dfs_edges`, `graph_dfs_search`, `graph_bfs_search`, `graph_dijkstra_search`
* Measures/ops/layout: `graph_transitivity`, `graph_core_number`, `graph_complement`, `local_complement`, `graph_union`, `graph_tensor_product`, `graph_cartesian_product`, `graph_num_shortest_paths_unweighted`, `graph_unweighted_average_shortest_path_length`, `graph_longest_simple_path`
* Extra algorithm: `graph_token_swapper`
* Centralities/layout/json: `graph_betweenness_centrality`, `graph_edge_betweenness_centrality`, `graph_closeness_centrality`, `graph_newman_weighted_closeness_centrality`, `graph_eigenvector_centrality`, `graph_katz_centrality`, `graph_random_layout`, `graph_bipartite_layout`, `graph_circular_layout`, `graph_shell_layout`, `graph_spiral_layout`, `graph_spring_layout`, `graph_node_link_json` ([rustworkx.org][3])

---

## 8) Exceptions (error model)

* `DAGHasCycle`, `DAGWouldCycle`
* `FailedToConverge`
* `GraphNotBipartite`
* `InvalidMapping`, `InvalidNode`
* `JSONDeserializationError`, `JSONSerializationError`
* `NegativeCycle`
* `NoEdgeBetweenNodes`, `NoPathFound`, `NoSuitableNeighbors`
* `NullGraph`
* Traversal control exceptions: `rustworkx.visit.PruneSearch`, `rustworkx.visit.StopSearch` ([rustworkx.org][6])

---

## 9) Custom return types (structured outputs)

Rustworkx defines a set of **custom container/Mapping return types** for performance + ergonomic typed returns:

* `BFSSuccessors`, `BFSPredecessors`
* `NodeIndices`, `EdgeIndices`
* `EdgeList`, `WeightedEdgeList`
* Mapping-like types:

  * `EdgeIndexMap`, `PathMapping`, `PathLengthMapping`, `Pos2DMapping`
  * `AllPairsPathMapping`, `AllPairsPathLengthMapping`
  * `CentralityMapping`, `EdgeCentralityMapping`
  * `NodeMap`, `ProductNodeMap`
  * `BiconnectedComponents`
* Other structured types:

  * `Chains`, `RelationalCoarsestPartition`, `IndexPartitionBlock` ([rustworkx.org][6])

---

## 10) Versioning / compatibility notes (practical)

* rustworkx release notes indicate the **minimum supported Python version is Python ≥ 3.9** (and Python 3.8 is no longer supported). ([rustworkx.org][8])

---


[1]: https://pypi.org/project/rustworkx/?utm_source=chatgpt.com "rustworkx"
[2]: https://www.rustworkx.org/apiref/rustworkx.PyGraph.html "PyGraph - rustworkx 0.17.1"
[3]: https://www.rustworkx.org/api/index.html "Rustworkx API Reference - rustworkx 0.17.1"
[4]: https://www.rustworkx.org/apiref/rustworkx.PyDiGraph.html "PyDiGraph - rustworkx 0.17.1"
[5]: https://www.rustworkx.org/apiref/rustworkx.PyDAG.html "PyDAG - rustworkx 0.17.1"
[6]: https://www.rustworkx.org/api/algorithm_functions/index.html "Algorithm Functions - rustworkx 0.17.1"
[7]: https://www.rustworkx.org/api/pydigraph_api_functions.html "API functions for PyDigraph - rustworkx 0.17.1"
[8]: https://www.rustworkx.org/release_notes.html?utm_source=chatgpt.com "Release Notes - rustworkx 0.17.1"

# Deep dive — Graph classes & core data structures (PyGraph / PyDiGraph / PyDAG)

This is the “how the objects actually behave” layer: identity, payload semantics, mutability rules, and the performance/footgun knobs that matter once you’re building serious graphs (like a CPG).

---

## 1) The shared data model (applies to PyGraph, PyDiGraph, PyDAG)

### 1.1 Nodes + edges are *index-addressed* handles (and indices can be reused)

* Every node/edge has an integer **index** that uniquely identifies it **while it exists** in the graph. Indices can become **non-contiguous** after deletions (“holes”), and **an index can be reused after a removal**. ([Rustworkx][1])
* This is a big philosophical difference vs NetworkX: you don’t “address a node by its object” — you address it by index. ([Rustworkx][2])

**Practical implication:** treat indices as *ephemeral handles*, not stable IDs. If you need stable identity, you supply it yourself (more on this in “Footguns & best practices”).

### 1.2 Payloads (“weights”) are arbitrary Python objects (nodes *and* edges)

* Nodes and edges carry a Python “data payload/weight” (any Python object, including unhashable types). ([Rustworkx][1])

### 1.3 Node payloads use the Python mapping protocol (`graph[idx]`)

* You can read/update a node’s payload via `graph[node_index]` and assignment. ([Rustworkx][3])

### 1.4 `attrs`: graph-level metadata payload

* Each graph has an `.attrs` field that can be any Python object; set at construction or later. ([Rustworkx][3])

### 1.5 Capacity + size limits

* **Max nodes** and **max edges** are each `2^32 - 1`. ([Rustworkx][3])
* `node_count_hint` / `edge_count_hint` let you preallocate capacity (performance only; does not create nodes/edges). ([Rustworkx][3])

### 1.6 “Batch add” returns special sequence types

* `add_nodes_from(...)` returns a `NodeIndices` object (a custom sequence type). ([Rustworkx][1])
* In current rustworkx, bulk edge adds return `EdgeIndices` (a custom sequence type). In earlier versions, some returned lists; release notes explicitly call out the change. ([Rustworkx][4])

---

## 2) `PyGraph` (undirected; optional multigraph)

### 2.1 What it is

`PyGraph` is an **undirected** graph. It can be a **multigraph** (parallel edges) unless you disable that at construction time. ([Rustworkx][3])

### 2.2 Constructor knobs that actually matter

```python
import rustworkx as rx
g = rx.PyGraph(
    multigraph=True,          # default
    attrs={"source": "…"},     # optional graph-level payload
    node_count_hint=100_000,   # perf hint
    edge_count_hint=500_000,   # perf hint
)
```

* `multigraph` can **only** be set at initialization; you can’t toggle it later. ([Rustworkx][3])
* If `multigraph=False`, **attempts to add a parallel edge update the existing edge’s payload instead of creating a new edge**. ([Rustworkx][3])

### 2.3 Node lifecycle

* Add single: `add_node(payload) -> node_index`
* Add batch: `add_nodes_from(iterable) -> NodeIndices`
* Remove: `remove_node(idx)` and `remove_nodes_from([...])`
* Clear: `clear()` clears everything; `clear_edges()` clears edges only ([Rustworkx][3])

**Identity footgun reminder:** deleting can create holes, and later additions can reuse indices. ([Rustworkx][3])

### 2.4 Edge lifecycle + multigraph semantics (this is the big one)

**Single edge**

* `add_edge(node_a, node_b, edge_payload) -> edge_index`
* If `multigraph=False` and an edge already exists between the endpoints, the existing edge payload is updated. ([Rustworkx][5])

**Batch edges**

* `add_edges_from([(a, b, payload), ...]) -> EdgeIndices`
* With `multigraph=False`, existing edges are updated **in order**, so if you pass multiple parallels, “last one wins.” ([Rustworkx][6])

**No-data edges**

* `add_edges_from_no_data([(a, b), ...])` creates edges whose payload is `None`. ([Rustworkx][7])
* With `multigraph=False`, adding an existing edge will update its payload to `None`. ([Rustworkx][7])

**Why you should care:** in “simple graph mode” (`multigraph=False`), `add_edge(s)` is *also* an **implicit update** API. That’s powerful, but it’s a drift risk if callers assume “add means append”.

### 2.5 Edge indexing & enumeration

* Edges have integer indices too; `edge_indices()` returns an `EdgeIndices` sequence. ([Rustworkx][8])
* Bulk edge add returns `EdgeIndices` (not a plain list in current versions). ([Rustworkx][6])

### 2.6 Fast paths + performance knobs in the data structure layer

* Prefer `add_nodes_from` / `add_edges_from` over repeated single inserts for large builds (fewer Python↔Rust crossings).
* Use `node_count_hint`/`edge_count_hint` when you know sizes. ([Rustworkx][3])
* If edge payloads are irrelevant, use `add_edges_from_no_data` to avoid shipping per-edge Python objects. ([Rustworkx][7])

### 2.7 “Graph surgery” primitives you’ll want for compaction / rewrites

The class exposes higher-level structural operations like:

* `copy()`, `compose(other)`
* `subgraph(...)`, `edge_subgraph(...)`
* `contract_nodes(...)`
* `substitute_node_with_subgraph(...)` ([Rustworkx][3])

(These are the building blocks for things like “collapse all nodes in a component into a supernode”, or “replace a node with a subgraph during normalization”.)

---

## 3) `PyDiGraph` (directed; optional multigraph; optional runtime cycle checking)

### 3.1 What it is

`PyDiGraph` is a **directed** graph, also a multigraph by default. Like `PyGraph`, nodes/edges are indexed and payload-backed. ([Rustworkx][9])

### 3.2 Constructor knobs

```python
import rustworkx as rx
dg = rx.PyDiGraph(
    check_cycle=False,        # default; enable to enforce DAG property while mutating
    multigraph=True,
    attrs={"run_id": "..."},
    node_count_hint=100_000,
    edge_count_hint=500_000,
)
```

#### `check_cycle`: enforcing DAG-ness *during mutation*

* With `check_cycle=True`, adding edges that would introduce a cycle raises `DAGWouldCycle`. ([Rustworkx][10])
* rustworkx explicitly warns that this has **noticeable runtime overhead** and grows with graph size. ([Rustworkx][11])
* The docs recommend `add_child()` / `add_parent()` when adding a node+edge together (these avoid the cycle-check overhead path). ([Rustworkx][11])

### 3.3 Directed edge APIs + batch/no-data variants

* `add_edge(parent, child, payload)` exists and supports multigraph behavior (parallel edges when multigraph is enabled). ([Rustworkx][12])
* Batch: `add_edges_from([(parent, child, payload), ...]) -> EdgeIndices`. ([Rustworkx][13])
* No-data: `add_edges_from_no_data([(parent, child), ...]) -> EdgeIndices`, with payload `None`. ([Rustworkx][14])

### 3.4 Update semantics with parallel edges (important in multigraph mode)

* `update_edge(source, target, payload)` updates an edge’s payload in place, but **if there are parallel edges only one is updated**; use `update_edge_by_index()` if you need a specific parallel edge or all parallels. ([Rustworkx][15])

This matters a lot for “semantic edges” in a CPG (e.g., multiple reasons for a call relationship). In multigraph mode, you generally want to update by edge index, not by endpoints.

### 3.5 Graph surgery methods that are *especially* valuable for DAG pipelines

Even in “core data structures”, `PyDiGraph` exposes DAG-ish rewrites such as:

* `remove_node_retain_edges*` variants
* `insert_node_on_in_edges*`, `insert_node_on_out_edges*`
* `merge_nodes`
* `can_contract_without_cycle` (useful when `check_cycle` is in play) ([Rustworkx][3])

These are the operations you reach for when you need to normalize a graph without losing reachability structure (e.g., inserting an explicit “phi” node, or collapsing helper nodes).

---

## 4) `PyDAG` (backwards-compatible alias, same underlying behavior)

* `PyDAG` was renamed to `PyDiGraph`; it **still exists as a Python subclass of `PyDiGraph` for backwards compatibility**. ([Rustworkx][16])
* It supports the same `check_cycle` behavior and the same multigraph semantics. ([Rustworkx][17])

**Rule of thumb:** use `PyDiGraph` in new code, treat `PyDAG` as a compatibility surface.

---

## 5) Footguns & best practices (core-structure edition)

### 5.1 Never treat node indices as “stable IDs”

Because indices can be reused after deletion, any of these patterns can silently corrupt meaning:

* storing `node_index` in a database and later reloading it against a mutated graph
* using `node_index` as a long-lived key across transformations

**Best practice:** create your own stable ID and keep a mapping:

```python
from dataclasses import dataclass

@dataclass(frozen=True)
class NodePayload:
    stable_id: str   # your canonical ID (e.g., scip_symbol, file+span, etc.)
    kind: str
    data: dict

stable_to_idx: dict[str, int] = {}

idx = g.add_node(NodePayload(stable_id="sym:foo", kind="func", data={}))
stable_to_idx["sym:foo"] = idx
```

Then: if you ever delete nodes, you *must* update that mapping.

### 5.2 Decide multigraph vs simple graph up front (and encode “meaning” accordingly)

* If you need **multiple distinct relations between the same endpoints**, you want `multigraph=True`.
* If you want “there can be only one edge” semantics, `multigraph=False` gives you a very convenient “add acts like upsert” behavior. ([Rustworkx][3])

### 5.3 If you use multigraphs, prefer edge-index addressing for updates

Because endpoint-addressed updates can touch only one of multiple parallels, update-by-index is safer for correctness-sensitive edges. ([Rustworkx][15])

### 5.4 `check_cycle=True` is a correctness guardrail, not a default

It’s explicitly described as incurring runtime overhead that can get large; only enable it when you truly need “reject cycles at insertion time.” ([Rustworkx][18])

---

## 6) Minimal “truth-table” snippets (these are the behaviors worth memorizing)

### 6.1 Index reuse + node mapping protocol

```python
import rustworkx as rx

g = rx.PyGraph()
a = g.add_node({"name": "A"})
b = g.add_node({"name": "B"})
g.remove_node(a)

# indices can be reused after deletion
c = g.add_node({"name": "C"})

# node payload mapping protocol
g[c] = {"name": "C2"}
assert g[c]["name"] == "C2"
```

(Index reuse + mapping protocol are explicitly documented.) ([Rustworkx][3])

### 6.2 `multigraph=False` turns “add edge” into “upsert edge”

```python
import rustworkx as rx

g = rx.PyGraph(multigraph=False)
n0 = g.add_node("n0")
n1 = g.add_node("n1")

g.add_edge(n0, n1, "A")
g.add_edge(n0, n1, "B")  # updates existing payload instead of adding parallel
```

(That “update instead of parallel” rule is in the docs for `multigraph=False` / `add_edge` / `add_edges_from`.) ([Rustworkx][5])

### 6.3 No-data batch edge adds use `None`

```python
import rustworkx as rx

g = rx.PyGraph()
g.add_nodes_from(range(3))
eidxs = g.add_edges_from_no_data([(0, 1), (1, 2)])
```

(`add_edges_from_no_data` creates edges with payload `None`, returns `EdgeIndices`.) ([Rustworkx][7])

### 6.4 `check_cycle=True` raises `DAGWouldCycle`

```python
import rustworkx as rx

dg = rx.PyDiGraph(check_cycle=True)
a = dg.add_node("A")
b = dg.add_node("B")
dg.add_edge(a, b, None)

# This would introduce a cycle:
# dg.add_edge(b, a, None)  -> raises rustworkx.DAGWouldCycle
```

(`check_cycle` → cycle introduction raises `DAGWouldCycle`, and has overhead.) ([Rustworkx][10])

---

## 7) Contract-style micro-tests (pytest) for this section

If you want to pin these semantics (so upgrades can’t silently drift), these 5 tests give you high value:

```python
import pytest
import rustworkx as rx

def test_index_reuse_is_possible():
    g = rx.PyGraph()
    g.add_nodes_from([1, 2, 3])
    g.remove_node(1)
    new_idx = g.add_node("x")
    assert new_idx == 1  # reuse is allowed (documented)

def test_multigraph_false_updates_payload_instead_of_parallel():
    g = rx.PyGraph(multigraph=False)
    a = g.add_node("a"); b = g.add_node("b")
    g.add_edge(a, b, "A")
    g.add_edge(a, b, "B")
    assert len(list(g.edge_indices())) == 1

def test_add_edges_from_order_last_wins_in_simple_graph():
    g = rx.PyGraph(multigraph=False)
    g.add_nodes_from([0, 1])
    g.add_edges_from([(0, 1, "A"), (0, 1, "B")])
    assert g.get_edge_data(0, 1) == "B"

def test_add_edges_from_no_data_payload_is_none():
    g = rx.PyDiGraph()
    g.add_nodes_from([0, 1])
    eidxs = g.add_edges_from_no_data([(0, 1)])
    assert list(eidxs)  # EdgeIndices is iterable
    assert g.get_edge_data(0, 1) is None

def test_update_edge_in_multigraph_updates_only_one_parallel():
    g = rx.PyDiGraph(multigraph=True)
    g.add_nodes_from([0, 1])
    g.add_edges_from([(0, 1, "A"), (0, 1, "B")])
    g.update_edge(0, 1, "X")
    # not asserting which one updates—only that you can't rely on it updating all parallels
```

The assertions above are all grounded in documented behaviors: index reuse, simple-graph update semantics, “last wins” ordering for batch adds, “None payload” for no-data adds, and update_edge’s “only one parallel edge” behavior. ([Rustworkx][3])

---

If you want the next deep dive after this, the most natural follow-on is **“Algorithm calls as typed dispatch + callback-weight extraction”** (because rustworkx’s biggest “feel” difference vs NetworkX is: *indices + callback extractors + typed return objects*).

[1]: https://www.rustworkx.org/tutorial/introduction.html "Introduction to rustworkx - rustworkx 0.17.1"
[2]: https://www.rustworkx.org/networkx.html?utm_source=chatgpt.com "rustworkx for NetworkX users"
[3]: https://www.rustworkx.org/apiref/rustworkx.PyGraph.html "PyGraph - rustworkx 0.17.1"
[4]: https://www.rustworkx.org/release_notes.html?utm_source=chatgpt.com "Release Notes - rustworkx 0.17.1"
[5]: https://www.rustworkx.org/apiref/rustworkx.PyGraph.add_edge.html?utm_source=chatgpt.com "rustworkx.PyGraph.add_edge"
[6]: https://www.rustworkx.org/apiref/rustworkx.PyGraph.add_edges_from.html?utm_source=chatgpt.com "rustworkx.PyGraph.add_edges_from"
[7]: https://www.rustworkx.org/apiref/rustworkx.PyGraph.add_edges_from_no_data.html?utm_source=chatgpt.com "rustworkx.PyGraph.add_edges_from_no_data"
[8]: https://www.rustworkx.org/apiref/rustworkx.PyGraph.edge_indices.html?utm_source=chatgpt.com "rustworkx.PyGraph.edge_indices"
[9]: https://www.rustworkx.org/apiref/rustworkx.PyDiGraph.html?utm_source=chatgpt.com "PyDiGraph - rustworkx 0.17.1"
[10]: https://www.rustworkx.org/apiref/rustworkx.PyDAG.html?utm_source=chatgpt.com "PyDAG - rustworkx 0.17.1"
[11]: https://www.rustworkx.org/apiref/rustworkx.PyDiGraph.html "PyDiGraph - rustworkx 0.17.1"
[12]: https://www.rustworkx.org/apiref/rustworkx.PyDiGraph.add_edge.html?utm_source=chatgpt.com "rustworkx.PyDiGraph.add_edge"
[13]: https://www.rustworkx.org/apiref/rustworkx.PyDiGraph.add_edges_from.html?utm_source=chatgpt.com "rustworkx.PyDiGraph.add_edges_from"
[14]: https://www.rustworkx.org/apiref/rustworkx.PyDiGraph.add_edges_from_no_data.html?utm_source=chatgpt.com "rustworkx.PyDiGraph.add_edges_from_no_data"
[15]: https://www.rustworkx.org/apiref/rustworkx.PyDiGraph.update_edge.html "rustworkx.PyDiGraph.update_edge - rustworkx 0.17.1"
[16]: https://www.rustworkx.org/release_notes.html "Release Notes - rustworkx 0.17.1"
[17]: https://www.rustworkx.org/apiref/rustworkx.PyDAG.html "PyDAG - rustworkx 0.17.1"
[18]: https://www.rustworkx.org/tutorial/dags.html?utm_source=chatgpt.com "Directed Acyclic Graphs"

# Deep dive — Algorithm calls: typed dispatch + callback-weight extraction

This is the “why rustworkx algorithms feel different from NetworkX” layer: **universal functions dispatching to typed implementations**, plus the library’s pervasive pattern of **“you own the payload → provide a callback to extract numeric meaning.”**

---

## 1) Universal vs typed algorithms: what “dispatch” actually means

### 1.1 Universal functions are thin wrappers

Most top-level algorithms (e.g., `rustworkx.dijkstra_shortest_paths`, `rustworkx.adjacency_matrix`, etc.) are decorated with an internal dispatcher that chooses the correct typed implementation based on the runtime graph type. The dispatcher is implemented with `functools.singledispatch`, registering:

* `PyDiGraph` → `digraph_{func_name}`
* `PyGraph` → `graph_{func_name}` ([Rustworkx][1])

So when you call:

```python
import rustworkx as rx
rx.dijkstra_shortest_paths(graph, ...)
```

…it effectively routes to either `rx.graph_dijkstra_shortest_paths(...)` or `rx.digraph_dijkstra_shortest_paths(...)` depending on whether `graph` is a `PyGraph` or `PyDiGraph`. ([Rustworkx][1])

### 1.2 Why typed functions exist (and when you should use them)

The docs explicitly present “typed algorithm functions” pages for each graph type and note universal functions internally call these typed functions. ([Rustworkx][2])

Use **universal functions** when:

* you accept either graph type and want a single call surface

Use **typed functions** when:

* you want to *guarantee* the graph type at the callsite (and fail early if someone passes the wrong kind)
* you’re building an internal “metrics library” and want explicit signatures per graph type

---

## 2) The core pattern: “payload is arbitrary → you provide a numeric extractor”

Because nodes/edges can hold **any** Python object, algorithms that need numeric weights can’t assume the payload is a float. So rustworkx typically asks for a callback that converts the payload to a `float`.

There are two common parameter names:

* `weight_fn`: optional; if absent you can supply `default_weight`
* `edge_cost_fn`: required in many typed APIs (same idea; “this edge’s cost/weight as float”)

### 2.1 Dijkstra (paths) — `weight_fn(edge_payload) -> float`

Both `graph_dijkstra_shortest_paths` and `digraph_dijkstra_shortest_paths` take:

* `weight_fn`: “accept a single argument, the edge’s weight object” and return a float cost
* `default_weight`: used if `weight_fn` is not provided
* `as_undirected`: treat directed graph as undirected when searching ([Rustworkx][3])

They also document raising `ValueError` for NaN or negative weights. ([Rustworkx][3])

### 2.2 Dijkstra (lengths) — typed API forces `edge_cost_fn`

For example `graph_dijkstra_shortest_path_lengths(graph, node, edge_cost_fn, /, goal=None)`:

* requires `edge_cost_fn(edge_payload) -> float` (must be non-negative)
* returns a `PathLengthMapping` (a custom dict-like return type) ([Rustworkx][4])

This “required callback” style is common in typed shortest-path APIs.

---

## 3) Canonical weight-extractor strategies (and how to keep them fast)

### Strategy A: payload is already numeric

Make the edge payload a float, and pass `weight_fn=float`:

```python
rx.graph_dijkstra_shortest_paths(g, source, weight_fn=float)
```

This is the lowest-friction pattern and matches docs’ own examples (cast edge object to float). ([Rustworkx][5])

### Strategy B: payload is a small struct (dict / dataclass)

Example: edge payload is a dict like `{"cost": 3.2, "kind": "call"}`

```python
def cost(edge):
    return float(edge["cost"])
```

### Strategy C: enforce “policy” in the extractor (recommended for bigger systems)

Centralize semantics + validation in one place:

```python
import math

def safe_cost(edge) -> float:
    w = float(edge["cost"])
    if math.isnan(w) or w < 0:
        raise ValueError("invalid edge cost")
    return w
```

This aligns with rustworkx’s own contracts that negative/NaN costs raise `ValueError` for Dijkstra-family functions. ([Rustworkx][3])

---

## 4) Multigraph semantics: “parallel edges” often get *combined*

This is where callback extraction and multigraphs intersect:

### 4.1 Adjacency matrix sums parallel-edge weights

`adjacency_matrix(...)` explicitly says: with multiple edges between nodes, the output matrix entry is **the sum of the edges’ weights** (after applying `weight_fn` / default). ([Rustworkx][1])

### 4.2 PageRank sums parallel-edge weights

`pagerank(...)` states that in multigraphs, weights of parallel edges are **summed** when computing PageRank; it also supports `weight_fn(edge_payload)->float` with `default_weight` fallback. ([Rustworkx][6])

**Design consequence:** if you model multiple “reasons” as parallel edges, you may accidentally inflate weight-driven algorithms unless you explicitly normalize (either by payload choice or by the extractor).

---

## 5) Not every algorithm is “weight-aware” (and sometimes “weighted” is a separate function)

### 5.1 Some algorithms ignore payload weights entirely

For example, `distance_matrix(...)` explicitly says the edge payload is **not used** and each edge is treated as distance 1. ([Rustworkx][1])

### 5.2 “Unweighted by design”: betweenness + closeness

* `betweenness_centrality(graph, normalized=True, ...)` has **no weight callback** in its signature. ([Rustworkx][7])
* `closeness_centrality(graph, wf_improved=True, ...)` likewise has **no weight callback**. ([Rustworkx][8])

### 5.3 Weighted closeness is a different function with different semantics

`graph_newman_weighted_closeness_centrality` supports `weight_fn(edge_payload)->float`, but its docs note the weights are assumed to represent **connection strength**; if your weights represent “distance/cost,” you should invert them, otherwise it’s considered a “logical error.” ([Rustworkx][9])

---

## 6) Callback patterns beyond “weight extraction”

### 6.1 A* pathfinding: node predicates + heuristics

`astar_shortest_path(graph, node, goal_fn, edge_cost_fn, estimate_cost_fn)` uses:

* `goal_fn(node_payload) -> bool`
* `edge_cost_fn(edge_payload) -> float` (non-negative)
* `estimate_cost_fn(node_payload) -> float` (non-negative; should be admissible) ([Rustworkx][1])

This is the same “payload is arbitrary → you supply meaning” pattern, applied to nodes and edges.

### 6.2 Isomorphism: node/edge matchers compare payload objects

`is_isomorphic(first, second, node_matcher=None, edge_matcher=None, ...)`:

* `node_matcher(payload_a, payload_b) -> bool`
* `edge_matcher(payload_a, payload_b) -> bool`
  …and includes knobs like `id_order` (performance) and `call_limit` (search bound). ([Rustworkx][10])

### 6.3 Serialization shaping: node-link JSON uses callbacks to map payloads → string dicts

`node_link_json(graph, ..., graph_attrs=None, node_attrs=None, edge_attrs=None)`:

* `graph_attrs(graph.attrs) -> dict[str,str]`
* `node_attrs(node_payload) -> dict[str,str]`
* `edge_attrs(edge_payload) -> dict[str,str]`
  …and will raise if you don’t return a dict of string keys/values. ([Rustworkx][1])

---

## 7) Return types: many algorithms return custom “read-only dict-like” objects

A few examples you’ll feel immediately:

* `all_pairs_dijkstra_shortest_paths(...)` returns an `AllPairsPathMapping` (read-only mapping of source→target→path). ([Rustworkx][1])
* `graph_dijkstra_shortest_path_lengths(...)` returns a `PathLengthMapping`. ([Rustworkx][4])
* `pagerank(...)` returns a “read-only dict-like object” mapping node index → score (`CentralityMapping`). ([Rustworkx][6])

**Practical tip:** if you’re producing “contracts” or serializing, normalize immediately:

```python
scores = dict(rx.pagerank(dg, weight_fn=...))  # make it a plain dict
```

---

## 8) Performance + determinism knobs you should treat as part of the API contract

### 8.1 Multithreading thresholds + `RAYON_NUM_THREADS`

Many algorithms are explicitly multithreaded and expose `parallel_threshold` (or similar), and note you can control threads via `RAYON_NUM_THREADS` (e.g., betweenness, closeness, Floyd–Warshall). ([Rustworkx][7])

### 8.2 Parallelism can change which “tie” you get

`longest_simple_path(...)` notes that if there are multiple equally-long paths, there’s **no guarantee** which one is returned because it depends on parallel execution order; it suggests using an explicit `max(...)` equivalent if you need a stable return. ([Rustworkx][1])

(That’s the kind of detail you’ll want in a pinned “algorithm contract” layer.)

---

## 9) Contract-style micro-fixtures (pytest) for this section

These are small, high-signal tests that pin the behaviors above:

```python
import math
import pytest
import rustworkx as rx

def test_dispatch_universal_works_for_both_graph_types():
    g = rx.PyGraph()
    dg = rx.PyDiGraph()
    # Just verify both accept the universal API without TypeError
    a = g.add_node("a"); b = g.add_node("b")
    g.add_edge(a, b, 1.0)
    rx.dijkstra_shortest_paths(g, a)

    x = dg.add_node("x"); y = dg.add_node("y")
    dg.add_edge(x, y, 1.0)
    rx.dijkstra_shortest_paths(dg, x)

def test_weight_fn_receives_edge_payload():
    g = rx.PyGraph()
    g.add_nodes_from([0, 1, 2])
    g.add_edge(0, 1, {"cost": 2.0})
    g.add_edge(1, 2, {"cost": 3.0})
    path = rx.graph_dijkstra_shortest_paths(g, 0, 2, weight_fn=lambda e: float(e["cost"]))
    assert path[2] == [0, 1, 2]

def test_negative_or_nan_cost_raises_value_error():
    g = rx.PyGraph()
    g.add_nodes_from([0, 1])
    g.add_edge(0, 1, "bad")
    with pytest.raises(ValueError):
        rx.graph_dijkstra_shortest_paths(g, 0, 1, weight_fn=lambda _: -1.0)
    with pytest.raises(ValueError):
        rx.graph_dijkstra_shortest_paths(g, 0, 1, weight_fn=lambda _: float("nan"))

def test_node_link_json_requires_str_dicts():
    g = rx.PyGraph(attrs={"ok": "1"})
    n0 = g.add_node({"x": 1})
    n1 = g.add_node({"y": 2})
    g.add_edge(n0, n1, {"w": 3})

    # Valid
    rx.node_link_json(
        g,
        graph_attrs=lambda attrs: {"ok": str(attrs["ok"])},
        node_attrs=lambda payload: {"kind": "node"},
        edge_attrs=lambda payload: {"kind": "edge"},
    )

    # Invalid: non-str values should raise
    with pytest.raises(Exception):
        rx.node_link_json(g, node_attrs=lambda payload: {"x": 1})
```

Everything these fixtures are pinning is directly documented: dispatching universal calls to typed functions, weight callbacks consuming edge payloads, Dijkstra-family `ValueError` on NaN/negative, and node-link JSON requiring string→string dicts. ([Rustworkx][1])

---

If you want the next deep dive after this, the natural continuation is **“Traversal visitors + early-stop/prune control flow + how return types (PathMapping/AllPairsPathMapping/etc.) should be normalized for deterministic downstream storage.”**

[1]: https://www.rustworkx.org/_modules/rustworkx.html "rustworkx - rustworkx 0.17.1"
[2]: https://www.rustworkx.org/api/pydigraph_api_functions.html "API functions for PyDigraph - rustworkx 0.17.1"
[3]: https://www.rustworkx.org/apiref/rustworkx.graph_dijkstra_shortest_paths.html "rustworkx.graph_dijkstra_shortest_paths - rustworkx 0.17.1"
[4]: https://www.rustworkx.org/apiref/rustworkx.graph_dijkstra_shortest_path_lengths.html "rustworkx.graph_dijkstra_shortest_path_lengths - rustworkx 0.17.1"
[5]: https://www.rustworkx.org/apiref/rustworkx.graph_floyd_warshall_numpy.html "rustworkx.graph_floyd_warshall_numpy - rustworkx 0.17.1"
[6]: https://www.rustworkx.org/apiref/rustworkx.pagerank.html "rustworkx.pagerank - rustworkx 0.17.1"
[7]: https://www.rustworkx.org/apiref/rustworkx.betweenness_centrality.html "rustworkx.betweenness_centrality - rustworkx 0.17.1"
[8]: https://www.rustworkx.org/apiref/rustworkx.closeness_centrality.html "rustworkx.closeness_centrality - rustworkx 0.17.1"
[9]: https://www.rustworkx.org/apiref/rustworkx.graph_newman_weighted_closeness_centrality.html "rustworkx.graph_newman_weighted_closeness_centrality - rustworkx 0.17.1"
[10]: https://www.rustworkx.org/apiref/rustworkx.is_isomorphic.html "rustworkx.is_isomorphic - rustworkx 0.17.1"

# Deep dive — Traversal visitors, early-stop/prune control flow, and deterministic normalization

This layer is about using rustworkx traversals as **instrumented “walkers”** (emit events, build predecessor maps, compute constraints) rather than just “return a list”.

Rustworkx provides three traversal “search” APIs that take a **Visitor object**:

* `bfs_search(graph, source_list, visitor)` ([Rustworkx][1])
* `dfs_search(graph, source_list, visitor)` ([Rustworkx][2])
* `dijkstra_search(graph, source_list, weight_fn, visitor)` ([Rustworkx][3])

Each supports:

* **multiple sources** (applied in the given order) ([Rustworkx][1])
* **early exit** via `StopSearch` ([Rustworkx][1])
* **pruning** via `PruneSearch` (with a key caveat around `finish_vertex`) ([Rustworkx][1])

---

## 1) The traversal contract: visitor event points and signatures

### 1.1 BFS: `BFSVisitor` (color-based, queue)

BFS is documented with pseudo-code and explicit event points; the visitor is called on those events. ([Rustworkx][1])

**Events you can hook**

* `discover_vertex(v)`
* `tree_edge(e)`
* `non_tree_edge(e)`
* `gray_target_edge(e)`
* `black_target_edge(e)`
* `finish_vertex(v)` ([Rustworkx][4])

**Signatures (important for writing visitors)**

* From the `rustworkx.visit` module source, BFS visitor methods are `discover_vertex(self, v)`, `finish_vertex(self, v)`, and edge callbacks `(..., e)` (edge is passed as one object). ([Rustworkx][5])

**Edge shape**
The docs’ BFS examples show `edge` as a 3-tuple `(source, target, edge_payload)` (payload may be `None`). ([Rustworkx][1])

---

### 1.2 DFS: `DFSVisitor` (stack + discovery/finish times)

DFS is also shown with pseudo-code and event points; crucially it includes **time stamps** for discovery/finish. ([Rustworkx][2])

**Events you can hook**

* `discover_vertex(v, t)`
* `tree_edge(e)`
* `back_edge(e)`
* `forward_or_cross_edge(e)` (only meaningful for directed graphs)
* `finish_vertex(v, t)` ([Rustworkx][6])

The module source explains a subtle undirected-graph ambiguity: for undirected graphs, both `tree_edge()` and `back_edge()` may be invoked for the same underlying edge, and suggests resolving by recording tree edges and ignoring already-marked “back edges.” ([Rustworkx][5])

---

### 1.3 Dijkstra: `DijkstraVisitor` (priority queue, weighted traversal)

Dijkstra traversal is documented with pseudo-code and event points. ([Rustworkx][3])

**Events you can hook**

* `discover_vertex(v, cost)` (called when the node is popped from the queue; “optimal distance” is provided) ([Rustworkx][3])
* `examine_edge(e)`
* `edge_relaxed(e)`
* `edge_not_relaxed(e)`
* `finish_vertex(v)` ([Rustworkx][7])

**Weight callback**
`dijkstra_search` takes an optional `weight_fn(edge_payload)->float` and defaults to 1.0 if not provided. ([Rustworkx][3])

---

## 2) Control flow: StopSearch + PruneSearch (and the “finish_vertex” caveat)

### 2.1 StopSearch: fast early exit

`StopSearch` is defined as an exception type in `rustworkx.visit`. ([Rustworkx][5])

* BFS: raising `StopSearch` from a visitor callback stops traversal; the docs state the function returns **without re-raising** the exception. ([Rustworkx][1])
* Dijkstra: same “swallowed StopSearch” behavior is explicitly documented. ([Rustworkx][3])
* DFS: docs say you can exit early by raising `StopSearch`, but do **not** explicitly state whether it’s swallowed; treat it as “control flow” and don’t rely on propagation. ([Rustworkx][2])

**Canonical early-exit pattern (Dijkstra)**
The docs include a visitor where `discover_vertex` raises `StopSearch` when reaching the goal, and `edge_relaxed` records predecessors; after search returns, you reconstruct the path. ([Rustworkx][3])

---

### 2.2 PruneSearch: cut off part of the search tree

`PruneSearch` is also defined as an exception type in `rustworkx.visit`. ([Rustworkx][5])

* BFS/DFS/Dijkstra all document that you can prune traversal by raising `PruneSearch`. ([Rustworkx][1])
* **Important restriction:** An exception is raised if `PruneSearch` is raised in the `finish_vertex` event (BFS/DFS/Dijkstra all call this out). ([Rustworkx][1])

**Prune in practice**
The BFS docs show a “restricted edges” example that raises `PruneSearch` in `tree_edge` to avoid traversing a disallowed edge. ([Rustworkx][1])

---

## 3) Safety rule: you cannot mutate the graph during traversal

All three search docs include a note:

> The graph cannot be mutated while traversing; trying to do so raises an exception. ([Rustworkx][1])

**Practical implication:** treat the graph as immutable for the duration, but freely mutate *external state* (your visitor’s dicts/lists).

---

## 4) How to use visitors in “systems work” (patterns that scale)

### 4.1 Build a predecessor map (BFS/DFS/Dijkstra)

Use `tree_edge` (BFS/DFS) or `edge_relaxed` (Dijkstra) to set:

```python
pred[target] = source
```

* BFS: gives shortest paths in *edge count* (unweighted)
* Dijkstra: gives shortest paths in *weighted cost*; `discover_vertex(v, cost)` tells you when a node’s distance is finalized. ([Rustworkx][3])

### 4.2 Early-stop: “find first match” / “stop at boundary”

* BFS: stop on `discover_vertex(v)` for “closest match”.
* DFS: stop on `discover_vertex(v, t)` for “any match” with minimal overhead.
* Dijkstra: stop on `discover_vertex(v, cost)` for “lowest-cost match”. ([Rustworkx][3])

### 4.3 Prune: enforce constraints without rebuilding a filtered graph

In `tree_edge` (or edge examination callbacks), if the candidate edge violates a policy, raise `PruneSearch` to skip exploring that branch. ([Rustworkx][1])

This is a strong fit for:

* “don’t traverse into generated/third-party nodes”
* “don’t cross subsystem boundaries”
* “don’t cross certain edge kinds” (by encoding kind in edge payload)

---

## 5) Deterministic normalization for downstream storage

Rustworkx returns a lot of **custom container types** (read-only mapping/sequence types) for speed. ([Rustworkx][8])
If you want deterministic Arrow/DuckDB ingestion and stable golden snapshots, normalize aggressively.

### 5.1 Know the key return types you’ll see

From the “Custom Return Types” docs: `PathMapping`, `PathLengthMapping`, `AllPairsPathMapping`, `AllPairsPathLengthMapping`, plus `NodeIndices`, `EdgeIndices`, `EdgeList`, `WeightedEdgeList`, etc. ([Rustworkx][8])

**PathMapping specifics**

* A read-only mapping: `target_node -> [path_nodes...]` ([Rustworkx][9])
* Implements mapping protocol and is iterable via `iter(...)` (yields “results in order”). ([Rustworkx][9])

**AllPairsPathMapping specifics**

* A read-only mapping-of-mappings: `source -> (target -> path)` ([Rustworkx][10])

### 5.2 Normalization rules that avoid drift

I recommend treating *every* rustworkx custom return object as “non-serializable” until normalized:

**Rule A — Convert to plain Python**

* `dict(mapping_like)` for mapping returns
* `list(sequence_like)` for `NodeIndices`, `EdgeIndices`, edge lists, etc. ([Rustworkx][8])

**Rule B — Sort keys explicitly**
Even if iteration order is described as “in order,” enforce determinism yourself:

* For `PathMapping`: `for dst in sorted(pm.keys()): ...`
* For `AllPairsPathMapping`: `for src in sorted(ap.keys()): for dst in sorted(ap[src].keys()): ...` ([Rustworkx][10])

**Rule C — Canonicalize paths**
Store paths as:

* `list[int]` (Python)
* or Arrow `list<int32>` / `list<uint32>` (recommended)

**Rule D — Include “algorithm metadata” columns**
If you persist results, add columns like:

* `algo` (`"bfs"|"dfs"|"dijkstra"|"all_pairs_dijkstra"`)
* `weight_policy` (e.g., `"unit"|"payload.cost"|"payload.strength_inverted"`)
* `run_id` / `graph_id` (your system identity)

This prevents “same path” from being ambiguous in downstream analysis.

---

## 6) Suggested “contract tables” for storing traversal outputs (Arrow-friendly)

These are pragmatic shapes that work well in Arrow/DuckDB and are stable under normalization.

### 6.1 `traversal_events`

For BFS/DFS/Dijkstra search with visitors, capture an event log:

**Columns**

* `run_id: string`
* `algo: string` (`bfs|dfs|dijkstra`)
* `event: string` (`discover_vertex|tree_edge|...`)
* `src: uint32` (nullable)
* `dst: uint32` (nullable)
* `cost: float64` (nullable; Dijkstra discover)
* `time: uint32` (nullable; DFS discover/finish times)
* `edge_payload_repr: string` (optional; if you need a deterministic textual representation)

Events and which callbacks exist are per-visitor documented. ([Rustworkx][4])

### 6.2 `shortest_paths`

For `PathMapping`-style outputs (single-source shortest paths):

**Columns**

* `run_id: string`
* `source: uint32`
* `target: uint32`
* `path: list<uint32>` (the node indices)
* `algo: string`
* `cost: float64` (optional, if you store Dijkstra distances separately)

PathMapping semantics are explicitly defined. ([Rustworkx][9])

### 6.3 `all_pairs_shortest_paths`

For `AllPairsPathMapping`:

**Columns**

* `run_id: string`
* `source: uint32`
* `target: uint32`
* `path: list<uint32>`
* `algo: string`

AllPairsPathMapping is explicitly described as a read-only mapping for “paths from all nodes.” ([Rustworkx][10])

---

## 7) Golden micro-fixtures (pytest) to pin the tricky parts

These directly assert the control-flow “gotchas” you’ll want stable across upgrades.

```python
import pytest
import rustworkx as rx
from rustworkx.visit import BFSVisitor, DFSVisitor, DijkstraVisitor, StopSearch, PruneSearch

def test_bfs_stopsearch_is_early_exit_and_swallowed():
    g = rx.PyGraph()
    g.add_nodes_from([0, 1, 2])
    g.add_edges_from_no_data([(0, 1), (1, 2)])

    class V(BFSVisitor):
        def discover_vertex(self, v):
            if v == 2:
                raise StopSearch

    # should not raise
    rx.bfs_search(g, [0], V())

def test_dijkstra_stopsearch_is_early_exit_and_swallowed():
    g = rx.PyGraph()
    g.add_nodes_from([0, 1])
    g.add_edge(0, 1, 1.0)

    class V(DijkstraVisitor):
        def discover_vertex(self, v, cost):
            if v == 1:
                raise StopSearch

    rx.dijkstra_search(g, [0], weight_fn=float, visitor=V())

def test_prunesearch_in_finish_vertex_is_error_bfs():
    g = rx.PyGraph()
    g.add_nodes_from([0, 1])
    g.add_edges_from_no_data([(0, 1)])

    class V(BFSVisitor):
        def finish_vertex(self, v):
            raise PruneSearch

    with pytest.raises(Exception):
        rx.bfs_search(g, [0], V())
```

These behaviors are explicitly documented: StopSearch early-exit (swallowed for BFS/Dijkstra), and PruneSearch-in-finish_vertex raising an exception. ([Rustworkx][1])

---


[1]: https://www.rustworkx.org/apiref/rustworkx.bfs_search.html "rustworkx.bfs_search - rustworkx 0.17.1"
[2]: https://www.rustworkx.org/apiref/rustworkx.dfs_search.html "rustworkx.dfs_search - rustworkx 0.17.1"
[3]: https://www.rustworkx.org/apiref/rustworkx.dijkstra_search.html "rustworkx.dijkstra_search - rustworkx 0.17.1"
[4]: https://www.rustworkx.org/apiref/rustworkx.visit.BFSVisitor.html "BFSVisitor - rustworkx 0.17.1"
[5]: https://www.rustworkx.org/_modules/rustworkx/visit.html?utm_source=chatgpt.com "rustworkx.visit - rustworkx 0.17.1"
[6]: https://www.rustworkx.org/apiref/rustworkx.visit.DFSVisitor.html?utm_source=chatgpt.com "DFSVisitor - rustworkx 0.17.1"
[7]: https://www.rustworkx.org/apiref/rustworkx.visit.DijkstraVisitor.html "DijkstraVisitor - rustworkx 0.17.1"
[8]: https://www.rustworkx.org/api/custom_return_types.html "Custom Return Types - rustworkx 0.17.1"
[9]: https://www.rustworkx.org/apiref/rustworkx.PathMapping.html "PathMapping - rustworkx 0.17.1"
[10]: https://www.rustworkx.org/apiref/rustworkx.AllPairsPathMapping.html?utm_source=chatgpt.com "AllPairsPathMapping - rustworkx 0.17.1"

Below is a deep dive into the **universal algorithm functions** from section (2) that we *haven’t* really covered yet (beyond shortest paths + traversal/visitors + basic “weight_fn dispatch” patterns). I’m grouping by the same clusters as the docs.

---

## Centrality and link analysis (beyond “they return a mapping”)

### What you actually get back (and why it matters)

Most centrality calls return **read-only dict-like custom containers** (e.g., `CentralityMapping` / `EdgeCentralityMapping`). In practice, treat them as “mapping-like” and normalize to `dict(...)` immediately for deterministic downstream storage.

### Degree centrality (quickest “importance” proxy)

* `degree_centrality(graph)` returns a mapping `node_index -> score` for **PyGraph or PyDiGraph**. ([Rustworkx][1])
* `in_degree_centrality(graph)` / `out_degree_centrality(graph)` are specifically for **PyDiGraph**. ([Rustworkx][2])

**Why it matters:** these are cheap and great for “hubness” screens before running heavier algorithms.

### Betweenness (node and edge) — parallelism knob

* `betweenness_centrality(..., parallel_threshold=50)` runs multithreaded above the threshold and is tunable via `RAYON_NUM_THREADS`. ([Rustworkx][3])
* `edge_betweenness_centrality(..., parallel_threshold=50)` has the same parallel-threshold concept. ([Rustworkx][4])

**Why it matters:** this is one of the best “structural chokepoint” measures, but it’s heavier and can be nondeterministic in tie-ish situations if you don’t normalize/sort before persisting.

### Eigenvector / Katz — power iteration + multigraph weight aggregation

* `eigenvector_centrality(...)` uses power iteration; **convergence is not guaranteed** (bounded by `max_iter` / tolerance logic). In multigraphs, **parallel edge weights are summed**. ([Rustworkx][5])
* `katz_centrality(...)` has the same “power iteration, convergence not guaranteed,” and also **sums parallel weights** in multigraphs. ([Rustworkx][6])

**Practical guidance**

* Keep edge payloads numeric (or supply a cheap `weight_fn`) to avoid Python overhead in the iteration loop.
* Always record `{algo, max_iter, tol, weight_policy}` alongside results so upgrades don’t silently drift.

### Weighted closeness (Newman) — semantics are “strength”, not “distance”

`newman_weighted_closeness_centrality` is explicitly described as treating weights as **connection strength** (it inverts weights internally to compute shortest paths), following Newman’s method. ([Rustworkx][7])

If your edge payload is a “cost/distance”, you typically want **standard closeness** (unweighted) or you must invert yourself and be explicit about it in your weight policy.

### PageRank + HITS (link analysis)

* `pagerank(...)` uses power iteration; **convergence not guaranteed**; sums parallel-edge weights in multigraphs. ([Rustworkx][8])
* `hits(...)` is similar (power iteration; convergence not guaranteed; sums parallel-edge weights). ([Rustworkx][9])

**Why it matters:** both are “global importance” measures that can be sensitive to normalization choices. For deterministic storage, persist the params (`alpha`, `tol`, `max_iter`, `weight_policy`) with the output.

---

## Coloring (node + edge) — strategies, presets, and bipartite fast paths

### Node coloring: `graph_greedy_color`

* Heuristic greedy node coloring (problem is NP-hard; may not be optimal). ([Rustworkx][10])
* Supports:

  * `strategy` (default Degree / largest-first)
  * `preset_color_fn(node_index) -> int | None` (no validation; you can create invalid colorings) ([Rustworkx][10])
* Returns `dict[node_index -> color_int]`. ([Rustworkx][10])
* Strategies are enumerated in `ColoringStrategy`: Degree, Saturation (DSATUR), IndependentSet. ([Rustworkx][11])

**When you use this in “systems” work**

* Scheduling / partitioning (e.g., independent sets, parallelizable batches)
* Fast “conflict class” approximation
* Constraint solving “prepass” (not a proof)

### Edge coloring (3 options, very different guarantees)

**1) Bipartite edge coloring: `graph_bipartite_edge_color`**

* Checks bipartiteness; if not bipartite, returns `None`. ([Rustworkx][12])
* Complexity stated as **O(n + m log m)**; based on Alon (2003). ([Rustworkx][12])
* Returns `dict[edge_index -> color_int]` if bipartite. ([Rustworkx][12])

**2) Greedy edge coloring: `graph_greedy_edge_color`**

* Colors the **line graph** greedily. ([Rustworkx][13])
* Supports:

  * `preset_color_fn(edge_index) -> int | None` (no validation)
  * `strategy` (Degree / Saturation / IndependentSet) ([Rustworkx][13])
* Returns `dict[edge_index -> color_int]`. ([Rustworkx][13])

**3) Misra–Gries edge coloring: `graph_misra_gries_edge_color`**

* Constructive Vizing-style method; guarantees ≤ **d + 1** colors (d = max degree). ([Rustworkx][14])
* Returns `dict[edge_index -> color_int]`. ([Rustworkx][14])

### Two-coloring: `two_color`

* “Two-coloring of a directed graph”; returns `None` if not bipartite; otherwise returns `dict[node_index -> 0|1]`. ([Rustworkx][15])

---

## Connectivity & cycles — the “danger zone” (combinatorics + multigraph caveats)

### Component finding (undirected + directed)

* `connected_components(PyGraph)` returns sets of node indices. ([Rustworkx][16])
* Directed connectivity:

  * `strongly_connected_components(PyDiGraph)` uses **Kosaraju’s algorithm**. ([Rustworkx][17])
  * `weakly_connected_components(PyDiGraph)` treats directions as ignored for reachability. ([Rustworkx][18])

### Path existence + isolates + bipartite check

* `has_path(graph, source, target, as_undirected=False)` is a cheap reachability predicate. ([Rustworkx][19])
* `isolates(graph)` returns `NodeIndices` where degree is 0 (for directed: both in/out degree 0). ([Rustworkx][20])
* `is_bipartite(graph)` returns bool. ([Rustworkx][21])

### Cycle primitives: “basis” vs “enumerate everything”

**Cycle basis (undirected): `cycle_basis(PyGraph, root=None)`**

* Returns a *basis* such that any cycle can be expressed as XOR-sum of basis cycles; adapted from Paton (1969). ([Rustworkx][22])
* **Assumes no parallel edges**; may be incorrect with parallel edges. ([Rustworkx][22])

**All simple cycles (directed): `simple_cycles(PyDiGraph)`**

* Enumerates all simple cycles (Johnson’s algorithm). ([Rustworkx][23])

**Find a cycle fast (directed): `digraph_find_cycle(PyDiGraph, source=None)`**

* Returns the *first* cycle encountered during DFS; empty list if none. Optional `source` controls where you start. ([Rustworkx][24])

> For any “real repo graph,” *enumerating all simple cycles* can explode. Use `digraph_find_cycle` or SCCs unless you truly need full enumeration.

### Structural “cut” and decomposition algorithms (key multigraph pitfall)

These functions explicitly warn they assume **no self-loops or parallel edges** and may produce incorrect results otherwise:

* `articulation_points(PyGraph)` ([Rustworkx][25])
* `bridges(PyGraph)` ([Rustworkx][26])
* `biconnected_components(PyGraph)` ([Rustworkx][27])
* `core_number(graph)` similarly warns about parallel edges/self loops. ([Rustworkx][28])

**Implication:** if you’re using multigraph edges to represent multiple semantic relationships, consider projecting to a **simple graph view** before running these (e.g., “edge exists iff any semantic edge exists”).

### Chain decomposition: `chain_decomposition(PyGraph, source=None)`

* Returns a **list of chains**, each chain is an `EdgeList` (edges) and the return is effectively “list of EdgeList”. ([Rustworkx][29])

### Simple paths (huge output potential)

* `all_simple_paths(graph, from_, to, min_depth=None, cutoff=None)` returns all simple paths between a source and one/many targets. ([Rustworkx][30])
* `all_pairs_all_simple_paths(graph, ...)` is explicitly **multithreaded**; uses Rayon threads (tunable with `RAYON_NUM_THREADS`). ([Rustworkx][31])

### Min-cut: `stoer_wagner_min_cut(PyGraph, weight_fn=None)`

* Weights must be nonnegative; NaN edge weights are ignored (treated as zero); returns `(min_cut_value, NodeIndices_partition)`; returns `None` for graphs with < 2 nodes. ([Rustworkx][32])

---

## DAG algorithms — layering, reductions, and “longest path” variants

### Validating DAG-ness

* `is_directed_acyclic_graph(PyDiGraph)` returns bool (no cycles). ([Rustworkx][33])

### Topological forms

* `topological_sort(graph)` returns a topological ordering of node indices (dependency resolution). ([Rustworkx][34])
* `lexicographical_topological_sort(dag, key, reverse=...)` breaks ties via a key function (lexicographic order). ([Rustworkx][35])
* `topological_generations(dag)` returns layered generations where ancestors are in earlier generations. ([Rustworkx][36])

### “Layer” packing: `layers(dag, first_layer, index_output=False)`

* Greedy layer construction; if `index_output=True`, returns layers as node indices instead of node payloads. ([Rustworkx][37])

### Transitive reduction (DAG simplification)

* `transitive_reduction(dag)` removes edges implied by longer paths (only defined for DAGs). ([Rustworkx][38])

### Longest path in a DAG (unweighted vs weighted)

* `dag_longest_path(graph, weight_fn=...)` traverses in topological order and checks incoming edges; `weight_fn` returns an **unsigned integer** edge weight (3-arg form). ([Rustworkx][39])
* `dag_weighted_longest_path(graph, weight_fn)` requires `weight_fn` returning `float`. ([Rustworkx][40])
* `_length` variants return just the length. ([Rustworkx][41])

---

## Dominance — the CFG/SSA workhorses

### `immediate_dominators(PyDiGraph, start_node)`

* Uses Cooper/Harvey/Kennedy (2006); quadratic in #vertices; returns `dict[node -> idom]`; raises `NullGraph` / `InvalidNode`. ([Rustworkx][42])

### `dominance_frontiers(PyDiGraph, start_node)`

* Same authors; returns `dict[node -> set[frontier_nodes]]`; raises `NullGraph` / `InvalidNode`. ([Rustworkx][43])

**How this usually gets used**

* Build dominator tree from `immediate_dominators`
* Use `dominance_frontiers` for SSA phi-placement / join-point reasoning (very relevant for CFG/DFG correctness)

---

## Graph operations — union/complement/cartesian product (and their “payload semantics”)

### Complement

* `complement(graph)` returns same graph type; **never creates parallel edges or self-loops**, even if multigraph is `True`. ([Rustworkx][44])

### Union

* `union(first, second, merge_nodes=False, merge_edges=False)` is explicitly described as 3 phases: add nodes, optionally merge nodes by equal payload, add edges (optionally merge edges by equal payload). Payload objects are passed **by reference** into the new graph. ([Rustworkx][45])

### Cartesian product

* `cartesian_product(first, second)` returns `(product_graph, ProductNodeMap)` mapping `(node1, node2) -> new_node_index`. Payload objects are passed by reference. ([Rustworkx][46])

---

## Isomorphism — VF2 surfaces and safety knobs

### `is_isomorphic(first, second, node_matcher=..., edge_matcher=..., id_order=True)`

* Compares structure and payloads via matcher callbacks. For large graphs, docs suggest `id_order=False` for better performance (heuristic order from VF2). ([Rustworkx][47])

### `is_subgraph_isomorphic(first, second, induced=True, call_limit=...)`

* `induced=True` checks node-induced subgraph; `call_limit` bounds VF2 search states; if exceeded, returns `False`. ([Rustworkx][48])

### `vf2_mapping(first, second, ...)`

* Runs the same VF2 logic as isomorphism checks, but returns an **iterator over all mappings** from `first` to `second` (empty if none). ([Rustworkx][49])

**Practical warning:** the mapping iterator can be enormous. If you need “any mapping,” take the first and stop.

---

## Matching — max-weight vs validation vs maximality

### `max_weight_matching(PyGraph, ...)`

* Undirected graph; expects no parallel edges (“multigraphs untested”); **O(n³)**; blossom + primal-dual (Edmonds-style). Options include:

  * `max_cardinality`
  * `weight_fn(edge_payload)->int` and `default_weight`
  * `verify_optimum` (testing) ([Rustworkx][50])
* Returns a `set` of node-index tuples (one direction only). ([Rustworkx][50])

### `is_matching(graph, matching)`

* Validates a matching is well-formed (no shared endpoints). ([Rustworkx][51])

### `is_maximal_matching(graph, matching)`

* Checks “maximal” (locally cannot add edges) not “maximum” (globally optimal). ([Rustworkx][52])

---

## Tree algorithms + metric closure (MST / Steiner)

### Minimum spanning edges/tree (Kruskal)

* `minimum_spanning_edges(PyGraph, ...)` returns a `WeightedEdgeList`; ValueError on NaN edge weights. ([Rustworkx][53])
* `minimum_spanning_tree(PyGraph, ...)` returns a new `PyGraph`; node indices preserved but edge indices may differ; ValueError on NaN weights. ([Rustworkx][54])

### Metric closure

* `metric_closure(PyGraph, weight_fn)` returns a complete graph where edges are shortest-path distances; raises `ValueError` on NaN/negative weights. ([Rustworkx][55])

### Steiner tree approximation

* `steiner_tree(PyGraph, terminal_nodes, weight_fn)` returns an approximation based on metric closure + MST logic; raises `ValueError` on NaN/negative weights. ([Rustworkx][56])

---

## “Other algorithm functions” (matrix + clustering-ish + planarity + bisimulation)

### Adjacency matrix

* `adjacency_matrix(graph, weight_fn=None, default_weight=1.0, null_value=0.0)` returns a numpy adjacency matrix; parallel edges are **summed**. ([Rustworkx][57])

### Transitivity (global clustering coefficient)

* `transitivity(graph)` is multithreaded and tunable via `RAYON_NUM_THREADS`; warns results may be incorrect with parallel edges/self-loops. ([Rustworkx][58])
* There are directed/undirected typed variants with slightly different formulas. ([Rustworkx][59])

### Core numbers (k-core)

* `core_number(graph)` returns `dict[node -> core]` and warns about parallel edges/self-loops. ([Rustworkx][28])

### Line graph

* `graph_line_graph(PyGraph)` constructs L(G): node per edge; edges between edges that share an endpoint. ([Rustworkx][60])

### Planarity

* `is_planar(PyGraph)` uses the Left-Right Planarity Test (Brandes) and returns bool. ([Rustworkx][61])

### Maximum bisimulation (directed)

* `digraph_maximum_bisimulation(PyDiGraph)` computes relational coarsest partition via Paige–Tarjan; returns `RelationalCoarsestPartition` (iterator of `NodeIndices`). ([Rustworkx][62])

---

## Bonus “often missed” typed-only algorithms (still part of the ecosystem)

These aren’t universal graph-type agnostic calls, but they’re easy to miss:

* `graph_token_swapper(graph, mapping, ...)` (approximate token swapping; has parallel threshold). ([Rustworkx][63])
* `local_complement(PyGraph, node)` (requires no self loops; errors if multigraph is True). ([Rustworkx][64])

---

## Deterministic storage / Arrow contract notes for the *new* outputs above

If you’re extending the `rustworkx_arrow_contracts` module you downloaded earlier:

* **Node-mapping outputs** (centrality, node-coloring, two-coloring, dominators):

  * normalize to rows: `(node, value)` sorted by `node`
* **Edge-mapping outputs** (edge-coloring):

  * normalize to rows: `(edge_index, color)` sorted by `(edge_index)`
* **Set-of-nodes outputs** (components, articulation points, isolates, min-cut partition):

  * normalize to rows: `(group_id, node)` sorted by `(group_id, node)` (and sort the node list before emitting)
* **List-of-paths / cycle lists / chain decompositions**:

  * store as `(group_id, seq_id, edge_list_or_node_list)` and sort deterministically; beware output size explosion (`all_pairs_all_simple_paths`, `simple_cycles`) ([Rustworkx][31])


[1]: https://www.rustworkx.org/apiref/rustworkx.degree_centrality.html?utm_source=chatgpt.com "rustworkx.degree_centrality"
[2]: https://www.rustworkx.org/apiref/rustworkx.in_degree_centrality.html?utm_source=chatgpt.com "rustworkx.in_degree_centrality"
[3]: https://www.rustworkx.org/stable/0.12/apiref/rustworkx.betweenness_centrality.html?utm_source=chatgpt.com "rustworkx.betweenness_centrality"
[4]: https://www.rustworkx.org/apiref/rustworkx.edge_betweenness_centrality.html?utm_source=chatgpt.com "rustworkx.edge_betweenness_centrality"
[5]: https://www.rustworkx.org/apiref/rustworkx.eigenvector_centrality.html?utm_source=chatgpt.com "rustworkx.eigenvector_centrality"
[6]: https://www.rustworkx.org/apiref/rustworkx.katz_centrality.html?utm_source=chatgpt.com "rustworkx.katz_centrality"
[7]: https://www.rustworkx.org/apiref/rustworkx.newman_weighted_closeness_centrality.html?utm_source=chatgpt.com "rustworkx.newman_weighted_closeness_centrality"
[8]: https://www.rustworkx.org/apiref/rustworkx.pagerank.html?utm_source=chatgpt.com "rustworkx.pagerank"
[9]: https://www.rustworkx.org/apiref/rustworkx.hits.html?utm_source=chatgpt.com "rustworkx.hits"
[10]: https://www.rustworkx.org/apiref/rustworkx.graph_greedy_color.html "rustworkx.graph_greedy_color - rustworkx 0.17.1"
[11]: https://www.rustworkx.org/apiref/rustworkx.ColoringStrategy.html "ColoringStrategy - rustworkx 0.17.1"
[12]: https://www.rustworkx.org/apiref/rustworkx.graph_bipartite_edge_color.html "rustworkx.graph_bipartite_edge_color - rustworkx 0.17.1"
[13]: https://www.rustworkx.org/apiref/rustworkx.graph_greedy_edge_color.html "rustworkx.graph_greedy_edge_color - rustworkx 0.17.1"
[14]: https://www.rustworkx.org/apiref/rustworkx.graph_misra_gries_edge_color.html "rustworkx.graph_misra_gries_edge_color - rustworkx 0.17.1"
[15]: https://www.rustworkx.org/apiref/rustworkx.two_color.html "rustworkx.two_color - rustworkx 0.17.1"
[16]: https://www.rustworkx.org/apiref/rustworkx.connected_components.html?utm_source=chatgpt.com "rustworkx.connected_components"
[17]: https://www.rustworkx.org/apiref/rustworkx.strongly_connected_components.html?utm_source=chatgpt.com "rustworkx.strongly_connected_components"
[18]: https://www.rustworkx.org/apiref/rustworkx.weakly_connected_components.html?utm_source=chatgpt.com "rustworkx.weakly_connected_components"
[19]: https://www.rustworkx.org/apiref/rustworkx.has_path.html?utm_source=chatgpt.com "rustworkx.has_path"
[20]: https://www.rustworkx.org/apiref/rustworkx.isolates.html?utm_source=chatgpt.com "rustworkx.isolates"
[21]: https://www.rustworkx.org/apiref/rustworkx.is_bipartite.html?utm_source=chatgpt.com "rustworkx.is_bipartite"
[22]: https://www.rustworkx.org/apiref/rustworkx.cycle_basis.html "rustworkx.cycle_basis - rustworkx 0.17.1"
[23]: https://www.rustworkx.org/apiref/rustworkx.simple_cycles.html "rustworkx.simple_cycles - rustworkx 0.17.1"
[24]: https://www.rustworkx.org/apiref/rustworkx.digraph_find_cycle.html "rustworkx.digraph_find_cycle - rustworkx 0.17.1"
[25]: https://www.rustworkx.org/apiref/rustworkx.articulation_points.html?utm_source=chatgpt.com "rustworkx.articulation_points"
[26]: https://www.rustworkx.org/apiref/rustworkx.bridges.html?utm_source=chatgpt.com "rustworkx.bridges"
[27]: https://www.rustworkx.org/apiref/rustworkx.biconnected_components.html?utm_source=chatgpt.com "rustworkx.biconnected_components"
[28]: https://www.rustworkx.org/apiref/rustworkx.core_number.html "rustworkx.core_number - rustworkx 0.17.1"
[29]: https://www.rustworkx.org/apiref/rustworkx.chain_decomposition.html?utm_source=chatgpt.com "rustworkx.chain_decomposition"
[30]: https://www.rustworkx.org/apiref/rustworkx.all_simple_paths.html?utm_source=chatgpt.com "rustworkx.all_simple_paths"
[31]: https://www.rustworkx.org/apiref/rustworkx.all_pairs_all_simple_paths.html?utm_source=chatgpt.com "rustworkx.all_pairs_all_simple_paths"
[32]: https://www.rustworkx.org/apiref/rustworkx.stoer_wagner_min_cut.html "rustworkx.stoer_wagner_min_cut - rustworkx 0.17.1"
[33]: https://www.rustworkx.org/apiref/rustworkx.is_directed_acyclic_graph.html?utm_source=chatgpt.com "rustworkx.is_directed_acyclic_graph"
[34]: https://www.rustworkx.org/apiref/rustworkx.topological_sort.html?utm_source=chatgpt.com "rustworkx.topological_sort"
[35]: https://www.rustworkx.org/apiref/rustworkx.lexicographical_topological_sort.html?utm_source=chatgpt.com "rustworkx.lexicographical_topological_sort"
[36]: https://www.rustworkx.org/apiref/rustworkx.topological_generations.html?utm_source=chatgpt.com "rustworkx.topological_generations"
[37]: https://www.rustworkx.org/apiref/rustworkx.layers.html?utm_source=chatgpt.com "rustworkx.layers"
[38]: https://www.rustworkx.org/apiref/rustworkx.transitive_reduction.html?utm_source=chatgpt.com "rustworkx.transitive_reduction"
[39]: https://www.rustworkx.org/apiref/rustworkx.dag_longest_path.html?utm_source=chatgpt.com "rustworkx.dag_longest_path"
[40]: https://www.rustworkx.org/apiref/rustworkx.dag_weighted_longest_path.html?utm_source=chatgpt.com "rustworkx.dag_weighted_longest_path"
[41]: https://www.rustworkx.org/apiref/rustworkx.dag_longest_path_length.html?utm_source=chatgpt.com "rustworkx.dag_longest_path_length"
[42]: https://www.rustworkx.org/apiref/rustworkx.immediate_dominators.html "rustworkx.immediate_dominators - rustworkx 0.17.1"
[43]: https://www.rustworkx.org/apiref/rustworkx.dominance_frontiers.html "rustworkx.dominance_frontiers - rustworkx 0.17.1"
[44]: https://www.rustworkx.org/apiref/rustworkx.complement.html "rustworkx.complement - rustworkx 0.17.1"
[45]: https://www.rustworkx.org/apiref/rustworkx.union.html "rustworkx.union - rustworkx 0.17.1"
[46]: https://www.rustworkx.org/apiref/rustworkx.cartesian_product.html "rustworkx.cartesian_product - rustworkx 0.17.1"
[47]: https://www.rustworkx.org/apiref/rustworkx.is_isomorphic.html?utm_source=chatgpt.com "rustworkx.is_isomorphic"
[48]: https://www.rustworkx.org/apiref/rustworkx.is_subgraph_isomorphic.html?utm_source=chatgpt.com "rustworkx.is_subgraph_isomorphic"
[49]: https://www.rustworkx.org/apiref/rustworkx.vf2_mapping.html?utm_source=chatgpt.com "rustworkx.vf2_mapping"
[50]: https://www.rustworkx.org/apiref/rustworkx.max_weight_matching.html "rustworkx.max_weight_matching - rustworkx 0.17.1"
[51]: https://www.rustworkx.org/apiref/rustworkx.is_matching.html?utm_source=chatgpt.com "rustworkx.is_matching"
[52]: https://www.rustworkx.org/dev/apiref/rustworkx.is_maximal_matching.html?utm_source=chatgpt.com "rustworkx.is_maximal_matching"
[53]: https://www.rustworkx.org/apiref/rustworkx.minimum_spanning_edges.html "rustworkx.minimum_spanning_edges - rustworkx 0.17.1"
[54]: https://www.rustworkx.org/apiref/rustworkx.minimum_spanning_tree.html "rustworkx.minimum_spanning_tree - rustworkx 0.17.1"
[55]: https://www.rustworkx.org/apiref/rustworkx.metric_closure.html "rustworkx.metric_closure - rustworkx 0.17.1"
[56]: https://www.rustworkx.org/apiref/rustworkx.steiner_tree.html "rustworkx.steiner_tree - rustworkx 0.17.1"
[57]: https://www.rustworkx.org/apiref/rustworkx.adjacency_matrix.html "rustworkx.adjacency_matrix - rustworkx 0.17.1"
[58]: https://www.rustworkx.org/apiref/rustworkx.transitivity.html "rustworkx.transitivity - rustworkx 0.17.1"
[59]: https://www.rustworkx.org/apiref/rustworkx.graph_transitivity.html "rustworkx.graph_transitivity - rustworkx 0.17.1"
[60]: https://www.rustworkx.org/apiref/rustworkx.graph_line_graph.html?utm_source=chatgpt.com "rustworkx.graph_line_graph"
[61]: https://www.rustworkx.org/apiref/rustworkx.is_planar.html "rustworkx.is_planar - rustworkx 0.17.1"
[62]: https://www.rustworkx.org/apiref/rustworkx.digraph_maximum_bisimulation.html "rustworkx.digraph_maximum_bisimulation - rustworkx 0.17.1"
[63]: https://www.rustworkx.org/apiref/rustworkx.graph_token_swapper.html?utm_source=chatgpt.com "rustworkx.graph_token_swapper"
[64]: https://www.rustworkx.org/apiref/rustworkx.local_complement.html?utm_source=chatgpt.com "rustworkx.local_complement"

## Deep dive — Graph generation in rustworkx (deterministic + random)

rustworkx’s generation surface splits cleanly into:

* **Deterministic “shape” generators** under `rustworkx.generators.*` (cycle/path/star/grid/lattice, plus a handful of canonical/special graphs). ([rustworkx.org][1])
* **Random graph generators** exposed as **top-level** functions like `undirected_gnp_random_graph`, `directed_gnm_random_graph`, `random_geometric_graph`, etc. ([rustworkx.org][2])

Below is the deeper “power knobs + semantics + gotchas” pass.

---

# A) Deterministic generators (`rustworkx.generators`)

### A1) The common signature pattern: `num_*` OR `weights`, plus `multigraph`

Many of the “classic” shapes share a consistent API:

* Provide a **size** (`num_nodes`, or `rows/cols`, etc.) → nodes are created with **node weights `None`** (when the docs say so).
* Provide **`weights: Sequence[Any]`** → node payloads come from that sequence and override `num_*` when both are provided. Example: `cycle_graph(num_nodes=None, weights=None, multigraph=True)`. ([rustworkx.org][3])
* Most accept **`multigraph: bool = True`**, and when `False` the resulting graph disallows parallel edges (attempts to add a parallel edge update existing edge payload instead). ([rustworkx.org][3])

Release notes call out that `multigraph` was added across several of these generator functions (cycle/path/star/mesh/grid) and defaults to `True`. ([rustworkx.org][4])

### A2) Directed variants often add `bidirectional` (and sometimes `inward`)

Directed “shape” generators frequently include:

* `bidirectional=False`: if set `True`, add edges **both directions** between neighbors. ([rustworkx.org][5])
* `inward` (directed star only): controls whether star edges point **toward the center**; ignored when `bidirectional=True`. ([rustworkx.org][6])

---

## A3) The “classic” shapes

### Cycle / Path / Star (undirected + directed)

* `cycle_graph(num_nodes=None, weights=None, multigraph=True)` ([rustworkx.org][3])
* `directed_cycle_graph(..., bidirectional=False, multigraph=True)` ([rustworkx.org][5])
* `path_graph(num_nodes=None, weights=None, multigraph=True)` ([rustworkx.org][7])
* `directed_path_graph(..., bidirectional=False, multigraph=True)` ([rustworkx.org][8])
* `star_graph(..., weights=..., first weight is center, multigraph=True)` ([rustworkx.org][9])
* `directed_star_graph(..., inward=False, bidirectional=False, multigraph=True)` ([rustworkx.org][6])

**High-leverage details**

* Every one of these raises `IndexError` if you provide neither a size nor weights. ([rustworkx.org][3])
* The only “special” weight rule is star: `weights[0]` becomes the center node. ([rustworkx.org][9])

---

## A4) Complete graphs: `mesh_graph` vs `complete_graph`

rustworkx historically used “mesh” naming for complete graphs:

* `mesh_graph(...)` generates an undirected complete graph. ([rustworkx.org][10])
* `directed_mesh_graph(...)` generates a directed complete graph. ([rustworkx.org][11])

Later, it added explicit:

* `complete_graph(...)` and `directed_complete_graph(...)`
  …and release notes state these are **equivalent to** `mesh_graph()` / `directed_mesh_graph()`. ([rustworkx.org][4])

The directed complete graph documentation also states the edge count is `n*(n-1)` (i.e., no self-loops). ([rustworkx.org][12])

---

## A5) Grid graphs (and a subtle “weights-only” behavior)

* `grid_graph(rows=None, cols=None, weights=None, multigraph=True)` generates an undirected grid. ([rustworkx.org][13])
* Node weights are filled **row-wise** if you provide both `rows` and `cols`. If `rows/cols` produce fewer nodes than `len(weights)`, trailing weights are ignored; if more, remaining nodes get `None`. ([rustworkx.org][13])

**Important special case:**
If `rows` and `cols` are **not** specified and you provide `weights`, rustworkx creates a **linear graph** containing the weights (as described in the docs). ([rustworkx.org][13])

Directed grid:

* `directed_grid_graph(..., bidirectional=False, multigraph=True)` and docs note edges “propagate towards right and bottom direction” when `bidirectional=False`. ([rustworkx.org][14])

---

## A6) Hexagonal lattice graphs: periodicity + embedded positions

* `hexagonal_lattice_graph(rows, cols, multigraph=True, periodic=False, with_positions=False)` ([rustworkx.org][15])

  * `periodic=True` joins boundaries and **requires** `cols` even, `rows>1`, `cols>1`. ([rustworkx.org][15])
  * `with_positions=True` assigns each node a coordinate pair `(x, y)` **as its weight**, embedding regular hexagons with side length 1. ([rustworkx.org][15])

Directed variant:

* `directed_hexagonal_lattice_graph(rows, cols, bidirectional=False, multigraph=True, periodic=False, with_positions=False)` with the same “propagate right/bottom” note when not bidirectional. ([rustworkx.org][16])

---

## A7) Binomial trees: order limits + overflow behavior

* `binomial_tree_graph(order, weights=None, multigraph=True)` ([rustworkx.org][17])
* `directed_binomial_tree_graph(order, weights=None, bidirectional=False, multigraph=True)` ([rustworkx.org][18])

**Key constraints**

* Maximum allowed `order` depends on platform: **60 on 64-bit, 29 on 32-bit**; higher raises `OverflowError`. ([rustworkx.org][18])
* Node count is `2**order` and edges `2**order - 1` (documented for directed; undirected mirrors). ([rustworkx.org][18])
* If `weights` is shorter than `2**order`, nodes are padded with `None`; overly long `weights` triggers an error. ([rustworkx.org][19])

Release notes also explicitly mention these binomial tree generators as added features. ([rustworkx.org][4])

---

## A8) Full r-ary trees: controlled branching + weight padding

* `full_rary_tree(branching_factor, num_nodes, weights=None, multigraph=True)` ([rustworkx.org][20])

  * If `len(weights) < num_nodes`, pads with `None`; if `len(weights) > num_nodes`, raises `IndexError`. ([rustworkx.org][20])

---

## A9) Composite classic graphs: lollipop + barbell

These are “glue” graphs built from complete graphs + paths, with separate weight sequences per component.

### Lollipop

* `lollipop_graph(num_mesh_nodes=None, num_path_nodes=None, mesh_weights=None, path_weights=None, multigraph=True)` ([rustworkx.org][21])
* If neither `num_path_nodes` nor `path_weights` is specified, it’s equivalent to `complete_graph()`. ([rustworkx.org][21])

### Barbell

* `barbell_graph(num_mesh_nodes=None, num_path_nodes=None, multigraph=True, mesh_weights=None, path_weights=None)` ([rustworkx.org][22])
* If `num_path_nodes` is not specified, it’s equivalent to two complete graphs joined together. ([rustworkx.org][22])

---

## A10) Specialty deterministic graphs

### Generalized Petersen graphs

* `generalized_petersen_graph(n, k, multigraph=True)` produces a graph with `2n` nodes and `3n` edges; Petersen graph is `G(5,2)`. ([rustworkx.org][23])
* Raises errors if `n`/`k` invalid or non-negative integer constraints aren’t met. ([rustworkx.org][23])

### Dorogovtsev–Goltsev–Mendes graph

* `dorogovtsev_goltsev_mendes_graph(n)` recursively builds a pseudofractal scale-free graph with `(3**n + 3)//2` nodes and `3**n` edges. ([rustworkx.org][24])

### Zachary’s Karate Club

* `karate_club_graph(multigraph=True)` generates the classic undirected social graph with **34 nodes and 78 edges**, with nodes labeled by faction. ([rustworkx.org][25])

### Heavy square + heavy hex code graphs (quantum-code motivated)

* `heavy_square_graph(d, multigraph=True)` uses a construction from arXiv 1907.09528; `d` must be odd; `d=1` returns a single node. ([rustworkx.org][26])
* Directed: `directed_heavy_square_graph(d, bidirectional=False, multigraph=True)` ([rustworkx.org][27])
* `heavy_hex_graph(d, bidirectional=False, multigraph=True)`; `d` odd; `d=1` single node; `bidirectional` controls whether edges exist both directions. ([rustworkx.org][28])
* Directed: `directed_heavy_hex_graph(d, bidirectional=False, multigraph=True)` ([rustworkx.org][29])

Release notes call out these heavy-hex/heavy-square generator additions explicitly. ([rustworkx.org][4])

---

# B) Random graph generators (top-level `rustworkx.*`)

The random generator family is listed under “Random Graph Generator Functions.” ([rustworkx.org][2])
They all take an optional `seed` for reproducibility.

---

## B1) Erdős–Rényi (binomial) `G(n,p)`: `*_gnp_random_graph`

### Undirected

* `undirected_gnp_random_graph(num_nodes, probability, seed=None)`
* For `n` nodes, considers all `n(n-1)/2` possible edges, each included independently with probability `p`. Expected edges `m = p*n(n-1)/2`. Runtime `O(n + m)`; for `0<p<1` it’s based on NetworkX’s `fast_gnp_random_graph`. ([rustworkx.org][30])

### Directed

* `directed_gnp_random_graph(num_nodes, probability, seed=None)`
* Considers `n(n-1)` possible directed edges; expected `m = p*n(n-1)`; runtime `O(n + m)` with the same “fast_gnp_random_graph-based” note for `0<p<1`. ([rustworkx.org][31])

**Edge-case determinism:** when `p=0` or `p=1`, results are always empty/complete respectively, so “randomness” disappears. ([rustworkx.org][30])

---

## B2) Erdős–Rényi with fixed edge count `G(n,m)`: `*_gnm_random_graph`

These generators explicitly state the generated graph:

* **is not a multigraph**
* **has no self loops** ([rustworkx.org][32])

### Undirected

* `undirected_gnm_random_graph(num_nodes, num_edges, seed=None)`
* Max possible edges `n(n-1)/2`. If `m` exceeds that, returns the max; if `m=0`, always empty. Seed makes results reproducible, **except** when `m=0` or `m>=max`, where seed has no effect. Complexity `O(n+m)`. ([rustworkx.org][32])

### Directed

* `directed_gnm_random_graph(num_nodes, num_edges, seed=None)`
* Max possible edges `n(n-1)` with the same seed/no-effect caveat at extremes, and `O(n+m)` complexity. ([rustworkx.org][33])

---

## B3) Stochastic block model (SBM): community-structured random graphs

### Directed

* `directed_sbm_random_graph(sizes, probabilities, loops, seed=None)`
* Block membership inferred from `sizes`; edge probability between nodes uses `probabilities[block_u][block_v]`. Complexity `O(n^2)`. `loops` toggles self-loops. ([rustworkx.org][34])

### Undirected

* `undirected_sbm_random_graph(sizes, probabilities, loops, seed=None)`
* Same model, but `probabilities` must be a **symmetric** `B x B` array. Complexity `O(n^2)`. ([rustworkx.org][35])

---

## B4) Geometric random graphs: positions + radius threshold

### Random geometric

* `random_geometric_graph(num_nodes, radius, dim=2, pos=None, p=2.0, seed=None)`
* Places nodes uniformly in the unit cube; connects pairs within `radius`. Uses Minkowski distance parameter `p` with constraint `1 <= p <= infinity` and default `p=2` (Euclidean). It also states each node has a `'pos'` attribute storing its coordinates (either provided via `pos` or generated). ([rustworkx.org][36])

### Hyperbolic random graph

* `hyperbolic_random_graph(pos, beta, r, seed=None)`
* Uses hyperboloid coordinates; dimension inferred from `pos`, and the “time” coordinate is inferred. Edges created with a sigmoid probability decreasing with hyperbolic distance; if `beta is None`, it becomes a hard threshold “connect if distance < r”. Complexity `O(n^2)`. ([rustworkx.org][37])

---

## B5) Preferential attachment: Barabási–Albert

### Undirected BA

* `barabasi_albert_graph(n, m, seed=None, initial_graph=None)`
* Grows a graph to `n` nodes by adding nodes each with `m` edges preferentially attached to high-degree nodes. It explicitly states **all nodes and edges added have weights `None`**. If no `initial_graph` is supplied, it uses `star_graph()` as a starting point; if `initial_graph` is provided, it is modified **in place**. ([rustworkx.org][38])

### Directed BA

* `directed_barabasi_albert_graph(n, m, seed=None, initial_graph=None)`
* Same high-level behavior, but notes that for extension purposes **directionality isn’t considered** (edges treated as “weak”). Also modifies `initial_graph` in place when provided, and nodes/edges added have weights `None`. ([rustworkx.org][39])

**Reproducibility note:** release notes mention a bug fix where the BA generators could produce different graphs for the same input seed; this was fixed. ([rustworkx.org][4])

---

## B6) Random bipartite graphs

### Directed bipartite

* `directed_random_bipartite_graph(num_l_nodes, num_r_nodes, probability, seed=None)`
* Creates `n+m` nodes; considers all `n*m` possible directed edges from left to right, included independently with probability `p`. ([rustworkx.org][40])

### Undirected bipartite

* `undirected_random_bipartite_graph(num_l_nodes, num_r_nodes, probability, seed=None)`
* Same idea but edges are undirected across the bipartition. ([rustworkx.org][41])

---

# C) “What’s missing?” (useful to know when you’re coming from NetworkX)

As of the current rustworkx “Random Graph Generator Functions” list, there is **no `random_regular_graph`** analogue included. ([rustworkx.org][2])
There’s also an open issue explicitly requesting a `random_regular_graph` equivalent. ([GitHub][42])

(So if you need regular graphs today, you either generate via another library and convert, or implement a small stub yourself.)

---

# D) Practical “contract” guidance for generator outputs (so upgrades don’t bite you)

If you’re using generators mainly to create **golden fixtures** for algorithm testing (highly recommended):

1. Always record `{generator_name, params, seed}` alongside the produced edge list.
2. Normalize by extracting:

   * `node_count`, `edge_count`
   * `edge_list()` (or `weighted_edge_list()` if you add weights) into a sorted canonical form.
3. For any generator with “edge-case determinism” (e.g., `p in {0,1}` or `m in {0,max}`) note that seed may not matter. ([rustworkx.org][32])


[1]: https://www.rustworkx.org/api/generators.html?utm_source=chatgpt.com "Generators - rustworkx 0.17.1"
[2]: https://www.rustworkx.org/api/random_graph_generator_functions.html?utm_source=chatgpt.com "Random Graph Generator Functions"
[3]: https://www.rustworkx.org/apiref/rustworkx.generators.cycle_graph.html "rustworkx.generators.cycle_graph - rustworkx 0.17.1"
[4]: https://www.rustworkx.org/release_notes.html?utm_source=chatgpt.com "Release Notes - rustworkx 0.17.1"
[5]: https://www.rustworkx.org/apiref/rustworkx.generators.directed_cycle_graph.html "rustworkx.generators.directed_cycle_graph - rustworkx 0.17.1"
[6]: https://www.rustworkx.org/apiref/rustworkx.generators.directed_star_graph.html?utm_source=chatgpt.com "rustworkx.generators.directed_star_graph"
[7]: https://www.rustworkx.org/apiref/rustworkx.generators.path_graph.html?utm_source=chatgpt.com "rustworkx.generators.path_graph"
[8]: https://www.rustworkx.org/apiref/rustworkx.generators.directed_path_graph.html?utm_source=chatgpt.com "rustworkx.generators.directed_path_graph"
[9]: https://www.rustworkx.org/apiref/rustworkx.generators.star_graph.html "rustworkx.generators.star_graph - rustworkx 0.17.1"
[10]: https://www.rustworkx.org/apiref/rustworkx.generators.mesh_graph.html?utm_source=chatgpt.com "rustworkx.generators.mesh_graph"
[11]: https://www.rustworkx.org/apiref/rustworkx.generators.directed_mesh_graph.html?utm_source=chatgpt.com "rustworkx.generators.directed_mesh_graph"
[12]: https://www.rustworkx.org/apiref/rustworkx.generators.directed_complete_graph.html "rustworkx.generators.directed_complete_graph - rustworkx 0.17.1"
[13]: https://www.rustworkx.org/apiref/rustworkx.generators.grid_graph.html "rustworkx.generators.grid_graph - rustworkx 0.17.1"
[14]: https://www.rustworkx.org/apiref/rustworkx.generators.directed_grid_graph.html?utm_source=chatgpt.com "rustworkx.generators.directed_grid_graph"
[15]: https://www.rustworkx.org/apiref/rustworkx.generators.hexagonal_lattice_graph.html "rustworkx.generators.hexagonal_lattice_graph - rustworkx 0.17.1"
[16]: https://www.rustworkx.org/apiref/rustworkx.generators.directed_hexagonal_lattice_graph.html?utm_source=chatgpt.com "rustworkx.generators.directed_hexagonal_lattice_graph"
[17]: https://www.rustworkx.org/apiref/rustworkx.generators.binomial_tree_graph.html?utm_source=chatgpt.com "rustworkx.generators.binomial_tree_graph"
[18]: https://www.rustworkx.org/apiref/rustworkx.generators.directed_binomial_tree_graph.html?utm_source=chatgpt.com "rustworkx.generators.directed_binomial_tree_graph"
[19]: https://www.rustworkx.org/stable/0.13/apiref/rustworkx.generators.binomial_tree_graph.html?utm_source=chatgpt.com "rustworkx.generators.binomial_tree_graph"
[20]: https://www.rustworkx.org/apiref/rustworkx.generators.full_rary_tree.html "rustworkx.generators.full_rary_tree - rustworkx 0.17.1"
[21]: https://www.rustworkx.org/apiref/rustworkx.generators.lollipop_graph.html "rustworkx.generators.lollipop_graph - rustworkx 0.17.1"
[22]: https://www.rustworkx.org/apiref/rustworkx.generators.barbell_graph.html "rustworkx.generators.barbell_graph - rustworkx 0.17.1"
[23]: https://www.rustworkx.org/apiref/rustworkx.generators.generalized_petersen_graph.html?utm_source=chatgpt.com "rustworkx.generators.generalized_petersen_graph"
[24]: https://www.rustworkx.org/apiref/rustworkx.generators.dorogovtsev_goltsev_mendes_graph.html "rustworkx.generators.dorogovtsev_goltsev_mendes_graph - rustworkx 0.17.1"
[25]: https://www.rustworkx.org/apiref/rustworkx.generators.karate_club_graph.html "rustworkx.generators.karate_club_graph - rustworkx 0.17.1"
[26]: https://www.rustworkx.org/apiref/rustworkx.generators.heavy_square_graph.html "rustworkx.generators.heavy_square_graph - rustworkx 0.17.1"
[27]: https://www.rustworkx.org/apiref/rustworkx.generators.directed_heavy_square_graph.html?utm_source=chatgpt.com "rustworkx.generators.directed_heavy_square_graph"
[28]: https://www.rustworkx.org/apiref/rustworkx.generators.heavy_hex_graph.html "rustworkx.generators.heavy_hex_graph - rustworkx 0.17.1"
[29]: https://www.rustworkx.org/apiref/rustworkx.generators.directed_heavy_hex_graph.html?utm_source=chatgpt.com "rustworkx.generators.directed_heavy_hex_graph"
[30]: https://www.rustworkx.org/apiref/rustworkx.undirected_gnp_random_graph.html "rustworkx.undirected_gnp_random_graph - rustworkx 0.17.1"
[31]: https://www.rustworkx.org/apiref/rustworkx.directed_gnp_random_graph.html "rustworkx.directed_gnp_random_graph - rustworkx 0.17.1"
[32]: https://www.rustworkx.org/apiref/rustworkx.undirected_gnm_random_graph.html "rustworkx.undirected_gnm_random_graph - rustworkx 0.17.1"
[33]: https://www.rustworkx.org/apiref/rustworkx.directed_gnm_random_graph.html "rustworkx.directed_gnm_random_graph - rustworkx 0.17.1"
[34]: https://www.rustworkx.org/apiref/rustworkx.directed_sbm_random_graph.html "rustworkx.directed_sbm_random_graph - rustworkx 0.17.1"
[35]: https://www.rustworkx.org/dev/apiref/rustworkx.undirected_sbm_random_graph.html "rustworkx.undirected_sbm_random_graph - rustworkx 0.17.1"
[36]: https://www.rustworkx.org/apiref/rustworkx.random_geometric_graph.html "rustworkx.random_geometric_graph - rustworkx 0.17.1"
[37]: https://www.rustworkx.org/apiref/rustworkx.hyperbolic_random_graph.html "rustworkx.hyperbolic_random_graph - rustworkx 0.17.1"
[38]: https://www.rustworkx.org/apiref/rustworkx.barabasi_albert_graph.html "rustworkx.barabasi_albert_graph - rustworkx 0.17.1"
[39]: https://www.rustworkx.org/apiref/rustworkx.directed_barabasi_albert_graph.html "rustworkx.directed_barabasi_albert_graph - rustworkx 0.17.1"
[40]: https://www.rustworkx.org/apiref/rustworkx.directed_random_bipartite_graph.html "rustworkx.directed_random_bipartite_graph - rustworkx 0.17.1"
[41]: https://www.rustworkx.org/apiref/rustworkx.undirected_random_bipartite_graph.html "rustworkx.undirected_random_bipartite_graph - rustworkx 0.17.1"
[42]: https://github.com/Qiskit/rustworkx/issues/1391?utm_source=chatgpt.com "Add networkx's `random_regular_graph` · Issue #1391"

# Deep dive — Layout + visualization (rustworkx)

This section is basically: **(1) generate 2D positions** (layouts) and **(2) render** (matplotlib or Graphviz). rustworkx’s layout outputs are **strictly 2-dimensional** and are typically returned as a **`Pos2DMapping`**, a read-only dict-like mapping from node indices → `[x, y]`. ([rustworkx.org][1])

---

## 1) Layout outputs: `Pos2DMapping` (the “pos contract”)

A `Pos2DMapping` is effectively:

```python
{ node_index: [x, y], ... }
```

…and it’s meant to be a drop-in, read-only dict replacement for layout positions. ([rustworkx.org][2])

That matters because:

* `mpl_draw(..., pos=...)` accepts either a **dict** or a **`Pos2DMapping`**. ([rustworkx.org][3])
* rustworkx’s own layout functions usually return `Pos2DMapping` (vs NetworkX returning a mutable dict). ([rustworkx.org][1])

---

## 2) Layout functions: what they do and the knobs that actually matter

### 2.1 `random_layout(graph, center=None, seed=None)`

Generates random positions (fast, good for initialization or “rough scatter”). ([rustworkx.org][4])

Key knobs:

* `center=(x,y)` recenters positions
* `seed=int` makes it reproducible
* Returns: `Pos2DMapping` ([rustworkx.org][4])

**Gotcha:** docs type the `graph` param as `PyGraph` here (not PyDiGraph). ([rustworkx.org][4])
(If you want randomness for directed graphs, you can still just use `spring_layout(..., seed=...)` as your default initializer.)

---

### 2.2 `spring_layout(...)` — the workhorse

`spring_layout` uses the **Fruchterman–Reingold** force-directed algorithm (edges as springs, nodes repel) and iterates until it converges (or hits iteration limit). ([rustworkx.org][5])

Signature highlights:
`spring_layout(graph, pos=None, fixed=None, k=None, repulsive_exponent=2, adaptive_cooling=True, num_iter=50, tol=1e-6, weight_fn=None, default_weight=1, scale=1, center=None, seed=None)` ([rustworkx.org][5])

Power knobs:

* **Determinism**

  * `seed` controls the random initialization when `pos=None`. ([rustworkx.org][5])
  * If you need *stable layouts across runs*, always set `seed`, or persist `pos` and reuse it as the next run’s initializer.
* **Partial “pinning”**

  * `fixed=set_of_nodes` keeps those nodes fixed — **but** it’s an error to set `fixed` without also providing `pos`. ([rustworkx.org][5])
  * This is the right pattern for incremental “add nodes, relax others” UIs.
* **Spacing**

  * `k` is the “ideal distance” between nodes; default is `1/sqrt(n)`. Raise `k` to spread nodes farther apart. ([rustworkx.org][5])
* **Physics tuning**

  * `repulsive_exponent` changes the repulsion force shape (default 2). ([rustworkx.org][5])
  * `adaptive_cooling` toggles adaptive vs linear cooling. ([rustworkx.org][5])
* **Edge weights**

  * `weight_fn(edge_payload)->float` lets you derive a spring strength from arbitrary edge payloads; otherwise `default_weight` is used. ([rustworkx.org][5])
* **Post-processing / normalization**

  * `scale` and `center` are *not used unless `fixed is None`*, and `scale=None` disables rescaling. ([rustworkx.org][5])

Return type in docs is “dict keyed by node id” (i.e., node index → coordinate list). In practice, treat it as a `dict` that’s directly feedable to `mpl_draw(pos=...)`. ([rustworkx.org][5])

---

### 2.3 `bipartite_layout(graph, first_nodes, horizontal=False, scale=1, center=None, aspect_ratio=4/3)`

Places the graph in two columns/rows:

* `first_nodes` is the set placed on the **left** (or **top** if `horizontal=True`) ([rustworkx.org][6])
* `horizontal` flips orientation
* `aspect_ratio` controls width/height ratio
* Returns `Pos2DMapping` ([rustworkx.org][6])

This is a great “semantic layout” for:

* producer→consumer graphs
* bipartite projections (e.g., files ↔ symbols, modules ↔ imports, routes ↔ handlers)

---

### 2.4 `circular_layout(graph, scale=1, center=None)`

Puts nodes evenly on a circle. ([rustworkx.org][7])
Useful for:

* small graphs (≤50 nodes) where you want symmetry
* comparing different edge sets on the same stable node order

Returns `Pos2DMapping`. ([rustworkx.org][7])

---

### 2.5 `shell_layout(graph, nlist=None, rotate=None, scale=1, center=None)`

This is the “multi-ring” layout:

* `nlist` is a list of lists of node indices, one list per shell ([rustworkx.org][8])
* `rotate` rotates each shell relative to the previous (radians) ([rustworkx.org][8])
* Returns `Pos2DMapping` ([rustworkx.org][8])

This is the most controllable layout when you already have **clusters** (e.g., subsystem communities) and want visual separation.

---

### 2.6 `spiral_layout(graph, scale=1, center=None, resolution=0.35, equidistant=False)`

Spiral placement:

* `resolution`: compactness (lower = more compressed spiral) ([rustworkx.org][9])
* `equidistant=True` forces equal spacing along the spiral ([rustworkx.org][9])
* Returns `Pos2DMapping` ([rustworkx.org][9])

Good for showing a linear ordering with a little more spatial separation than a line (e.g., topological order, time order, “ranked nodes”).

---

## 3) Visualization: `mpl_draw` vs `graphviz_draw`

### 3.1 `rustworkx.visualization.mpl_draw(graph, pos=None, ax=None, arrows=True, with_labels=False, **kwds)`

The Matplotlib drawer.

Key semantics:

* `pos` can be a dict or `Pos2DMapping`. If not specified, `mpl_draw` computes a spring layout automatically. ([rustworkx.org][3])
* For directed graphs, `arrows=True` draws arrowheads; there are `arrowstyle` and `arrow_size` knobs. ([rustworkx.org][3])
* Labels are **callbacks**:

  * `labels(node_payload)->str`
  * `edge_labels(edge_payload)->str` ([rustworkx.org][3])
* It returns a Matplotlib `Figure` when not using an interactive backend or when `ax` isn’t provided. ([rustworkx.org][3])
* Matplotlib is an optional dependency: install via `pip install matplotlib` or `pip install 'rustworkx[mpl]'`. ([rustworkx.org][3])

**When to use it**

* Small graphs
* When drawing is part of a larger Matplotlib figure (subplots, overlays)

The docs explicitly frame `mpl_draw()` as better for smaller graphs or when integrating into larger visualizations. ([rustworkx.org][10])

**Multigraph caution (historical)**
There was a documented limitation (in older rustworkx versions) where parallel edges overlapped and edge labels collapsed for multigraphs in `mpl_draw`; that issue is closed, but it’s still a good reason to sanity-check multigraph rendering or prefer Graphviz for multi-edges. ([GitHub][11])

---

### 3.2 `rustworkx.visualization.graphviz_draw(...)`

Graphviz-based rendering (Graphviz does the layout).

Signature:
`graphviz_draw(graph, node_attr_fn=None, edge_attr_fn=None, graph_attr=None, filename=None, image_type=None, method=None)` ([rustworkx.org][12])

Key knobs:

* **Dependencies:** requires `pydot`, `pillow`, and Graphviz installed (Graphviz installed separately). ([rustworkx.org][12])
* **Layout engine**: `method` chooses the Graphviz engine: `'dot'`, `'twopi'`, `'neato'`, `'circo'`, `'fdp'`, `'sfdp'` (default `'dot'`). ([rustworkx.org][12])
* **Styling via callbacks**

  * `node_attr_fn(node_payload) -> dict[str,str]`
  * `edge_attr_fn(edge_payload) -> dict[str,str]` ([rustworkx.org][12])
  * `graph_attr` is a dict of graph-level attributes, also string→string. ([rustworkx.org][12])
* **Output behavior**

  * Returns a `PIL.Image` if `filename` is not specified
  * Returns `None` if you pass `filename` (writes to disk)
  * `image_type` controls format; Graphviz supports many, but Pillow can’t load all when returning an in-memory image. ([rustworkx.org][12])

**When to use it**
The rustworkx tutorial explicitly recommends Graphviz for larger graphs (“Graphviz is a dedicated tool for drawing graphs”). ([rustworkx.org][10])

---

## 4) Determinism playbook (so layouts don’t “drift”)

If you want stable visuals across runs/CI snapshots:

1. **Don’t rely on `mpl_draw()`’s implicit layout** (it computes a spring layout if `pos` isn’t provided). ([rustworkx.org][3])
2. Compute positions explicitly:

   * `spring_layout(..., seed=...)` for stable force-directed results ([rustworkx.org][5])
   * `random_layout(..., seed=...)` for stable randomized scatter ([rustworkx.org][4])
   * or deterministic structural layouts (`bipartite_layout`, `shell_layout`, etc.) ([rustworkx.org][6])
3. Store the `pos` mapping (normalized) if you need cross-version stability.
4. If you want “incremental stability” on evolving graphs:

   * carry forward previous `pos` as initializer
   * set `fixed` for anchor nodes (requires `pos` is provided). ([rustworkx.org][5])

---

## 5) Minimal “best practice” snippets

### Deterministic Matplotlib draw

```python
import rustworkx as rx
from rustworkx.visualization import mpl_draw

g = rx.generators.karate_club_graph()
pos = rx.spring_layout(g, seed=7)  # stable

mpl_draw(
    g,
    pos=pos,
    with_labels=True,
    labels=str,           # labels(node_payload)->str
)
```

`mpl_draw` accepts `pos` (dict or Pos2DMapping) and labels are callback-based. ([rustworkx.org][3])

### Graphviz draw with semantic coloring

```python
import rustworkx as rx
from rustworkx.visualization import graphviz_draw

def node_attr(payload):
    # must return dict[str,str]
    return {"label": str(payload), "shape": "circle"}

g = rx.generators.directed_star_graph(weights=list(range(32)))
img = graphviz_draw(g, node_attr_fn=node_attr, method="sfdp")
```

Graphviz method options include `dot/twopi/neato/circo/fdp/sfdp`, and node/edge attr fns return string->string dictionaries. ([rustworkx.org][12])

---


[1]: https://www.rustworkx.org/networkx.html "rustworkx for NetworkX users - rustworkx 0.17.1"
[2]: https://www.rustworkx.org/apiref/rustworkx.Pos2DMapping.html "Pos2DMapping - rustworkx 0.17.1"
[3]: https://www.rustworkx.org/apiref/rustworkx.visualization.mpl_draw.html "rustworkx.visualization.mpl_draw - rustworkx 0.17.1"
[4]: https://www.rustworkx.org/apiref/rustworkx.random_layout.html "rustworkx.random_layout - rustworkx 0.17.1"
[5]: https://www.rustworkx.org/apiref/rustworkx.spring_layout.html "rustworkx.spring_layout - rustworkx 0.17.1"
[6]: https://www.rustworkx.org/apiref/rustworkx.bipartite_layout.html "rustworkx.bipartite_layout - rustworkx 0.17.1"
[7]: https://www.rustworkx.org/apiref/rustworkx.circular_layout.html "rustworkx.circular_layout - rustworkx 0.17.1"
[8]: https://www.rustworkx.org/apiref/rustworkx.shell_layout.html "rustworkx.shell_layout - rustworkx 0.17.1"
[9]: https://www.rustworkx.org/apiref/rustworkx.spiral_layout.html "rustworkx.spiral_layout - rustworkx 0.17.1"
[10]: https://www.rustworkx.org/tutorial/introduction.html "Introduction to rustworkx - rustworkx 0.17.1"
[11]: https://github.com/Qiskit/rustworkx/issues/774 "`mpl_draw()` does not work for multigraphs · Issue #774 · Qiskit/rustworkx · GitHub"
[12]: https://www.rustworkx.org/apiref/rustworkx.visualization.graphviz_draw.html "rustworkx.visualization.graphviz_draw - rustworkx 0.17.1"

# Deep dive — Converters (NetworkX bridge)

## 0) What exists (and what *doesn’t*)

rustworkx ships **one built-in NetworkX bridge**:

* `rustworkx.networkx_converter(graph, keep_attributes=False)` → converts a NetworkX graph into a rustworkx `PyGraph` or `PyDiGraph`. ([rustworkx.org][1])

rustworkx **does not** ship the reverse (rustworkx → NetworkX) converter, specifically to avoid taking an optional dependency on NetworkX; the docs show a simple example you can write yourself. ([rustworkx.org][2])

---

## 1) Exact conversion semantics (what gets mapped to what)

### 1.1 Output graph type + multigraph flag

The converter chooses the output type from NetworkX’s directedness and multigraph-ness:

* `graph.is_directed()` → `PyDiGraph` else `PyGraph`
* `multigraph=graph.is_multigraph()` on the rustworkx graph constructor ([rustworkx.org][3])

So:

* `nx.Graph` → `rx.PyGraph(multigraph=False)`
* `nx.MultiGraph` → `rx.PyGraph(multigraph=True)`
* `nx.DiGraph` → `rx.PyDiGraph(multigraph=False)`
* `nx.MultiDiGraph` → `rx.PyDiGraph(multigraph=True)` ([rustworkx.org][3])

### 1.2 Node mapping: NetworkX node IDs become rustworkx **node payloads**

The implementation does:

```python
nodes = list(graph.nodes)
node_indices = dict(zip(nodes, new_graph.add_nodes_from(nodes)))
```

Meaning:

* The node payload stored in rustworkx is the **NetworkX node object itself** (the node ID), **unless** `keep_attributes=True` (next section). ([rustworkx.org][3])
* Node indices in rustworkx are assigned in **the iteration order of `graph.nodes`**.

> Practical: if you need stable rustworkx indices, you must ensure NetworkX’s node iteration order is deterministic for your graph build, or write your own “sorted converter” wrapper.

### 1.3 Edge mapping: NetworkX edge attribute dict becomes rustworkx **edge payload**

The converter calls `graph.edges(data=True)` and uses the third item as the rustworkx edge payload:

````python
new_graph.add_edges_from([
  (node_indices[u], node_indices[v], data_dict)
  for (u, v, data_dict) in graph.edges(data=True)
])
``` :contentReference[oaicite:5]{index=5}

So:
- Edge payload in rustworkx becomes the NetworkX **edge attribute dict** (e.g., `{"weight": 3, "kind": "call"}`).
- For Multi(Graph/DiGraph), `edges(data=True)` returns repeated `(u, v, dict)` tuples when multiple edges exist; **edge keys are not included unless you ask for them** in NetworkX. NetworkX documents that edge keys are only returned when `keys=True`. :contentReference[oaicite:6]{index=6}  
  - Therefore: **edge keys are not preserved** by `networkx_converter()` unless you have already stored the key inside the edge attribute dict yourself.

### 1.4 What is *not* copied
From the implementation:
- NetworkX **graph-level attributes** (`G.graph`) are not transferred into `rustworkx_graph.attrs` (the converter never touches `attrs`). :contentReference[oaicite:7]{index=7}  
- There is also no explicit “edge attribute shaping” (it just passes the dict through). :contentReference[oaicite:8]{index=8}

---

## 2) `keep_attributes=True`: node payload becomes an attribute dict (+ reserved key)

When `keep_attributes=True`, the converter replaces each node payload with a dict of that node’s NetworkX attributes and injects a reserved key:

- The node payload becomes a dict of node attributes
- It writes `attributes["__networkx_node__"] = node`
- It sets `new_graph[node_index] = attributes` :contentReference[oaicite:9]{index=9}

**Implications you should treat as part of the contract**
- You can always recover the original NetworkX node ID via `payload["__networkx_node__"]`. :contentReference[oaicite:10]{index=10}
- **Key collision risk:** if your original node attributes already had `__networkx_node__`, it will be overwritten. (The docs explicitly reserve that key.) :contentReference[oaicite:11]{index=11}
- The implementation directly mutates the attribute dict returned by `graph.nodes[node]` before assigning it to rustworkx. In NetworkX, those dicts are the node-attribute dicts, so this **may also mutate your input NetworkX graph** by inserting `__networkx_node__`. (That follows directly from the code’s `attributes["__networkx_node__"] = node` line.) :contentReference[oaicite:12]{index=12}

---

## 3) Best-practice patterns you’ll actually use

### 3.1 Reconstruct the “node ↔ index” mapping after conversion
`networkx_converter()` doesn’t return `node_indices`, but you can rebuild it deterministically from payloads.

```python
import rustworkx as rx

def build_nx_index_maps(g: rx.PyGraph | rx.PyDiGraph, keep_attributes: bool):
    idx_to_nx = {}
    for idx in g.node_indices():
        payload = g[idx]
        nx_node = payload["__networkx_node__"] if keep_attributes else payload
        idx_to_nx[idx] = nx_node
    nx_to_idx = {nx_node: idx for idx, nx_node in idx_to_nx.items()}
    return nx_to_idx, idx_to_nx
````

This is the key enabling trick for “convert → run rustworkx algo → map results back to NetworkX nodes”.

### 3.2 Weight extraction for algorithms (the attribute-dict edge payload case)

Because edges become dicts, you’ll almost always do:

```python
weight_fn = lambda edge_attrs: float(edge_attrs.get("weight", 1.0))
```

This matches rustworkx’s broader design of using callbacks to turn payload objects into numeric weights.

### 3.3 If you care about MultiGraph edge keys, do a custom converter

NetworkX only includes keys if `keys=True`. ([NetworkX][4])
So for MultiGraph/MultiDiGraph you typically want:

```python
def nx_to_rx_preserve_keys(nxg, keep_attributes=False, key_field="__nx_key__"):
    import rustworkx as rx
    out = rx.PyDiGraph(multigraph=True) if nxg.is_directed() else rx.PyGraph(multigraph=True)

    nodes = list(nxg.nodes)
    node_to_idx = dict(zip(nodes, out.add_nodes_from(nodes)))

    # preserve (u,v,key,data)
    for u, v, k, d in nxg.edges(keys=True, data=True):
        payload = dict(d)
        payload[key_field] = k
        out.add_edge(node_to_idx[u], node_to_idx[v], payload)

    if keep_attributes:
        for node, idx in node_to_idx.items():
            attrs = dict(nxg.nodes[node])
            attrs["__networkx_node__"] = node
            out[idx] = attrs

    return out
```

That’s the only reliable way to round-trip MultiGraph edges with stable identity.

---

## 4) Reverse conversion (rustworkx → NetworkX): what rustworkx docs show, and what you should improve

### 4.1 What the docs provide (simple, but assumes node payloads are usable as NX nodes)

The NetworkX users guide shows a straightforward reverse conversion using `weighted_edge_list()` and `graph[payload]` lookups, selecting NetworkX graph classes based on `graph.multigraph` and graph type. ([rustworkx.org][2])

That example is good as a *starter*, but in real systems it breaks when:

* rustworkx node payloads are **unhashable** (NetworkX nodes must be hashable)
* you want to preserve node attributes cleanly (as attributes, not as node IDs)
* you want to preserve MultiGraph edge keys

### 4.2 A more “production” reverse converter pattern

Choose **one of two** output node-ID policies:

**Policy A (safe): use rustworkx node indices as NetworkX node IDs**

* Always works (ints are hashable)
* You can store original payload/attrs as node attributes

**Policy B (convenient): use node payload as NetworkX node IDs**

* Only if payload is hashable + stable

Here’s a robust “Policy A” skeleton:

```python
def rx_to_nx_by_index(rxg, node_attrs_from_payload=True, edge_attrs_from_payload=True):
    import networkx as nx
    import rustworkx as rx

    if isinstance(rxg, rx.PyGraph):
        G = nx.MultiGraph() if rxg.multigraph else nx.Graph()
    else:
        G = nx.MultiDiGraph() if rxg.multigraph else nx.DiGraph()

    # nodes: use indices as node IDs
    for idx in rxg.node_indices():
        payload = rxg[idx]
        attrs = {}
        if node_attrs_from_payload:
            # common convention: payload dict becomes node attrs; otherwise store repr
            attrs["payload"] = payload
        G.add_node(idx, **attrs)

    # edges: preserve endpoints as indices
    for u, v, epayload in rxg.weighted_edge_list():
        eattrs = {}
        if edge_attrs_from_payload:
            if isinstance(epayload, dict):
                eattrs.update(epayload)
            else:
                eattrs["payload"] = epayload
        G.add_edge(u, v, **eattrs)

    return G
```

This makes reverse conversion *total* (never fails on unhashables) and keeps semantics explicit.

---

## 5) Micro-fixtures you want if you’re going to rely on this in pipelines

1. **Directedness/multigraph mapping**

* `nx.MultiDiGraph` converts to `PyDiGraph(multigraph=True)` ([rustworkx.org][3])

2. **keep_attributes inserts `__networkx_node__`**

* Node payload dict contains the original node at that key ([rustworkx.org][1])

3. **MultiGraph edge keys are not preserved by default**

* Because `networkx_converter` calls `edges(data=True)` not `edges(keys=True, data=True)` ([rustworkx.org][3])

---


[1]: https://www.rustworkx.org/apiref/rustworkx.networkx_converter.html "rustworkx.networkx_converter - rustworkx 0.17.1"
[2]: https://www.rustworkx.org/networkx.html "rustworkx for NetworkX users - rustworkx 0.17.1"
[3]: https://www.rustworkx.org/_modules/rustworkx.html "rustworkx - rustworkx 0.17.1"
[4]: https://networkx.org/documentation/stable/reference/classes/generated/networkx.MultiGraph.edges.html?utm_source=chatgpt.com "MultiGraph.edges — NetworkX 3.6.1 documentation"


# Deep dive — Serialization + interchange (rustworkx)

Rustworkx’s “serialization” surface is intentionally **small**: it’s basically **Node-Link JSON** (good for interchange + JS tooling) and **GraphML** (good for interchange with graph tools). The docs group these under “Serialization”: `node_link_json`, `parse_node_link_json`, `from_node_link_json_file`, `read_graphml`, `write_graphml`. ([rustworkx.org][1])

---

## 1) Node-Link JSON (write side)

### 1.1 API: universal + typed

* **Universal**: `rustworkx.node_link_json(graph, path=None, graph_attrs=None, node_attrs=None, edge_attrs=None)` where `graph` can be `PyGraph` or `PyDiGraph`. ([rustworkx.org][2])
* **Typed variants** exist (e.g. `digraph_node_link_json(PyDiGraph, ...)`) but are effectively the same interface, just type-restricted. ([rustworkx.org][3])

### 1.2 Output / file behavior (important for “contracting”)

* If `path` is **not** provided: returns a **JSON string**. ([rustworkx.org][2])
* If `path` **is** provided: it writes to that file and returns `None`. ([rustworkx.org][2])

### 1.3 The key design constraint: you must supply stringy attribute extractors

`node_link_json` **does not attempt to serialize arbitrary Python payloads**. Instead, you provide extractors:

* `graph_attrs(graph.attrs) -> dict[str, str]` (included as attributes in the JSON)
* `node_attrs(node_payload) -> dict[str, str]` (becomes the node’s `data` field)
* `edge_attrs(edge_payload) -> dict[str, str]` (becomes the edge’s `data` field) ([rustworkx.org][2])

If `graph_attrs` returns anything other than a `dict[str, str]`, the docs say an exception is raised. ([rustworkx.org][2])
(And in practice, you should assume the same “must be `dict[str,str]`” contract for `node_attrs` / `edge_attrs` too, because that’s what the function expects to embed.)

### 1.4 What the JSON looks like (and the “data:null” gotcha)

The docs don’t show an example payload, but users have reported output shaped like:

```json
{
  "directed": false,
  "multigraph": true,
  "attrs": null,
  "nodes": [{"id":0,"data":null}, ...],
  "links": [{"source":0,"target":1,"id":0,"data":null}, ...]
}
```

…where `data` is `null` unless you provide `node_attrs` / `edge_attrs`. ([GitHub][4])

**Operational takeaway:** treat **extractors as mandatory** if you want meaningful node/edge payloads in Node-Link JSON.

---

## 2) Node-Link JSON (read side)

Rustworkx supports both “parse from string” and “parse from file”:

* `parse_node_link_json(data: str, graph_attrs=None, node_attrs=None, edge_attrs=None)` ([rustworkx.org][5])
* `from_node_link_json_file(path: str, graph_attrs=None, node_attrs=None, edge_attrs=None)` ([rustworkx.org][6])

### 2.1 Read-side adapters invert the write-side constraints

On read, rustworkx gives you **string dicts** from the JSON and lets you map them back to richer Python objects:

* `graph_attrs(dict[str,str]) -> Any` becomes the output graph’s `.attrs`
* `node_attrs(dict[str,str]) -> Any` becomes each node payload
* `edge_attrs(dict[str,str]) -> Any` becomes each edge payload ([rustworkx.org][5])

If you don’t pass these callables, the dicts of strings are used as-is for payloads/attrs. ([rustworkx.org][5])

### 2.2 “Lossless” pattern for complex payloads (recommended)

Because write-side requires `dict[str,str]`, your options are:

1. **Flatten to strings** (best for human-readable interop)
2. **Embed structured JSON inside a string** (best for lossless roundtrip)

Example (lossless-ish) pattern:

```python
import json
import rustworkx as rx

def node_attrs(payload) -> dict[str, str]:
    # payload is your Python object
    return {
        "kind": payload["kind"],
        "stable_id": payload["stable_id"],
        "blob": json.dumps(payload, sort_keys=True),  # lossless payload-in-string
    }

def node_decode(d: dict[str, str]):
    # d is dict[str,str] parsed from JSON
    return json.loads(d["blob"])

g = rx.PyDiGraph()
a = g.add_node({"kind": "func", "stable_id": "sym:foo", "x": 1})
b = g.add_node({"kind": "func", "stable_id": "sym:bar", "x": 2})
g.add_edge(a, b, {"kind": "calls", "weight": 3})

s = rx.node_link_json(
    g,
    node_attrs=node_attrs,
    edge_attrs=lambda e: {"blob": json.dumps(e, sort_keys=True)},
)

g2 = rx.parse_node_link_json(
    s,
    node_attrs=node_decode,
    edge_attrs=lambda d: json.loads(d["blob"]),
)
```

This aligns perfectly with rustworkx’s documented “write dict[str,str] / read dict[str,str] and map to Python object” contract. ([rustworkx.org][2])

---

## 3) Node-Link JSON failure modes you should explicitly gate

### 3.1 Type contract violations

* `graph_attrs` must return `dict[str,str]` or an exception is raised. ([rustworkx.org][2])
  In practice you should treat *all* three extractors as “must return dict[str,str]”.

### 3.2 Dedicated JSON exceptions exist

Rustworkx defines `JSONSerializationError` and `JSONDeserializationError`. ([rustworkx.org][7])
(When you harden your pipeline, treat these as “expected operational errors” for bad payloads / invalid JSON and surface them cleanly.)

---

## 4) GraphML (read side)

### 4.1 API and return type

`read_graphml(path, /, compression=None)` reads a GraphML file and returns a **list of graphs** (`PyGraph` and/or `PyDiGraph`). ([rustworkx.org][8])

### 4.2 Explicit limitations (important)

Rustworkx’s GraphML implementation **does not support**:

* mixed graphs (directed + undirected edges together)
* hyperedges
* nested graphs
* ports ([rustworkx.org][8])

### 4.3 Where GraphML “graph domain” attributes go

* GraphML attributes with **graph** domain are stored in the graph’s `.attrs` field. ([rustworkx.org][8])

### 4.4 Compression support

* The API accepts `compression=`. ([rustworkx.org][8])
* Release notes indicate rustworkx can read gzip-compressed GraphML and recognizes `.graphmlz` and `.gz` extensions (and can force gzip via `compression`). ([rustworkx.org][9])

### 4.5 Error model

* Raises `RuntimeError` on parse errors. ([rustworkx.org][8])

---

## 5) GraphML (write side)

### 5.1 API

`write_graphml(graph, path, /, keys=None, compression=None)` writes a `PyGraph` or `PyDiGraph` to a GraphML file. ([rustworkx.org][10])

### 5.2 GraphML “graph domain” attributes come from `.attrs`

* Graph-domain attributes are written from the graph’s `.attrs` field. ([rustworkx.org][10])

### 5.3 Keys + inference (the determinism knob)

* `keys` is an optional list of GraphML key definitions. If omitted, keys are **inferred from the graph data**. ([rustworkx.org][10])

**Practical advice:** if you care about upgrade-stable output, you should aim to **provide explicit `keys`** (so inference changes don’t drift your schema). The docs don’t spell out the structure of the key definitions, so you’ll typically discover the expected structure via a small trial file in your environment and then pin it.

### 5.4 Same GraphML limitations + error model

* Same “no mixed/hyperedges/nested/ports” limitation. ([rustworkx.org][10])
* Raises `RuntimeError` on write errors. ([rustworkx.org][10])

---

## 6) Choosing between Node-Link JSON vs GraphML (what I’d do in a CodeIntel-ish pipeline)

### Use Node-Link JSON when you want…

* **A crisp “wire format”** for CI goldens and diffs (it’s a string, trivial to snapshot)
* Easy downstream consumers (JS viz, lightweight tooling)
* Tight control over payload by enforcing `dict[str,str]` + your own encode/decode policy ([rustworkx.org][2])

### Use GraphML when you want…

* Interop with graph tooling that expects GraphML (Gephi, yEd, etc.)
* File-based interchange, with optional compression, and support for multiple graphs per file (hence `read_graphml` returning a list) ([rustworkx.org][8])

---

## 7) What I’d add to your existing “Arrow contracts” pack next

You already have normalizers for `PathMapping` / `AllPairsPathMapping` / `CentralityMapping` / `EdgeList`. The next tight add-on for serialization would be:

1. A **Node-Link JSON contract**:

   * canonical `node_attrs`/`edge_attrs` encoder + matching decoders
   * golden snapshot of the **parsed graph’s canonical edge list + payload hashes** (instead of snapshotting raw JSON ordering)

2. A **GraphML contract**:

   * write → read roundtrip harness
   * normalize to your stable edge-list table and assert equality (plus `.attrs` roundtrip)

If you want, I can generate that as an additional module + pytest fixtures that drop directly into the `rustworkx_arrow_contracts` package you downloaded earlier.

[1]: https://www.rustworkx.org/api/serialization.html "Serialization - rustworkx 0.17.1"
[2]: https://www.rustworkx.org/apiref/rustworkx.node_link_json.html "rustworkx.node_link_json - rustworkx 0.17.1"
[3]: https://www.rustworkx.org/apiref/rustworkx.digraph_node_link_json.html "rustworkx.digraph_node_link_json - rustworkx 0.17.1"
[4]: https://github.com/Qiskit/rustworkx/issues/1298?utm_source=chatgpt.com "Serializing data with node_link_json returns \"data\": null"
[5]: https://www.rustworkx.org/apiref/rustworkx.parse_node_link_json.html "rustworkx.parse_node_link_json - rustworkx 0.17.1"
[6]: https://www.rustworkx.org/apiref/rustworkx.from_node_link_json_file.html "rustworkx.from_node_link_json_file - rustworkx 0.17.1"
[7]: https://www.rustworkx.org/api/exceptions.html?utm_source=chatgpt.com "Exceptions - rustworkx 0.17.1"
[8]: https://www.rustworkx.org/apiref/rustworkx.read_graphml.html "rustworkx.read_graphml - rustworkx 0.17.1"
[9]: https://www.rustworkx.org/release_notes.html?utm_source=chatgpt.com "Release Notes - rustworkx 0.17.1"
[10]: https://www.rustworkx.org/apiref/rustworkx.write_graphml.html "rustworkx.write_graphml - rustworkx 0.17.1"


# Deep dive — Type-specific algorithm functions (`graph_*` / `digraph_*`) that we haven’t fully covered yet

You already have the big idea (universal functions dispatch to typed ones). This pass goes deeper on **what’s materially different** once you *choose* to call the typed APIs directly: **signature shape**, **callback conventions**, **return-type conventions**, **multithreading knobs**, and the **typed-only “specials”** that don’t really have universal twins.

Rustworkx’s docs explicitly maintain separate “API functions for PyGraph” and “API functions for PyDigraph” pages and note that universal functions call these typed ones internally based on data type. ([Rustworkx][1])

---

## 1) Typed vs universal: what changes *at the callsite*

### 1.1 Typed functions are strict about graph type

* `graph_*` functions accept **only** `PyGraph`
* `digraph_*` functions accept **only** `PyDiGraph`/`PyDAG` (where applicable) ([Rustworkx][1])

Universal wrappers use `functools.singledispatch` under the hood and raise `TypeError` for invalid input types. For example, `adjacency_matrix()` dispatches to `graph_adjacency_matrix()` or `digraph_adjacency_matrix()` based on graph type. ([Rustworkx][2])

### 1.2 Typed signatures are often **positional-only**

You’ll see the `/` marker in many typed function signatures (positional-only parameters). Example:

* `graph_dijkstra_shortest_path_lengths(graph, node, edge_cost_fn, /, goal=None)` ([Rustworkx][3])
* `graph_all_pairs_dijkstra_shortest_paths(graph, edge_cost_fn, /)` ([Rustworkx][4])

This matters for “contract pack” style code: your wrappers should **accept keyword args** and then call typed functions positionally to keep callsites clean.

### 1.3 The “edge weight callback” convention changes name depending on the family

A frequent source of confusion:

* Many **universal** shortest-path functions accept `weight_fn` + `default_weight`. Example:
  `graph_dijkstra_shortest_paths(..., weight_fn=None, default_weight=..., as_undirected=...)` ([Rustworkx][5])
* Many **typed** “**path_lengths” and “all_pairs**” functions require `edge_cost_fn` (no default). Example:
  `graph_dijkstra_shortest_path_lengths(..., edge_cost_fn, ...)` ([Rustworkx][3])

In other words: **typed functions often force you to be explicit** about how to turn edge payloads into floats, while some universal functions offer a fallback default.

---

## 2) Typed return types: what you get back (and how to normalize)

Rustworkx uses a set of custom container types for performance (read-only mapping/sequence protocols). ([Rustworkx][6])
Typed algorithms are where you’ll run into these most.

### 2.1 Single-source shortest paths

Common pattern:

* `*_dijkstra_shortest_paths` → “destination → node-index list path” mapping (often `PathMapping`-like)
* `*_dijkstra_shortest_path_lengths` → `PathLengthMapping` (explicitly a custom mapping return type) ([Rustworkx][3])

**Normalization rule (for Arrow / DuckDB):**

* Convert mapping → `dict(...)`
* Sort keys explicitly
* Store `path` as `list<uint32>` and optional `cost` as `float64`

(You already implemented this pattern in your Arrow contracts module; the key point here is: typed calls are the ones most likely to hand you these custom mapping types.)

### 2.2 All-pairs shortest paths: nested mapping container

* `digraph_all_pairs_dijkstra_shortest_paths` (typed) is explicitly **multithreaded** and returns an “all pairs” mapping. ([Rustworkx][7])
* The `AllPairsPathMapping` class is documented as a **read-only mapping** of “source → (target → path)” and supports mapping methods like `keys/items/values`. ([Rustworkx][8])

**Normalization rule:**

* Iterate sources in sorted order, targets in sorted order, emit `(source, target, path)` rows.
* Record `RAYON_NUM_THREADS` (or at least “parallel on/off”) because multithreading can affect performance and (occasionally) tie-breaking in “pick one of many equal solutions” scenarios.

### 2.3 “All shortest paths” is list-of-lists (and can blow up)

* `digraph_all_shortest_paths` returns **a list of paths** (each a list of node indices), not a mapping. ([Rustworkx][9])
* Universal `all_shortest_paths` is the same shape (list of paths). ([Rustworkx][10])
* The single-source “all shortest paths” variants warn that they can return an **exponential** number of paths, especially with zero-weight edges, and suggest `dijkstra_shortest_paths` for a single path. ([Rustworkx][11])

**Contract guidance:** if you store these, you need an output cap (max paths, max total nodes) and a deterministic truncation policy.

---

## 3) Typed implementations matter most for: matrices, layouts, and JSON emitters

### 3.1 Adjacency matrices: typed functions are the “real work”

* Universal `adjacency_matrix(graph, weight_fn=None, default_weight=1.0, null_value=0.0)` accepts either graph type and states that **multiple edges are summed** in the output matrix. ([Rustworkx][12])
* Internally it dispatches to `digraph_adjacency_matrix` / `graph_adjacency_matrix`. ([Rustworkx][2])
* `digraph_adjacency_matrix` documents the same `weight_fn` + `default_weight` pattern. ([Rustworkx][13])

**Why typed matters:** if you want to guarantee “directed adjacency semantics” (and avoid accidental conversion to undirected in your own code), call the typed function directly.

### 3.2 Typed layout functions exist (`graph_*_layout`, `digraph_*_layout`)

Even though you already deep-dived layouts/visualization, it’s worth noting that the typed layout functions are first-class and mirror the universal ones. For example, `digraph_spring_layout` documents `weight_fn`, `default_weight`, and `scale=None` semantics (no rescaling if `scale` is `None`). ([Rustworkx][14])

**Contract tip:** if your pipeline is type-partitioned (PyGraph vs PyDiGraph), using typed layout calls avoids universal dispatch and makes static typing cleaner.

### 3.3 Typed node-link JSON emitters exist (`graph_node_link_json`, `digraph_node_link_json`)

If you’re building a “typed-only module surface” (common in strict codebases), prefer the typed JSON functions rather than universal wrappers; they’re the same semantics, just restricted by input type. (The typed functions are listed under the type-specific API pages.) ([Rustworkx][1])

---

## 4) Multithreading + determinism: typed functions sometimes document threading more clearly

A concrete example:

* `digraph_all_pairs_dijkstra_shortest_paths` explicitly states it is **multithreaded**, uses a thread pool sized to CPUs by default, and can be tuned with `RAYON_NUM_THREADS`. ([Rustworkx][7])

In practice:

* **Correctness**: deterministic for most uses (same costs → same lengths), but
* **Choice among equal options**: when multiple equal shortest paths exist, *which* path is returned can depend on internal ordering. (Even if stable in practice, you should treat tie-breaking as an implementation detail unless the docs promise otherwise.)

So: if you store “the path” (not just the length), it’s worth pinning a fixture that has a deterministic tie-breaker (unique weights) when you need golden stability.

---

## 5) Typed-only “specials” you should treat as a separate cluster

These are in the typed namespace and are easy to miss if you only live in universal wrappers.

### 5.1 `graph_token_swapper` (PyGraph-only)

* `graph_token_swapper(graph, mapping, /, trials=None, seed=None, parallel_threshold=50)`
* Implements an approximately optimal token swapping algorithm; supports partial mappings. ([Rustworkx][15])

**When it’s useful:** any “assignment on a graph” style scheduling / routing problem where tokens (labels) must be permuted via adjacent swaps.

**Contract knobs:** `seed` and `trials` are *semantic* knobs (change solution quality), not just performance.

### 5.2 `local_complement` (PyGraph-only)

* Local complementation of a node in an undirected graph
* Assumes **no self loops**
* Returns an error if `multigraph=True` ([Rustworkx][16])

**When it’s useful:** graph transformation problems (especially in specialized domains); in general pipelines, treat it as “requires a simple graph”.

### 5.3 Directed-only algorithm that is not a universal dispatch pair: `digraph_maximum_bisimulation`

* Signature: `digraph_maximum_bisimulation(graph: PyDiGraph)`
* Returns `RelationalCoarsestPartition` (sequence-like container of blocks of node indices)
* Based on Paige–Tarjan partition refinement. ([Rustworkx][17])

**Contract guidance:** normalize as `(block_id, node)` rows, with nodes sorted within each block.

---

## 6) A practical “typed API wrapper” pattern (for stable internal use)

If you want your own internal surface to be **deterministic + schema-stable**, I’d recommend a wrapper layer with:

1. **Typed entrypoints only** (`graph_*` / `digraph_*`), no universal calls inside the pipeline
2. A single **edge-cost extractor policy** per pipeline step
3. A standard “return normalizer” per return-class family (mapping, nested mapping, list-of-paths, edge list)

Example: unify Dijkstra costs across typed calls:

```python
def edge_cost_from_dict(edge_payload) -> float:
    # One policy everywhere.
    return float(edge_payload.get("weight", 1.0))

# PyGraph
lengths = rx.graph_dijkstra_shortest_path_lengths(g, src, edge_cost_from_dict)
paths_all = rx.graph_all_pairs_dijkstra_shortest_paths(g, edge_cost_from_dict)

# PyDiGraph
paths_all_d = rx.digraph_all_pairs_dijkstra_shortest_paths(dg, edge_cost_from_dict)
```

Then feed each result into your Arrow normalizers (which you already have in `rustworkx_arrow_contracts.zip`).

---


[1]: https://www.rustworkx.org/api/pydigraph_api_functions.html?utm_source=chatgpt.com "API functions for PyDigraph"
[2]: https://www.rustworkx.org/stable/0.12/_modules/rustworkx.html?utm_source=chatgpt.com "rustworkx 0.12.1 documentation"
[3]: https://www.rustworkx.org/apiref/rustworkx.graph_dijkstra_shortest_path_lengths.html?utm_source=chatgpt.com "rustworkx.graph_dijkstra_shortest_path_lengths"
[4]: https://www.rustworkx.org/apiref/rustworkx.graph_all_pairs_dijkstra_shortest_paths.html?utm_source=chatgpt.com "rustworkx.graph_all_pairs_dijkstra_shortest_paths"
[5]: https://www.rustworkx.org/apiref/rustworkx.graph_dijkstra_shortest_paths.html?utm_source=chatgpt.com "rustworkx.graph_dijkstra_shortest_paths"
[6]: https://www.rustworkx.org/api/custom_return_types.html?utm_source=chatgpt.com "Custom Return Types - rustworkx 0.17.1"
[7]: https://www.rustworkx.org/apiref/rustworkx.digraph_all_pairs_dijkstra_shortest_paths.html?utm_source=chatgpt.com "rustworkx.digraph_all_pairs_dijkstra_shortest_paths"
[8]: https://www.rustworkx.org/apiref/rustworkx.AllPairsPathMapping.html?utm_source=chatgpt.com "AllPairsPathMapping - rustworkx 0.17.1"
[9]: https://www.rustworkx.org/apiref/rustworkx.digraph_all_shortest_paths.html?utm_source=chatgpt.com "rustworkx.digraph_all_shortest_paths"
[10]: https://www.rustworkx.org/apiref/rustworkx.all_shortest_paths.html?utm_source=chatgpt.com "rustworkx.all_shortest_paths"
[11]: https://www.rustworkx.org/apiref/rustworkx.single_source_all_shortest_paths.html?utm_source=chatgpt.com "rustworkx.single_source_all_shortest_paths"
[12]: https://www.rustworkx.org/apiref/rustworkx.adjacency_matrix.html?utm_source=chatgpt.com "rustworkx.adjacency_matrix"
[13]: https://www.rustworkx.org/apiref/rustworkx.digraph_adjacency_matrix.html?utm_source=chatgpt.com "rustworkx.digraph_adjacency_matrix"
[14]: https://www.rustworkx.org/apiref/rustworkx.digraph_spring_layout.html?utm_source=chatgpt.com "rustworkx.digraph_spring_layout"
[15]: https://www.rustworkx.org/apiref/rustworkx.graph_token_swapper.html?utm_source=chatgpt.com "rustworkx.graph_token_swapper"
[16]: https://www.rustworkx.org/apiref/rustworkx.local_complement.html?utm_source=chatgpt.com "rustworkx.local_complement"
[17]: https://www.rustworkx.org/apiref/rustworkx.digraph_maximum_bisimulation.html?utm_source=chatgpt.com "rustworkx.digraph_maximum_bisimulation"

# Deep dive — rustworkx exceptions (error model)

rustworkx’s “error model” is a mix of:

1. **rustworkx-defined exception classes** (e.g. `DAGHasCycle`, `NoEdgeBetweenNodes`, `NullGraph`, etc.) plus traversal control-flow exceptions (`visit.StopSearch`, `visit.PruneSearch`), and
2. **standard Python exceptions** (`IndexError`, `ValueError`, `TypeError`, `RuntimeError`, `OverflowError`, …) for parameter validation, invalid indices, and some I/O boundaries.

The docs enumerate the core rustworkx exceptions and their one-line meanings. ([rustworkx.org][1])

---

## 1) The exception taxonomy that’s actually useful in pipelines

### A) Graph-structure / index validity

**These are “you called with a bad node/edge handle” errors.**

* `InvalidNode`: “The provided node is invalid.” ([rustworkx.org][1])
* `NoEdgeBetweenNodes`: “There is no edge present between the provided nodes.” ([rustworkx.org][1])
* Standard `IndexError` also appears for invalid indices in some APIs (example: `PyGraph.get_node_data` explicitly raises `IndexError` on invalid node index). ([rustworkx.org][2])

**Key nuance (important):** rustworkx is not perfectly uniform: some APIs raise `InvalidNode`, others raise `IndexError`, and some return an empty result instead of raising (example: `PyDiGraph.out_edge_indices` returns an empty list if the node index isn’t present). ([rustworkx.org][3])

---

### B) Graph property preconditions (DAG/bipartite/empty graph)

**These mean: “your graph doesn’t satisfy the required property.”**

* `NullGraph`: “Invalid operation on a null graph.” ([rustworkx.org][4])

  * Example: `is_connected(graph)` raises `NullGraph` if an empty graph is passed. ([rustworkx.org][5])
  * Dominance functions (`immediate_dominators`, `dominance_frontiers`) raise `NullGraph` if the graph is empty. ([rustworkx.org][6])
* `DAGHasCycle`: “The specified Directed Graph has a cycle and can't be treated as a DAG.” ([rustworkx.org][1])

  * `topological_sort()` explicitly raises `DAGHasCycle` if a cycle is encountered. ([rustworkx.org][7])
* `GraphNotBipartite`: “Graph is not bipartite.” ([rustworkx.org][1])

  * **But** many “bipartite-ish” APIs choose *sentinels* instead of exceptions (example: `graph_bipartite_edge_color()` returns `None` if not bipartite). ([rustworkx.org][8])

**Operational takeaway:** treat these as “precondition failures” and decide if you want:

* strict mode: raise/propagate, or
* soft mode: return a sentinel / partial result (see `TopologicalSorter` below).

---

### C) Mutation-time constraints (cycle prevention)

This is the “you tried to add an edge that violates the configured invariant” class.

* `DAGWouldCycle`: “Performing this operation would result in trying to add a cycle to a DAG.” ([rustworkx.org][1])
* When `PyDiGraph.check_cycle=True`, adding edges that would introduce a cycle will raise `DAGWouldCycle` (documented). ([rustworkx.org][9])

**Doc nuance:** the `PyDiGraph.add_edge` page describes the cycle-add rejection as a `PyIndexError`. ([rustworkx.org][10])
In practice, you should treat “cycle insertion rejected” as a single category and catch both `DAGWouldCycle` *and* `IndexError` if you want a future-proof wrapper.

---

### D) Algorithm-domain “no solution / undefined”

These aren’t “bad inputs”, they’re “the math says the requested thing doesn’t exist”.

* `NegativeCycle`: “Negative Cycle found on shortest-path algorithm.” ([rustworkx.org][1])

  * Bellman–Ford shortest path APIs raise `NegativeCycle` when a negative cycle exists and shortest paths are undefined. ([rustworkx.org][11])
* `NoPathFound`: “No path was found between the specified nodes.” ([rustworkx.org][1])

  * Not all shortest-path APIs explicitly document it in their “Raises” section, but the exception type exists and is intended for “no route exists” semantics. ([rustworkx.org][1])
* `NoSuitableNeighbors`: “No neighbors found matching the provided predicate.” ([rustworkx.org][1])

  * Example: `PyDiGraph.find_adjacent_node_by_edge(...)` raises `NoSuitableNeighbors` if none match. ([rustworkx.org][12])

**Related standard exception:** some “finder” APIs use `ValueError` for “not found” rather than a custom exception. Example: `find_negative_cycle(...)` raises `ValueError` when there is no cycle in the graph. ([rustworkx.org][13])

---

### E) Algorithm “numerics / invalid weights”

This is extremely common in weighted algorithms:

* Many shortest-path functions raise `ValueError` for NaN/negative weights in contexts where they’re invalid (e.g., Dijkstra-family and “all shortest paths” docs call this out). ([rustworkx.org][14])

---

### F) Iterative non-convergence

rustworkx defines:

* `FailedToConverge`: “Failed to Converge on a solution.” ([rustworkx.org][15])

Some centrality/link-analysis functions are documented as using power iteration and note convergence is not guaranteed (e.g., PageRank, HITS, Katz). ([rustworkx.org][16])
In practice, you should treat `{max_iter reached}` as a meaningful outcome and record the parameters, regardless of whether a particular function raises `FailedToConverge` or simply returns after `max_iter`.

---

### G) Serialization / interchange boundaries

* `JSONSerializationError` / `JSONDeserializationError` exist as named exception types. ([rustworkx.org][1])
* `node_link_json(...)` requires `graph_attrs` to return a `dict[str,str]` (and similarly for node/edge attrs), otherwise “an exception will be raised.” ([rustworkx.org][1])
* GraphML is *standard-exception-driven* in the Python API surface:

  * `read_graphml(...)` raises `RuntimeError` on parse errors. ([rustworkx.org][17])
  * `write_graphml(...)` raises `RuntimeError` on write errors. ([rustworkx.org][18])

---

### H) Traversal “exceptions as control flow” (not failures)

These are intentionally used for early-exit/pruning in BFS/DFS/Dijkstra search.

* `rustworkx.visit.StopSearch`: “Stop graph traversal” ([rustworkx.org][19])
* `rustworkx.visit.PruneSearch`: “Prune part of the search tree…” ([rustworkx.org][19])

Behavioral contract:

* If your visitor raises an exception, traversal stops immediately; you can exploit this by raising `StopSearch` (swallowed) or `PruneSearch` (prunes). ([rustworkx.org][20])
* Raising `PruneSearch` inside `finish_vertex` is specifically called out as an error (BFS/DFS docs). ([rustworkx.org][20])

---

## 2) “Same concept, different surface” — common mismatches to normalize

### Invalid node handling is inconsistent across APIs

* Some APIs: `InvalidNode` (e.g., `node_connected_component`, `layers`, dominance functions). ([rustworkx.org][21])
* Others: `IndexError` (e.g., `get_node_data`). ([rustworkx.org][2])
* Others: no error, empty results (e.g., `out_edge_indices`). ([rustworkx.org][3])

**Recommendation:** in your own “graph service layer”, define a single “invalid handle” error class and map all `{InvalidNode, IndexError}` into it, *plus* optionally treat empty-return APIs as “invalid handle” when that’s safer for your pipeline.

---

## 3) Production wrapper pattern (stable error codes + rich context)

Here’s a wrapper approach that works well for “contract packs” and service endpoints: capture context and normalize exception types to stable codes.

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional

import rustworkx as rx


@dataclass(frozen=True)
class RxErrorEnvelope(Exception):
    code: str
    op: str
    message: str
    details: dict[str, Any]


def run_rx(op: str, fn: Callable[[], Any], *, details: Optional[dict[str, Any]] = None) -> Any:
    details = dict(details or {})
    try:
        return fn()

    # --- traversal control flow: treat as non-errors in *your* layer ---
    except rx.visit.StopSearch:
        raise RxErrorEnvelope("TRAVERSAL_STOP", op, "StopSearch raised", details)
    except rx.visit.PruneSearch:
        raise RxErrorEnvelope("TRAVERSAL_PRUNE", op, "PruneSearch raised", details)

    # --- structural / preconditions ---
    except rx.NullGraph as e:
        raise RxErrorEnvelope("NULL_GRAPH", op, str(e) or "NullGraph", details)
    except rx.InvalidNode as e:
        raise RxErrorEnvelope("INVALID_NODE", op, str(e) or "InvalidNode", details)
    except IndexError as e:
        # rustworkx often uses IndexError for invalid indices (incl. add_edge node missing, get_node_data invalid)
        raise RxErrorEnvelope("INVALID_INDEX", op, str(e) or "IndexError", details)

    # --- DAG / cycle ---
    except rx.DAGHasCycle as e:
        raise RxErrorEnvelope("DAG_HAS_CYCLE", op, str(e) or "DAGHasCycle", details)
    except (rx.DAGWouldCycle, ) as e:
        raise RxErrorEnvelope("DAG_WOULD_CYCLE", op, str(e) or "DAGWouldCycle", details)

    # --- edges / paths ---
    except rx.NoEdgeBetweenNodes as e:
        raise RxErrorEnvelope("NO_EDGE", op, str(e) or "NoEdgeBetweenNodes", details)
    except rx.NoSuitableNeighbors as e:
        raise RxErrorEnvelope("NO_SUITABLE_NEIGHBORS", op, str(e) or "NoSuitableNeighbors", details)
    except rx.NoPathFound as e:
        raise RxErrorEnvelope("NO_PATH", op, str(e) or "NoPathFound", details)
    except rx.NegativeCycle as e:
        raise RxErrorEnvelope("NEGATIVE_CYCLE", op, str(e) or "NegativeCycle", details)

    # --- serialization ---
    except (rx.JSONSerializationError, rx.JSONDeserializationError) as e:
        raise RxErrorEnvelope("JSON_ERROR", op, str(e) or e.__class__.__name__, details)

    # --- algorithm domain / parameters ---
    except rx.FailedToConverge as e:
        raise RxErrorEnvelope("FAILED_TO_CONVERGE", op, str(e) or "FailedToConverge", details)
    except ValueError as e:
        # weight NaN/negative, or “not found” patterns like find_negative_cycle() raising ValueError when no cycle
        raise RxErrorEnvelope("VALUE_ERROR", op, str(e) or "ValueError", details)
    except RuntimeError as e:
        # GraphML read/write uses RuntimeError for parse/write errors
        raise RxErrorEnvelope("RUNTIME_ERROR", op, str(e) or "RuntimeError", details)
    except TypeError as e:
        # note: some PyO3 type checks can be message-less (e.g., to_dot string-attr requirement)
        raise RxErrorEnvelope("TYPE_ERROR", op, str(e) or "TypeError", details)
```

Notes:

* You’ll want to attach `details` like `{graph_type, num_nodes, num_edges, source, target, edge_index}` at callsites, because some `TypeError`s can be message-less due to PyO3 type checking limits (documented on `to_dot`). ([rustworkx.org][22])
* This wrapper also makes it easy to keep CI goldens stable across behavioral shifts (e.g., `add_edge` behavior changed to raise `IndexError` when nodes don’t exist). ([rustworkx.org][23])

---

## 4) Where to be “strict” vs “soft” (design choices rustworkx already offers)

### Topological sorting: strict vs partial results

* `topological_sort()` is strict: raises `DAGHasCycle` if a cycle exists. ([rustworkx.org][7])
* `TopologicalSorter` has a `check_cycle` parameter: if `True` (default) it raises `DAGHasCycle` at initialization; if `False`, it will emit as many nodes as possible until cycles block progress. ([rustworkx.org][24])

That “soft mode” is a great pattern when you want to **diagnose** cycles (or proceed partially) instead of hard-failing.

---

## 5) Concrete “which functions raise what” examples (anchor points)

A few high-signal anchor points you can rely on:

* **DAG cycle found at runtime**: `topological_sort` → `DAGHasCycle`. ([rustworkx.org][7])
* **Cycle-preventing mutation**: `PyDiGraph(check_cycle=True)` edge add → `DAGWouldCycle` (docs), with `add_edge` docs also describing it as a `PyIndexError`. ([rustworkx.org][9])
* **No edge between endpoints**: `get_edge_data`, `remove_edge`, `update_edge`, `get_all_edge_data` → `NoEdgeBetweenNodes`. ([rustworkx.org][25])
* **Negative cycle in Bellman–Ford**: `bellman_ford_shortest_paths` (and all-pairs variants) → `NegativeCycle`. ([rustworkx.org][11])
* **Empty graph precondition**: `is_connected` → `NullGraph`; dominance → `NullGraph` / `InvalidNode`. ([rustworkx.org][5])
* **Predicate-based neighbor selection**: `find_adjacent_node_by_edge` → `NoSuitableNeighbors` when nothing matches. ([rustworkx.org][12])

---


[1]: https://www.rustworkx.org/api/exceptions.html "Exceptions - rustworkx 0.17.1"
[2]: https://www.rustworkx.org/apiref/rustworkx.PyGraph.get_node_data.html?utm_source=chatgpt.com "rustworkx.PyGraph.get_node_data"
[3]: https://www.rustworkx.org/apiref/rustworkx.PyDiGraph.out_edge_indices.html?utm_source=chatgpt.com "rustworkx.PyDiGraph.out_edge_indices"
[4]: https://www.rustworkx.org/apiref/rustworkx.NullGraph.html?utm_source=chatgpt.com "rustworkx.NullGraph"
[5]: https://www.rustworkx.org/apiref/rustworkx.is_connected.html?utm_source=chatgpt.com "rustworkx.is_connected"
[6]: https://www.rustworkx.org/apiref/rustworkx.immediate_dominators.html?utm_source=chatgpt.com "rustworkx.immediate_dominators"
[7]: https://www.rustworkx.org/apiref/rustworkx.topological_sort.html?utm_source=chatgpt.com "rustworkx.topological_sort"
[8]: https://www.rustworkx.org/apiref/rustworkx.graph_bipartite_edge_color.html?utm_source=chatgpt.com "rustworkx.graph_bipartite_edge_color"
[9]: https://www.rustworkx.org/apiref/rustworkx.PyDiGraph.html?utm_source=chatgpt.com "PyDiGraph - rustworkx 0.17.1"
[10]: https://www.rustworkx.org/apiref/rustworkx.PyDiGraph.add_edge.html?utm_source=chatgpt.com "rustworkx.PyDiGraph.add_edge"
[11]: https://www.rustworkx.org/apiref/rustworkx.bellman_ford_shortest_paths.html?utm_source=chatgpt.com "rustworkx.bellman_ford_shortest_paths"
[12]: https://www.rustworkx.org/apiref/rustworkx.PyDiGraph.find_adjacent_node_by_edge.html?utm_source=chatgpt.com "rustworkx.PyDiGraph.find_adjacent_node_by_edge"
[13]: https://www.rustworkx.org/apiref/rustworkx.find_negative_cycle.html?utm_source=chatgpt.com "rustworkx.find_negative_cycle"
[14]: https://www.rustworkx.org/apiref/rustworkx.all_shortest_paths.html "rustworkx.all_shortest_paths - rustworkx 0.17.1"
[15]: https://www.rustworkx.org/apiref/rustworkx.FailedToConverge.html?utm_source=chatgpt.com "rustworkx.FailedToConverge"
[16]: https://www.rustworkx.org/apiref/rustworkx.pagerank.html "rustworkx.pagerank - rustworkx 0.17.1"
[17]: https://www.rustworkx.org/apiref/rustworkx.read_graphml.html?utm_source=chatgpt.com "rustworkx.read_graphml"
[18]: https://www.rustworkx.org/apiref/rustworkx.write_graphml.html?utm_source=chatgpt.com "rustworkx.write_graphml"
[19]: https://www.rustworkx.org/_modules/rustworkx/visit.html "rustworkx.visit - rustworkx 0.17.1"
[20]: https://www.rustworkx.org/apiref/rustworkx.bfs_search.html?utm_source=chatgpt.com "rustworkx.bfs_search"
[21]: https://www.rustworkx.org/apiref/rustworkx.node_connected_component.html?utm_source=chatgpt.com "rustworkx.node_connected_component"
[22]: https://www.rustworkx.org/apiref/rustworkx.PyGraph.to_dot.html?utm_source=chatgpt.com "rustworkx.PyGraph.to_dot"
[23]: https://www.rustworkx.org/release_notes.html?utm_source=chatgpt.com "Release Notes - rustworkx 0.17.1"
[24]: https://www.rustworkx.org/apiref/rustworkx.TopologicalSorter.html?utm_source=chatgpt.com "TopologicalSorter - rustworkx 0.17.1"
[25]: https://www.rustworkx.org/apiref/rustworkx.PyGraph.get_edge_data.html?utm_source=chatgpt.com "rustworkx.PyGraph.get_edge_data"

# Deep dive — Custom return types (structured outputs) in rustworkx

rustworkx exposes a set of **custom container classes** (instead of plain `list`/`dict`) for many algorithm and graph-method returns. They’re designed to be **read-only**, fast, and still “Pythonic” by implementing either:

* the **sequence protocol** (indexable like a list), or
* the **mapping protocol** (dict-like, with `keys/items/values`).

The full list is documented under “Custom Return Types” (BFSSuccessors, NodeIndices, EdgeList, PathMapping, Pos2DMapping, CentralityMapping, Chains, NodeMap, etc.). ([rustworkx.org][1])

Below is the practical “what each one means, how to use it, and how to normalize it deterministically.”

---

## 0) The two big “shape families”

### A) Read-only sequences (list-like)

These support `obj[i]`, `len(obj)`, and `iter(obj)` (read-only). Examples: `NodeIndices`, `EdgeList`, `WeightedEdgeList`, `Chains`, etc. ([rustworkx.org][2])

**Storage stance:** treat as `list(obj)` and (usually) sort for determinism.

### B) Read-only mappings (dict-like)

These support `obj[key]`, `key in obj`, and have `keys()/items()/values()` methods. Examples: `PathMapping`, `EdgeIndexMap`, `Pos2DMapping`, `CentralityMapping`, etc. ([rustworkx.org][3])

**Storage stance:** treat as `dict(obj)` and always sort keys explicitly before writing snapshots/tables.

---

## 1) Sequence protocol return types (what they contain + where they come from)

### 1.1 `NodeIndices`

**Shape:** read-only sequence of integer node indices. ([rustworkx.org][2])
**Produced by:** graph methods like `graph.node_indices()` (and other bulk node-returning APIs).

**Key usage:**

```python
nodes = g.node_indices()
third = nodes[2]
for n in nodes: ...
```

(Iteration “yields results in order” per docs; don’t assume numeric ordering—sort if needed.) ([rustworkx.org][2])

---

### 1.2 `EdgeIndices`

**Shape:** read-only sequence of integer edge indices. ([rustworkx.org][4])
**Produced by:** graph methods like `edge_indices()` and bulk edge-add operations (and other edge-index APIs).

**Why it matters:** edge indices are the only *precise handle* for **multigraph parallel edges** (endpoint-based APIs can be ambiguous).

---

### 1.3 `EdgeList`

**Shape:** read-only sequence of `(u, v)` edge endpoint tuples (node indices). ([rustworkx.org][5])
**Produced by:** `graph.edge_list()` (and other “edge list” producers).

**Key detail:** endpoints are **indices**, not payloads.

---

### 1.4 `WeightedEdgeList`

**Shape:** read-only sequence of `(u, v, weight)` tuples, where `weight` is the **edge payload object**. ([rustworkx.org][6])
**Produced by:** `graph.weighted_edge_list()` (and many algorithms that return edge lists with payloads).

**Footgun:** `weight` can be *any Python object* → for deterministic storage, you need a policy (e.g., stable scalar, stable dict, or a `payload_to_str`/hash).

---

### 1.5 `BFSSuccessors` / `BFSPredecessors`

These two are easy to misinterpret.

* `BFSSuccessors`: read-only sequence of `(node_payload, [successor_payloads...])` ([rustworkx.org][7])
* `BFSPredecessors`: read-only sequence of `(node_payload, [predecessor_payloads...])` ([rustworkx.org][8])

**Important:** these tuples hold **node data payloads**, *not node indices*. ([rustworkx.org][7])

**When to use:** when you want a traversal result already “decoded” to the domain objects you stored as node payloads.

**When not to use:** if you need stable identity or joins (e.g., storing in DuckDB). In that case, use `bfs_search` with a visitor to record indices/events instead (you already have that visitor deep dive).

---

### 1.6 `Chains`

**Shape:** read-only sequence of `EdgeList` instances (i.e., “list of lists of edges”). ([rustworkx.org][9])
**Produced by:** `chain_decomposition(graph)` returns a `Chains` container. ([rustworkx.org][9])

So:

* `chains[i]` is an `EdgeList`
* each `EdgeList` element is an `(u, v)` tuple ([rustworkx.org][9])

---

### 1.7 `RelationalCoarsestPartition` and `IndexPartitionBlock`

These show up with maximum bisimulation.

* `RelationalCoarsestPartition`: read-only sequence of `NodeIndices` blocks; returned by `digraph_maximum_bisimulation`. ([rustworkx.org][10])
* `IndexPartitionBlock`: read-only sequence of integers representing one block of node indices (also from `digraph_maximum_bisimulation`). ([rustworkx.org][11])

**Practical interpretation:** bisimulation gives you a partition of the node set into “equivalence blocks” (store as `(block_id, node)` rows).

---

## 2) Mapping protocol return types (what they contain + where they come from)

### 2.1 `PathMapping` (single-source paths)

**Shape:** read-only mapping `target_node_index -> [path_node_indices...]`. ([rustworkx.org][3])
**Produced by:** shortest-path routines like Dijkstra shortest paths.

Docs explicitly say it implements the mapping protocol and can be iterated (via `iter(obj)`) to yield results “in order.” ([rustworkx.org][3])

---

### 2.2 `PathLengthMapping` (single-source distances)

**Shape:** read-only mapping `target_node_index -> float distance`. ([rustworkx.org][12])
Same iteration/mapping semantics as `PathMapping`. ([rustworkx.org][12])

---

### 2.3 `AllPairsPathMapping` / `AllPairsPathLengthMapping` (nested mappings)

* `AllPairsPathMapping`: `source_node_index -> PathMapping` ([rustworkx.org][13])
* `AllPairsPathLengthMapping`: `source_node_index -> PathLengthMapping` ([rustworkx.org][14])

These are “mapping-of-mappings”. For storage, you almost always flatten them to rows `(source, target, path)` or `(source, target, dist)`.

---

### 2.4 `EdgeIndexMap`

**Shape:** read-only mapping `edge_index -> (u, v, edge_payload)`. ([rustworkx.org][15])
**Produced by:** graph methods like `edge_index_map()` / incident-edge maps, etc.

Docs call it a drop-in replacement for a **read-only dict**. ([rustworkx.org][15])

**Why it matters:** it’s the bridge from *edge index* → *(endpoints + payload)*, which is critical for multigraph correctness.

---

### 2.5 `Pos2DMapping`

**Shape:** read-only mapping `node_index -> [x, y]`. ([rustworkx.org][16])
**Produced by:** layout functions (spring/circular/bipartite/etc.) and accepted by `mpl_draw`.

Docs call it a drop-in replacement for a **read-only dict**. ([rustworkx.org][16])

---

### 2.6 `CentralityMapping` / `EdgeCentralityMapping`

* `CentralityMapping`: `node_index -> float score` ([rustworkx.org][17])
* `EdgeCentralityMapping`: `edge_index -> float score` ([rustworkx.org][18])

Used by centrality routines (betweenness, edge betweenness, etc.). These are the most straightforward to normalize (just rows `(node, score)` / `(edge_index, score)`).

---

### 2.7 `NodeMap` / `ProductNodeMap`

These are “mapping outputs” from graph construction operations (subgraph mappings, products).

* `NodeMap`: mapping `node_index -> node_index`, **unordered**; docs explicitly warn that iteration order may differ between NodeMap objects with the same contents, and recommend sorting when you need consistent order. ([rustworkx.org][19])
* `ProductNodeMap`: mapping `(node_index_a, node_index_b) -> node_index` (e.g., cartesian product mapping). ([rustworkx.org][20])

---

### 2.8 `BiconnectedComponents`

**Shape:** mapping `(u, v) -> component_id_int` where `(u, v)` is the edge endpoints. ([rustworkx.org][21])
This is how rustworkx represents the component assignment compactly.

**Storage:** flatten to rows `(u, v, component_id)` sorted by `(component_id, u, v)` or similar.

---

## 3) Deterministic normalization (the “contract layer” you want in practice)

If you’re snapshotting outputs (CI gating, reproducible pipelines), don’t trust iteration order unless the docs promise it—and even then, **sort** for stability.

### 3.1 Recommended canonicalization rules

* **Sequence types** (`NodeIndices`, `EdgeList`, `WeightedEdgeList`, `Chains`, …):

  * `rows = list(obj)`
  * if you need determinism: `rows = sorted(rows, key=...)` (choose a stable key)
  * for nested sequences (`Chains`): flatten with `(chain_id, u, v)` rows ([rustworkx.org][9])
* **Mapping types** (`PathMapping`, `EdgeIndexMap`, `CentralityMapping`, …):

  * never rely on iteration order → always iterate `for k in sorted(mapping.keys()): ...`
  * `NodeMap` explicitly warns order can vary; sorting is required for determinism ([rustworkx.org][19])
* **Nested mappings** (`AllPairsPathMapping`, `AllPairsPathLengthMapping`):

  * `for src in sorted(all_pairs.keys()): inner = all_pairs[src]; for dst in sorted(inner.keys()): ...` ([rustworkx.org][13])
* **Payload-bearing outputs** (`WeightedEdgeList`, `EdgeIndexMap`, BFSSuccessors payloads):

  * define a policy: `payload_to_str`, `payload_hash`, or “structured payload must be JSON-serializable”
  * otherwise your “deterministic” story will break the moment payloads are dicts with non-deterministic ordering or contain non-serializable objects.

### 3.2 Minimal “universal normalizer” idiom

```python
def as_py(obj):
    # mapping-like
    if hasattr(obj, "keys") and hasattr(obj, "__getitem__"):
        return dict(obj)
    # sequence-like
    return list(obj)
```

Then you still apply explicit sorting + flattening based on the expected shape.

---

## 4) Quick “which ones contain indices vs payloads” cheat sheet

* **Indices everywhere (best for joins):**

  * `NodeIndices`, `EdgeIndices`, `EdgeList`, `WeightedEdgeList` (endpoints are indices) ([rustworkx.org][2])
  * `PathMapping`, `PathLengthMapping`, `AllPairs*`, `CentralityMapping`, `EdgeCentralityMapping`, `Pos2DMapping`, `EdgeIndexMap`, `NodeMap`, `ProductNodeMap`, `BiconnectedComponents` ([rustworkx.org][3])
* **Payload-focused (be careful for storage):**

  * `BFSSuccessors` / `BFSPredecessors` return **node payloads**, not indices ([rustworkx.org][7])

---

[1]: https://www.rustworkx.org/api/custom_return_types.html "Custom Return Types - rustworkx 0.17.1"
[2]: https://www.rustworkx.org/apiref/rustworkx.NodeIndices.html "NodeIndices - rustworkx 0.17.1"
[3]: https://www.rustworkx.org/apiref/rustworkx.PathMapping.html "PathMapping - rustworkx 0.17.1"
[4]: https://www.rustworkx.org/apiref/rustworkx.EdgeIndices.html "EdgeIndices - rustworkx 0.17.1"
[5]: https://www.rustworkx.org/apiref/rustworkx.EdgeList.html "EdgeList - rustworkx 0.17.1"
[6]: https://www.rustworkx.org/apiref/rustworkx.WeightedEdgeList.html "WeightedEdgeList - rustworkx 0.17.1"
[7]: https://www.rustworkx.org/apiref/rustworkx.BFSSuccessors.html "BFSSuccessors - rustworkx 0.17.1"
[8]: https://www.rustworkx.org/apiref/rustworkx.BFSPredecessors.html "BFSPredecessors - rustworkx 0.17.1"
[9]: https://www.rustworkx.org/apiref/rustworkx.Chains.html "Chains - rustworkx 0.17.1"
[10]: https://www.rustworkx.org/apiref/rustworkx.RelationalCoarsestPartition.html "RelationalCoarsestPartition - rustworkx 0.17.1"
[11]: https://www.rustworkx.org/apiref/rustworkx.IndexPartitionBlock.html "IndexPartitionBlock - rustworkx 0.17.1"
[12]: https://www.rustworkx.org/apiref/rustworkx.PathLengthMapping.html "PathLengthMapping - rustworkx 0.17.1"
[13]: https://www.rustworkx.org/apiref/rustworkx.AllPairsPathMapping.html "AllPairsPathMapping - rustworkx 0.17.1"
[14]: https://www.rustworkx.org/apiref/rustworkx.AllPairsPathLengthMapping.html "AllPairsPathLengthMapping - rustworkx 0.17.1"
[15]: https://www.rustworkx.org/apiref/rustworkx.EdgeIndexMap.html "EdgeIndexMap - rustworkx 0.17.1"
[16]: https://www.rustworkx.org/apiref/rustworkx.Pos2DMapping.html "Pos2DMapping - rustworkx 0.17.1"
[17]: https://www.rustworkx.org/apiref/rustworkx.CentralityMapping.html "CentralityMapping - rustworkx 0.17.1"
[18]: https://www.rustworkx.org/apiref/rustworkx.EdgeCentralityMapping.html "EdgeCentralityMapping - rustworkx 0.17.1"
[19]: https://www.rustworkx.org/apiref/rustworkx.NodeMap.html "NodeMap - rustworkx 0.17.1"
[20]: https://www.rustworkx.org/apiref/rustworkx.ProductNodeMap.html "ProductNodeMap - rustworkx 0.17.1"
[21]: https://www.rustworkx.org/apiref/rustworkx.BiconnectedComponents.html "BiconnectedComponents - rustworkx 0.17.1"
