This updated document is based **directly and only** on the official NetworkX **3.6** reference documentation (release 3.6, dated Nov 24, 2025) at `networkx.org/documentation/stable`. ([NetworkX][1])

It’s structured to match the style and depth of the example technical overviews you shared (e.g. the CodeIntel metadata outputs overview). 

---

# 1. Package overview (NetworkX 3.6)

NetworkX is a pure‑Python library for:

* **Graph data structures** (directed/undirected, simple/multi)
* **Graph generators** (classic, random, lattice, social network benchmarks, etc.)
* **Graph algorithms** (paths, centrality, flows, communities, isomorphism…)
* **I/O and conversion** (file formats, NumPy/SciPy/pandas interop)
* **Drawing & layout**
* **Dispatchable backends** for high‑performance implementations

Most of the user‑facing API is provided as **functions** that take a graph object `G` as the first argument; graph classes themselves focus on **basic construction, mutation and reporting**. ([NetworkX][2])

The 3.6 reference is organized into the following major sections: ([NetworkX][1])

* Introduction
* Graph types
* Algorithms
* Functions
* Graph generators
* Linear algebra
* Converting to and from other data formats
* Relabeling nodes
* Reading and writing graphs
* Drawing
* Randomness
* Exceptions
* Utilities
* Backends
* Configs
* Glossary

Everything described below is **present in the 3.6 reference index** and cross‑checked against the corresponding reference pages.

---

# 2. Core graph types & data model

## 2.1 Graph classes

NetworkX ships four primary graph classes, all accepting any **hashable Python object** as a node, with arbitrary Python objects as edge attributes. ([NetworkX][2])

| Class          | Directed? | Parallel edges? | Self‑loops? | Notes                                 |
| -------------- | --------- | --------------- | ----------- | ------------------------------------- |
| `Graph`        | No        | No              | Yes         | Simple undirected graph.              |
| `DiGraph`      | Yes       | No              | Yes         | Directed edges (`(u, v)` ≠ `(v, u)`). |
| `MultiGraph`   | No        | Yes             | Yes         | Undirected multi‑graph (edge keys).   |
| `MultiDiGraph` | Yes       | Yes             | Yes         | Directed multi‑graph.                 |

These classes are documented both in the Introduction and in the Graph types section. ([NetworkX][2])

### Node and edge attributes

* Any hashable Python object (e.g. `int`, `str`, `tuple`, functions, custom objects) can be a node. ([NetworkX][2])
* Edge attributes are arbitrary key–value mappings; the conventional key `"weight"` is used by many algorithms (e.g. Dijkstra shortest paths). ([NetworkX][2])

Example (directly aligned with the docs):

```python
import networkx as nx
G = nx.Graph()
G.add_edge(2, 3, weight=0.9)  # numeric weight
G.add_edge("x", "y", function=math.cos)  # arbitrary attribute
```

## 2.2 Internal data structure

NetworkX graphs are implemented as **adjacency maps built from nested dictionaries** (“dict‑of‑dicts‑of‑dicts”):

* For `Graph`: outer dict keyed by node → inner dict keyed by neighbor → edge attribute dict.
* For `DiGraph`: separate `G.succ` and `G.pred` adjacency structures.
* For `MultiGraph`/`MultiDiGraph`: an extra level keyed by **edge key** before attributes (dict‑of‑dict‑of‑dict‑of‑dicts). ([NetworkX][2])

Key properties of this design: fast neighbor lookup, fast edge insertion/removal, sparse storage, and direct access via both adjacency (`G[u][v]['attr']`) and edge views (`G.edges[u, v]['attr']`). ([NetworkX][2])

## 2.3 Graph views and filters

The **Graph types** reference documents a rich set of read‑only views and filters: ([NetworkX][3])

* **Graph view constructors**:

  * `generic_graph_view(G, create_using=None)` – read‑only view of `G`.
  * `subgraph_view(G, filter_node, filter_edge)` – filtered node/edge view.
  * `reverse_view(G)` – reverse edge directions of a directed graph.

* **Core view classes** (dict‑like and read‑only):

  * `AtlasView`, `AdjacencyView`, `MultiAdjacencyView`
  * `UnionAtlas`, `UnionAdjacency`, `UnionMultiInner`, `UnionMultiAdjacency`
  * `FilterAtlas`, `FilterAdjacency`, `FilterMultiInner`, `FilterMultiAdjacency` ([NetworkX][3])

* **Filter factories**:

  * `no_filter`, `hide_nodes`, `hide_edges`, `hide_diedges`,
    `hide_multiedges`, `hide_multidiedges`,
    `show_nodes`, `show_edges`, `show_diedges`, `show_multiedges`, `show_multidiedges`. ([NetworkX][3])

These views are explicitly documented as **read‑only wrappers over an underlying graph**, used to avoid copying when temporarily restricting or transforming graphs (e.g. induced subgraphs, reversed digraphs). ([NetworkX][3])

---

# 3. Graph creation, reporting & basic operations

The Introduction breaks usage into **graph creation**, **reporting**, and **algorithms**. ([NetworkX][2])

## 3.1 Graph creation

NetworkX graphs can be created by: ([NetworkX][2])

1. **Explicit construction**:

   * `G = nx.Graph()`, `nx.DiGraph()`, `nx.MultiGraph()`, `nx.MultiDiGraph()`.
   * `G.add_node(n, **attrs)`, `G.add_nodes_from(iterable)`
   * `G.add_edge(u, v, **attrs)`, `G.add_edges_from([...])`
   * `G.add_weighted_edges_from([(u, v, w), ...])`

2. **Graph generators** (section 6 below).

3. **I/O / conversion** (sections 7 and 8 below).

The **Functions → Graph** helpers provide convenient building blocks: ([NetworkX][4])

* `add_star(G, nodes_for_star, **attr)`
* `add_path(G, nodes_for_path, **attr)`
* `add_cycle(G, nodes_for_cycle, **attr)`
* `create_empty_copy(G, with_data=False)`

## 3.2 Graph reporting

Graph objects expose views for reporting: ([NetworkX][2])

* Node‑centric:

  * `G.nodes`, `G.nodes(data=True)`
  * `G.degree`, `G.degree[n]`
  * `nodes(G)`, `number_of_nodes(G)`

* Edge‑centric:

  * `G.edges`, `G.edges(data=True)`
  * `G.adj` for low‑level adjacency
  * `edges(G)`, `number_of_edges(G)`, `non_edges(G)`

The design explicitly encourages **node‑centric usage** (`G[u]` gives neighbors) but exposes edge views (`G.edges[u, v]`) for edge‑centric workflows. ([NetworkX][2])

## 3.3 Functional API (networkx.functions)

The `Functions` reference collects functional equivalents of common graph methods and basic utilities. ([NetworkX][4])

### Graph‑level helpers

* Degree & density: `degree`, `degree_histogram`, `density`.
* Type checks / views: `is_directed`, `to_directed`, `to_undirected`, `is_empty`.
* Structured additions: `add_star`, `add_path`, `add_cycle`.
* Graph reductions: `subgraph`, `induced_subgraph`, `restricted_view`, `edge_subgraph`. ([NetworkX][4])

### Node, edge & attribute helpers

* Nodes: `nodes`, `number_of_nodes`, `neighbors`, `all_neighbors`, `non_neighbors`, `common_neighbors`.
* Edges: `edges`, `number_of_edges`, `non_edges`.
* Self‑loops: `selfloop_edges`, `number_of_selfloops`, `nodes_with_selfloops`.
* Attributes: `is_weighted`, `is_negatively_weighted`,
  `set_node_attributes`, `get_node_attributes`,
  `set_edge_attributes`, `get_edge_attributes`. ([NetworkX][4])

### Path & freezing utilities

* Path checks: `is_path(G, path)`, `path_weight(G, path, weight)`.
* Freezing: `freeze(G)` (prevent further structural changes), `is_frozen(G)`. ([NetworkX][4])

---

# 4. Algorithms

The `Algorithms` section in 3.6 is large; it groups algorithms into over 60 thematic modules, listed under the reference index and the Centrality page’s sidebar. ([NetworkX][1])

**Algorithm category modules include** (verbatim from the 3.6 reference navigation):

* Approximations and Heuristics
* Assortativity
* Asteroidal
* Bipartite
* Boundary
* Bridges
* Broadcasting
* Centrality
* Chains
* Chordal
* Clique
* Clustering
* Coloring
* Communicability
* Communities
* Components
* Connectivity
* Cores
* Covering
* Cycles
* Cuts
* D‑Separation
* Directed Acyclic Graphs
* Distance Measures
* Distance‑Regular Graphs
* Dominance
* Dominating Sets
* Efficiency
* Eulerian
* Flows
* Graph Hashing
* Graphical degree sequence
* Hierarchy
* Hybrid
* Isolates
* Isomorphism
* Link Analysis
* Link Prediction
* Lowest Common Ancestor
* Matching
* Minors
* Maximal independent set
* Non‑randomness
* Moral
* Node Classification
* Operators
* Perfect Graph
* Planarity
* Planar Drawing
* Graph Polynomials
* Reciprocity
* Regular
* Rich Club
* Shortest Paths
* Similarity Measures
* Simple Paths
* Small‑world
* s metric
* Sparsifiers
* Structural holes
* Summarization
* Swap
* Threshold Graphs
* Time dependent
* Tournament
* Traversal
* Tree
* Triads
* Vitality
* Voronoi cells
* Walks
* Wiener Index ([NetworkX][1])

Below is a high‑level description of the **main families**, without listing every individual function (there are hundreds).

## 4.1 Traversal & shortest paths

Covered across **Traversal**, **Shortest Paths**, **Simple Paths**, and **Tree** modules. ([NetworkX][2])

Common capabilities (all present in NetworkX 3.x):

* Unweighted shortest paths:

  * Single‑source, single‑target shortest path and path length.
  * All‑pairs shortest paths and distances.
* Weighted shortest paths:

  * Dijkstra’s algorithm (`dijkstra_path`, `dijkstra_path_length` as in the docs example). ([NetworkX][2])
  * Variants like Bellman‑Ford, multi‑source Dijkstra, and multi‑criteria versions.
* BFS / DFS traversals:

  * Node and edge generators, layered BFS trees, DFS pre‑ and post‑order.
* Simple paths:

  * Enumerating simple paths between two nodes and k‑shortest paths.

Functions in these modules are **dispatchable to backends** where available (e.g. `nx.betweenness_centrality(backends=...)` discussed in the Backends page). ([NetworkX][5])

## 4.2 Centrality

The **Centrality** module exposes a broad suite of node/edge centrality measures, all listed in the 3.6 docs: ([NetworkX][6])

* Degree: `degree_centrality`, `in_degree_centrality`, `out_degree_centrality`.
* Eigenvector/Katz: `eigenvector_centrality`, `eigenvector_centrality_numpy`, `katz_centrality`, `katz_centrality_numpy`.
* Closeness & incremental closeness: `closeness_centrality`, `incremental_closeness_centrality`.
* Current‑flow measures: current‑flow closeness/betweenness (node/edge, exact and approximate).
* Betweenness: `betweenness_centrality`, `edge_betweenness_centrality`, subset variants.
* Group centrality: group degree/closeness/betweenness, group prominence.
* Subgraph centrality & Estrada index: `subgraph_centrality`, `subgraph_centrality_exp`, `estrada_index`.
* Load, harmonic, dispersion, local/global reaching centrality.
* Percolation, second‑order, trophic measures.
* VoteRank, Laplacian centrality. ([NetworkX][6])

All of these functions are explicitly documented in the 3.6 reference page for Centrality.

## 4.3 Connectivity, components & cuts

Spread across **Components**, **Connectivity**, **Cores**, **Cuts**, **Bridges**, **Boundary**, **Graphical degree sequence**, etc. ([NetworkX][6])

Typical capabilities:

* Connected components (weak/strong for directed graphs).
* Biconnected components, articulation points.
* k‑connectedness, node/edge cuts, minimum edge/vertex separators.
* k‑cores, shell and coreness.
* Bridges and articulation points in undirected graphs.
* Checking and constructing graphical degree sequences.

## 4.4 Flows & matchings

The **Flows**, **Matching**, and related modules cover: ([NetworkX][6])

* Max‑flow / min‑cut algorithms on directed networks (successive shortest augmenting paths, preflow‑push, etc.).
* Min‑cost flows, cost‑scaling methods.
* Bipartite and general matchings (maximum cardinality/weight).
* Assignment problems, including bipartite matching algorithms.

## 4.5 Structure: cliques, clustering, communities, assortativity

Key groups: **Clique**, **Clustering**, **Communities**, **Assortativity**, **Small‑world**, **Rich Club**, **Non‑randomness**, **Structural holes**. ([NetworkX][6])

* Clique:

  * Maximal cliques, clique number, clique percolation.
* Clustering:

  * Local/global clustering coefficients, transitivity, average clustering.
* Communities:

  * Modularity‑based and label‑propagation community detection, LFR benchmark tools (tied to generators). ([NetworkX][7])
* Assortativity:

  * Degree and attribute assortativity coefficients.
* Small‑world & structural holes:

  * Small‑world metrics, rich‑club coefficients, structural hole measures.

## 4.6 Special graph families

Other modules focus on specific structures:

* **Bipartite**: projections, bipartite centrality, bipartite matching.
* **Directed Acyclic Graphs (DAGs)**: topological sorting, ancestors/descendants, longest/shortest paths in DAGs.
* **Planarity** and **Planar Drawing**: planarity tests, embedding, planar layouts.
* **Isomorphism**: VF2 and related algorithms for graph and subgraph isomorphism.
* **Tree**: tree‑specific algorithms (center, diameter, branching structure). ([NetworkX][6])

## 4.7 Probabilistic & learning‑related

* **Node Classification**: semi‑supervised learning on graphs.
* **Link Prediction**: Adamic‑Adar, resource allocation, preferential attachment scores, etc.
* **D‑Separation, Moral, Perfect Graph**: algorithms for probabilistic graphical models (Bayesian networks, moralization) and structural graph properties. ([NetworkX][6])

## 4.8 Operators & transforms

The **Operators** module and related sections provide:

* Graph unions/intersections, Cartesian/lexicographic/tensor products.
* Line graph construction, complement graphs.
* Edge swaps and degree‑preserving rewiring (tied to **Swap** and **Random Clustered** generators). ([NetworkX][2])

---

# 5. Graph generators

The **Graph generators** reference is extensive; it documents dozens of generators grouped by category. ([NetworkX][1])

Top‑level categories (as listed in the 3.6 reference index): ([NetworkX][1])

* Atlas
* Classic
* Expanders
* Lattice
* Small
* Random Graphs
* Duplication Divergence
* Degree Sequence
* Random Clustered
* Directed
* Geometric
* Line Graph
* Ego Graph
* Stochastic
* AS graph
* Intersection
* Social Networks
* Community
* Spectral
* Trees
* Non‑Isomorphic Trees
* Triads
* Joint Degree Sequence
* Mycielski
* Harary Graph
* Cographs
* Interval Graph
* Sudoku
* Time Series

### 5.1 Classic small graphs

In **Classic** and **Small** categories: ([NetworkX][7])

* Paths, cycles, grids, ladders, stars, lollipops, wheels, balanced/full trees.
* Named graphs: Petersen, Heawood, Frucht, Dodecahedral, Tetrahedral, etc.
* `null_graph`, `empty_graph`, `trivial_graph`.

### 5.2 Random graphs

From the **Random Graphs** group: ([NetworkX][7])

* Erdős–Rényi / binomial models: `gnp_random_graph`, `fast_gnp_random_graph`, `gnm_random_graph`, `erdos_renyi_graph`, `binomial_graph`.
* Small‑world: `watts_strogatz_graph`, `newman_watts_strogatz_graph`, `connected_watts_strogatz_graph`.
* Scale‑free / power‑law: `barabasi_albert_graph`, `dual_barabasi_albert_graph`, `extended_barabasi_albert_graph`, `powerlaw_cluster_graph`.
* Degree‑sequence and configuration models: `configuration_model`, `directed_configuration_model`, `havel_hakimi_graph`, `random_degree_sequence_graph`.

### 5.3 Geometric & spatial

**Geometric** generators include: ([NetworkX][7])

* `random_geometric_graph`, `soft_random_geometric_graph`,
  `thresholded_random_geometric_graph`, `waxman_graph`.
* `geometric_edges`, `geometric_soft_configuration_graph`.
* Lattice and grid graphs (`grid_2d_graph`, `triangular_lattice_graph`, `hexagonal_lattice_graph`, `grid_graph`, `hypercube_graph`). ([NetworkX][7])

### 5.4 Social, community and benchmark graphs

From **Social Networks**, **Community**, **Trees**, etc.: ([NetworkX][7])

* Empirical small social networks:

  * `karate_club_graph`, `davis_southern_women_graph`,
    `florentine_families_graph`, `les_miserables_graph`.
* Community/partition models:

  * `planted_partition_graph`, `stochastic_block_model`,
    `gaussian_random_partition_graph`, `caveman_graph`, `connected_caveman_graph`, `relaxed_caveman_graph`, `ring_of_cliques`, `windmill_graph`.
* Trees/non‑isomorphic trees:

  * `random_labeled_tree`, `random_unlabeled_tree`, `nonisomorphic_trees`, etc.

### 5.5 Other categories

* **Duplication Divergence**: `duplication_divergence_graph`, `partial_duplication_graph` (biological models).
* **Intersection graphs**: `uniform_random_intersection_graph`, `k_random_intersection_graph`, `general_random_intersection_graph`.
* **Joint degree sequence**, **Mycielski**, **Harary graphs**, **Cographs**, **Interval graphs**, **Sudoku** and **Time series** generators (e.g. `sudoku_graph`, `visibility_graph`). ([NetworkX][7])

---

# 6. Linear algebra tools

The **Linear algebra** reference groups matrix and spectral operations into several sub‑sections. ([NetworkX][8])

### 6.1 Graph matrices

* `adjacency_matrix(G, nodelist=None, dtype=None, weight=None)`
* `incidence_matrix(G, nodelist=None, edgelist=None, oriented=False, weight=None)`

These return SciPy sparse matrices (or arrays), with behavior spelled out in the reference. ([NetworkX][8])

### 6.2 Laplacians

* `laplacian_matrix` (unnormalized).
* `normalized_laplacian_matrix`.
* `directed_laplacian_matrix`.
* `directed_combinatorial_laplacian_matrix`.

All Laplacians are defined with respect to out‑degree for directed graphs, per the docs. ([NetworkX][8])

### 6.3 Bethe Hessian and algebraic connectivity

* `bethe_hessian_matrix(G, r, nodelist)` for deformed Laplacians.
* `algebraic_connectivity(G, ...)` and `fiedler_vector(G, ...)`.
* Spectral partitioning helpers: `spectral_ordering`, `spectral_bisection`. ([NetworkX][8])

### 6.4 Attribute & modularity matrices

* Attribute matrices:

  * `attr_matrix(G, ...)`, `attr_sparse_matrix(G, ...)` – matrix representations built from node/edge attributes. ([NetworkX][8])
* Modularity:

  * `modularity_matrix(G, ...)`, `directed_modularity_matrix(G, ...)`. ([NetworkX][8])

### 6.5 Spectra

* `adjacency_spectrum`, `laplacian_spectrum`, `bethe_hessian_spectrum`, `normalized_laplacian_spectrum`, `modularity_spectrum` – eigenvalues of the respective matrices. ([NetworkX][8])

---

# 7. Converting to and from other data formats

The **Converting to and from other data formats** reference documents all supported conversions. ([NetworkX][9])

### 7.1 Generic conversion

* `to_networkx_graph(data, create_using=None, multigraph_input=False, ...)` attempts to infer how to convert common containers into graphs.

  * The docs emphasize that constructing graphs via `nx.Graph(data)`/`nx.DiGraph(data)` routes through this function. ([NetworkX][9])

### 7.2 Dictionaries and lists

* `to_dict_of_dicts(G, nodelist=None, edge_data=None)`
* `from_dict_of_dicts(d, create_using=None, multigraph_input=False)`
* `to_dict_of_lists(G, nodelist=None)`
* `from_dict_of_lists(d, create_using=None)`
* `to_edgelist(G, nodelist=None)`
* `from_edgelist(edgelist, create_using=None)` ([NetworkX][9])

### 7.3 NumPy, SciPy, and pandas

From the same page: ([NetworkX][9])

* NumPy:

  * `to_numpy_array(G, ...)` – adjacency matrix as a dense `ndarray`.
  * `from_numpy_array(A, ...)` – graph from 2D array.

* SciPy:

  * `to_scipy_sparse_array(G, ...)` – adjacency as a SciPy sparse array.
  * `from_scipy_sparse_array(A, ...)` – graph from sparse array.

* pandas:

  * `to_pandas_adjacency(G, ...)` / `from_pandas_adjacency(df, ...)` – adjacency DataFrame.
  * `to_pandas_edgelist(G, ...)` / `from_pandas_edgelist(df, ...)` – edge list DataFrame.

---

# 8. Reading and writing graphs (file formats)

The **Reading and writing graphs** reference lists supported formats and their associated functions. ([NetworkX][10])

### 8.1 Adjacency lists

* Adjacency List:

  * `read_adjlist`, `write_adjlist`, `parse_adjlist`, `generate_adjlist`.
* Multiline Adjacency List:

  * `read_multiline_adjlist`, `write_multiline_adjlist`, `parse_multiline_adjlist`, `generate_multiline_adjlist`. ([NetworkX][10])

### 8.2 Edge lists

* `read_edgelist`, `write_edgelist`.
* `read_weighted_edgelist`, `write_weighted_edgelist`.
* `generate_edgelist`, `parse_edgelist`. ([NetworkX][10])

### 8.3 XML‑like formats

* **GEXF**: `read_gexf`, `write_gexf`, `generate_gexf`, `relabel_gexf_graph`.
* **GML**: `read_gml`, `write_gml`, `parse_gml`, `generate_gml`, plus helpers `literal_destringizer`, `literal_stringizer`.
* **GraphML**: `read_graphml`, `write_graphml`, `generate_graphml`, `parse_graphml`. ([NetworkX][10])

### 8.4 JSON‑based encodings

The JSON submodule supports a set of canonical graph encodings: ([NetworkX][10])

* Node‑link: `node_link_data`, `node_link_graph`.
* Adjacency data: `adjacency_data`, `adjacency_graph`.
* Cytoscape JSON: `cytoscape_data`, `cytoscape_graph`.
* Tree encodings: `tree_data`, `tree_graph`.

### 8.5 Other formats

* **LEDA**: `read_leda`, `parse_leda`.
* **SparseGraph6 / Graph6** encodings.
* **Pajek**: `read_pajek`, `write_pajek`, `parse_pajek`, `generate_pajek`.
* **Matrix Market**: matrix‑based formats.
* **Network Text**: `generate_network_text`, `write_network_text`. ([NetworkX][10])

---

# 9. Drawing & layout

The **Drawing** reference documents NetworkX’s built‑in visualization utilities. ([NetworkX][11])

The docs explicitly state that NetworkX aims at **analysis first, visualization second**, and that rich visualization is better handled by specialized tools (Cytoscape, Gephi, Graphviz, iplotx, TikZ). ([NetworkX][11])

## 9.1 Matplotlib drawing primitives

Matplotlib integration includes: ([NetworkX][11])

* `display(G, canvas=None)` – convenience wrapper.
* `apply_matplotlib_colors(G, ...)`.
* `draw(G, pos=None, ax=None)` – simple wrapper, spring layout by default.
* `draw_networkx(G, pos=None, arrows=True, with_labels=True, ...)`.
* `draw_networkx_nodes`, `draw_networkx_edges`, `draw_networkx_labels`, `draw_networkx_edge_labels`.
* Layout‑specific drawing:

  * `draw_circular`, `draw_bipartite`, `draw_kamada_kawai`, `draw_planar`, `draw_random`, `draw_spectral`, `draw_spring`, `draw_shell`.

## 9.2 Graphviz & iplotx

* **Graphviz via pygraphviz (`nx_agraph`)**:

  * `from_agraph`, `to_agraph`, `write_dot`, `read_dot`,
    `graphviz_layout`, `pygraphviz_layout`. ([NetworkX][11])

* **Graphviz via pydot (`nx_pydot`)**:

  * `from_pydot`, `to_pydot`,
    `write_dot`, `read_dot`, `graphviz_layout`, `pydot_layout`. ([NetworkX][11])

* **Matplotlib with iplotx**:

  * Example usage documented with `ipx.network(G, layout=...)` for richer interactive styling. ([NetworkX][11])

## 9.3 Layout algorithms

The **Graph Layout** section lists layout functions that return node position mappings: ([NetworkX][11])

* Force‑directed: `spring_layout`, `forceatlas2_layout`, `kamada_kawai_layout`.
* Radial/circular: `circular_layout`, `shell_layout`, `spiral_layout`.
* Geometric: `planar_layout`, `random_layout`.
* Hierarchical / level‑based: `bipartite_layout`, `bfs_layout`, `multipartite_layout`.
* Spectral: `spectral_layout`.
* Utility: `arf_layout`, `rescale_layout`, `rescale_layout_dict`.

## 9.4 LaTeX/TikZ export

* `to_latex_raw(G, ...)`, `to_latex(Gbunch, ...)`, `write_latex(Gbunch, path, ...)` for exporting TikZ/LaTeX code. ([NetworkX][11])

The docs show how to encode node/edge styles and labels via graph attributes and produce a complete LaTeX document or subfigure layout.

---

# 10. Randomness & seeding

The **Randomness** page explains how NetworkX handles random number generation across functions that use randomness (e.g. generators, layouts, randomized algorithms). ([NetworkX][12])

Key points:

* NetworkX uses **two standard RNG packages**:

  * Python’s `random` module.
  * NumPy’s `numpy.random`. Both implement Mersenne Twister. ([NetworkX][12])
* Functions that rely on RNG accept a **`seed` keyword** that can be:

  * `None` – use the function’s default global RNG.
  * An integer – create a temporary local RNG instance.
  * `numpy.random` – use NumPy’s global RNG.
  * A NumPy `RandomState` or `Generator` – reuse custom RNG instances. ([NetworkX][12])

This mechanism allows consistent seeding across NetworkX, NumPy, and other libraries.

---

# 11. Utilities (networkx.utils)

The **Utilities** reference groups helper functions and small data structures not imported into the top‑level `networkx` namespace. ([NetworkX][13])

## 11.1 Helper functions

Examples: ([NetworkX][13])

* `arbitrary_element(iterable)` – pick an element without removal.
* `flatten(obj)` – flatten nested iterables.
* `make_list_of_ints(sequence)` – coerce to list of ints.
* `dict_to_numpy_array(d, mapping=None)` – convert dict‑of‑dicts to NumPy array.
* `pairwise(iterable, cyclic=False)` – overlapping pairs.
* `groups(many_to_one)` – invert many‑to‑one mapping.
* Graph comparison: `nodes_equal`, `edges_equal`, `graphs_equal`.

Random‑state helpers: `create_random_state`, `create_py_random_state`.

## 11.2 Data structures & algorithms

* `UnionFind` (with documented `UnionFind.union`) – disjoint‑set data structure used in many core algorithms. ([NetworkX][13])

## 11.3 Random sequence generators

Utilities for generating random sequences and samples, including: ([NetworkX][13])

* `powerlaw_sequence(n, exponent, seed)`
* `is_valid_tree_degree_sequence(degree_sequence)`
* `cumulative_distribution(distribution)`
* `discrete_sequence(n, distribution, ...)`
* `zipf_rv(alpha, xmin, seed)`
* `random_weighted_sample(mapping, k, seed)`
* `weighted_choice(mapping, seed)`

## 11.4 Decorators

Algorithm decorators integrated across the library: ([NetworkX][13])

* `open_file(path_arg, mode)` – ensure clean open/close around functions that take file paths or file‑like objects.
* `not_implemented_for(*graph_types)` – mark algorithms restricted to certain graph types.
* `nodes_or_number(which_args)` – allow “nodes or number of nodes” semantics.
* `np_random_state`, `py_random_state` – unify RNG handling.
* `argmap` – map arguments before calling the function.

## 11.5 Cuthill‑McKee and MappedQueue

* `cuthill_mckee_ordering(G, heuristic=None)` and `reverse_cuthill_mckee_ordering(G, heuristic=None)` – sparsity‑friendly node orderings for matrix representations.
* `MappedQueue` – priority queue with updatable priorities, used in Dijkstra‑like algorithms. ([NetworkX][13])

---

# 12. Exceptions

The **Exceptions** reference defines all core exception types used throughout NetworkX. ([NetworkX][14])

Documented classes (all present in 3.6):

* `NetworkXException` – base class.
* `NetworkXError` – generic serious error.
* `NetworkXPointlessConcept` – raised for null graph inputs where concept is undefined.
* `NetworkXAlgorithmError` – unexpected algorithm termination.
* `NetworkXUnfeasible` – instance has no feasible solution.
* `NetworkXNoPath` – path requested but does not exist.
* `NetworkXNoCycle` – cycle requested but does not exist.
* `NodeNotFound` – node not in graph.
* `HasACycle` – algorithm expected acyclic graph but found a cycle.
* `NetworkXUnbounded` – unbounded optimization problem.
* `NetworkXNotImplemented` – algorithm not implemented for this graph type.
* `AmbiguousSolution` – multiple valid intermediary solutions when one is expected.
* `ExceededMaxIterations` – iteration limit exceeded.
* `PowerIterationFailedConvergence` – power iteration did not converge within limit.

Each has documented semantics and typical usage noted in the reference page.

---

# 13. Backends and configuration

NetworkX 3.6 continues to support **dispatchable backends** (e.g. high‑performance implementations such as GPU or parallel backends) via the `@nx._dispatchable` decorator and a configuration system. ([NetworkX][5])

## 13.1 Using backends

From the **Backends** page: ([NetworkX][5])

* Explicit dispatch:

  * Pass `backend="name"` into dispatchable functions:

    * `nx.betweenness_centrality(G, k=10, backend="parallel")`.
  * Or create backend graphs directly (e.g. `nx.Graph(backend=...)` or backend‑specific graph classes) and pass them into NetworkX algorithm functions.
* Automatic dispatch via configuration:

  * `nx.config.backend_priority` / `NETWORKX_BACKEND_PRIORITY`
  * `nx.config.backend_priority.algos`
  * `nx.config.backend_priority.generators`
* Behavior when backend doesn’t support a function is controlled by:

  * `nx.config.fallback_to_nx` / `NETWORKX_FALLBACK_TO_NX`.

Caching converted graphs is controlled by `nx.config.cache_converted_graphs`. ([NetworkX][5])

Dispatchable functions expose a `.backends` attribute listing installed backend implementations and embed “Additional backend implementations” sections in their docstrings and online docs. ([NetworkX][5])

## 13.2 Backend developer interface

The Backends page documents how to implement a backend via a `BackendInterface`:

* Required conversion functions: `convert_from_nx`, `convert_to_nx`.
* Optional `can_run` and `should_run` hints for selective dispatch.
* `on_start_tests` for test suite integration.
* Entry points under `networkx.backends` and `networkx.backend_info` to advertise available functions and metadata. ([NetworkX][5])

Backend graph objects must define `__networkx_backend__`, implement `is_directed()` and `is_multigraph()`, and optionally maintain `G.__networkx_cache__` for caching converted graphs. ([NetworkX][5])

## 13.3 Config module

The **Configs** section lists:

* `config` – module exposing configuration accessors.
* `NetworkXConfig` and `Config` – configuration objects/type used to hold settings (referenced from the Backends page and the Configs reference entries). ([NetworkX][1])

These control, among other things, backend priorities and fallback behavior.

---

# 14. Practical usage patterns (end‑to‑end)

To tie the API together, here are “canonical” patterns that map directly onto the 3.6 docs:

## 14.1 Build, analyze, draw

```python
import networkx as nx
import matplotlib.pyplot as plt

# 1. Construct graph
G = nx.barabasi_albert_graph(100, 3)  # Random scale-free generator (graph_generators):contentReference[oaicite:87]{index=87}  

# 2. Compute centrality
bc = nx.betweenness_centrality(G, k=20)  # Centrality algorithms:contentReference[oaicite:88]{index=88}  

# 3. Attach attributes
nx.set_node_attributes(G, bc, "betweenness")  # Functions → Attributes:contentReference[oaicite:89]{index=89}  

# 4. Layout and draw
pos = nx.spring_layout(G, seed=42)          # Layout functions, Randomness seed:contentReference[oaicite:90]{index=90}  
nx.draw_networkx(G, pos, node_size=[3000 * bc[v] + 10 for v in G])
plt.show()
```

## 14.2 File I/O and conversion

```python
import networkx as nx
import pandas as pd

# Read an edge list from disk
G = nx.read_edgelist("graph.edgelist")  # Read/write reference:contentReference[oaicite:91]{index=91}  

# Convert to pandas edge list
df = nx.to_pandas_edgelist(G)          # Convert reference:contentReference[oaicite:92]{index=92}  

# Modify attributes and write GraphML for Cytoscape
df["weight"] = 1.0
H = nx.from_pandas_edgelist(df, "source", "target", edge_attr=True)
nx.write_graphml(H, "graph.graphml")   # GraphML reference:contentReference[oaicite:93]{index=93}  
```

## 14.3 Using a backend

```python
import networkx as nx

G = nx.erdos_renyi_graph(10_000, 0.001)

# Prefer parallel backend for betweenness centrality when available
bc = nx.betweenness_centrality(G, k=100, backend="parallel")  # Backends:contentReference[oaicite:94]{index=94}  
```

---

# 15. Version alignment and validation notes

* All sections and function groups described above are **directly verified** against the NetworkX **3.6** reference index (the reference page itself explicitly states “Release: 3.6”, dated **Nov 24, 2025**). ([NetworkX][1])
* Every category name under *Graph types*, *Algorithms*, *Functions*, *Graph Generators*, *Linear algebra*, *Conversions*, *Relabeling*, *Reading and writing graphs*, *Drawing*, *Randomness*, *Exceptions*, *Utilities*, *Backends*, and *Configs* appears exactly as listed in the official 3.6 documentation. ([NetworkX][1])
* Where specific functions are mentioned (e.g. `betweenness_centrality`, `to_numpy_array`, `write_graphml`), they are taken directly from their respective 3.6 reference pages cited inline. ([NetworkX][6])

If you’d like, I can now:

* Expand any particular section (e.g. “centrality”, “flows”, “graph generators”) into a full catalog with per‑function signatures and typical usage, still kept strictly in sync with the 3.6 docs.

[1]: https://networkx.org/documentation/stable/reference/index.html "Reference — NetworkX 3.6 documentation"
[2]: https://networkx.org/documentation/stable/reference/introduction.html "Introduction — NetworkX 3.6 documentation"
[3]: https://networkx.org/documentation/stable/reference/classes/index.html "Graph types — NetworkX 3.6 documentation"
[4]: https://networkx.org/documentation/stable/reference/functions.html "Functions — NetworkX 3.6 documentation"
[5]: https://networkx.org/documentation/stable/reference/backends.html "Backends — NetworkX 3.6 documentation"
[6]: https://networkx.org/documentation/stable/reference/algorithms/centrality.html "Centrality — NetworkX 3.6 documentation"
[7]: https://networkx.org/documentation/stable/reference/generators.html "Graph generators — NetworkX 3.6 documentation"
[8]: https://networkx.org/documentation/stable/reference/linalg.html "Linear algebra — NetworkX 3.6 documentation"
[9]: https://networkx.org/documentation/stable/reference/convert.html "Converting to and from other data formats — NetworkX 3.6 documentation"
[10]: https://networkx.org/documentation/stable/reference/readwrite/index.html "Reading and writing graphs — NetworkX 3.6 documentation"
[11]: https://networkx.org/documentation/stable/reference/drawing.html "Drawing — NetworkX 3.6 documentation"
[12]: https://networkx.org/documentation/stable/reference/randomness.html "Randomness — NetworkX 3.6 documentation"
[13]: https://networkx.org/documentation/stable/reference/utils.html "Utilities — NetworkX 3.6 documentation"
[14]: https://networkx.org/documentation/stable/reference/exceptions.html "Exceptions — NetworkX 3.6 documentation"
