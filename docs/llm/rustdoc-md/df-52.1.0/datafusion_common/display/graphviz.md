**datafusion_common > display > graphviz**

# Module: display::graphviz

## Contents

**Structs**

- [`GraphvizBuilder`](#graphvizbuilder)

---

## datafusion_common::display::graphviz::GraphvizBuilder

*Struct*

**Methods:**

- `fn next_id(self: & mut Self) -> usize`
- `fn start_graph(self: & mut Self, f: & mut fmt::Formatter) -> fmt::Result`
- `fn end_graph(self: & mut Self, f: & mut fmt::Formatter) -> fmt::Result`
- `fn start_cluster(self: & mut Self, f: & mut fmt::Formatter, title: &str) -> fmt::Result`
- `fn end_cluster(self: & mut Self, f: & mut fmt::Formatter) -> fmt::Result`
- `fn quoted(label: &str) -> String` - makes a quoted string suitable for inclusion in a graphviz chart
- `fn add_node(self: &Self, f: & mut fmt::Formatter, id: usize, label: &str, tooltip: Option<&str>) -> fmt::Result`
- `fn add_edge(self: &Self, f: & mut fmt::Formatter, from_id: usize, to_id: usize) -> fmt::Result`

**Trait Implementations:**

- **Default**
  - `fn default() -> GraphvizBuilder`



