Tree-sitter gives you a **syntactic CST** with **parents/children/siblings + byte/point spans**; you can turn that into a “neighborhood view” by (1) choosing a stable **anchor node** for the symbol occurrence, then (2) materializing a small **local subgraph**:

* **Parents:** enclosing constructs (attribute → call → statement → function → class → module)
* **Children:** immediate structure under the anchor (or under the “semantic container” you pick)
* **Siblings:** immediate prev/next nodes at the same parent (plus statement-level siblings)
* **“Conjunction peers”:** *other children of the same expression/statement container* (arguments, other operands, RHS/LHS, attribute object, etc.)

Tree-sitter’s Python bindings expose exactly the primitives you need: `Node.parent`, `Node.named_children`, `Node.prev_named_sibling/next_named_sibling`, and point/byte descendant selectors. ([Tree-sitter][1])

---

## Neighborhood view recipe

### 1) Parse + load the Python grammar

Use `tree-sitter` + the `tree-sitter-python` grammar wheel:

* `PY_LANGUAGE = Language(tspython.language())`
* `parser = Parser(PY_LANGUAGE)`
* `tree = parser.parse(source_bytes)` ([GitHub][2])

### 2) Pick the anchor node for the “symbol”

Common choices:

* **Leaf token**: the `identifier` node at a cursor position (best for “symbol at caret”)
* **Semantic container**: the nearest ancestor that “explains the use” (e.g., `call`, `attribute`, `assignment`, `import_statement`, etc.)

To locate the leaf from a cursor (line0/col0), use:

* `root.named_descendant_for_point_range((line, col), (line, col))` (or `descendant_for_point_range`) ([Tree-sitter][1])

### 3) Materialize neighborhood layers

* **Parent chain:** follow `node.parent` repeatedly ([Tree-sitter][1])
* **Children:** `node.named_children` (optionally depth-limited) ([Tree-sitter][1])
* **Immediate siblings:** `prev_named_sibling` / `next_named_sibling` ([Tree-sitter][1])
* **Statement siblings:** climb to the node whose parent is `block` or `module`, then take its prev/next named siblings (this gives “what happens right above/below”). ([GitHub][2])
* **Conjunction peers:** for each ancestor *until you hit `block/module`*, include **other named children of that ancestor** besides the path-to-anchor child. This captures “used in conjunction with” (other args, other operands, RHS, decorator list, etc.). ([Tree-sitter][1])

### 4) Add “field-aware” structure labels (enrichment)

For better context than “just siblings,” annotate relationships with **field names** (e.g., `function:`, `arguments:`, `condition:`, `body:`) via:

* `parent.field_name_for_child(i)` / `child_by_field_name(name)` / `children_by_field_name(name)` ([Tree-sitter][1])

For large traversals, a `TreeCursor` is faster and provides cursor movement ops like `goto_parent()` / `goto_next_sibling()`. ([Tree-sitter][3])

---

## Reference implementation (Python)

```python
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Optional

import tree_sitter_python as tspython
from tree_sitter import Language, Parser, Node


PY_LANGUAGE = Language(tspython.language())
PARSER = Parser(PY_LANGUAGE)


# ----------------------------
# Basic “node facts” container
# ----------------------------
@dataclass(frozen=True)
class NodeInfo:
    type: str
    is_named: bool
    start_byte: int
    end_byte: int
    start_point: tuple[int, int]
    end_point: tuple[int, int]
    text: str
    field_in_parent: Optional[str] = None
    node_id: Optional[int] = None  # useful cache key within a Tree


def _slice_text(src: bytes, node: Node, *, max_len: int = 200) -> str:
    raw = src[node.start_byte : node.end_byte]
    s = raw.decode("utf-8", errors="replace")
    s = " ".join(s.split())  # cheap normalization
    return s if len(s) <= max_len else (s[: max_len - 1] + "…")


def _field_name_in_parent(node: Node) -> Optional[str]:
    p = node.parent
    if p is None:
        return None
    # Find the child index by identity; then ask parent for the field name.
    # (Node.child(i) is log(i); using p.children list is typically fine for modest arity.)
    for i, ch in enumerate(p.children):
        if ch == node:
            return p.field_name_for_child(i)
    return None


def node_info(src: bytes, node: Node) -> NodeInfo:
    return NodeInfo(
        type=node.type,
        is_named=node.is_named,
        start_byte=node.start_byte,
        end_byte=node.end_byte,
        start_point=tuple(node.start_point),
        end_point=tuple(node.end_point),
        text=_slice_text(src, node),
        field_in_parent=_field_name_in_parent(node),
        node_id=getattr(node, "id", None),
    )


# ----------------------------
# Anchor selection
# ----------------------------
def node_at_point(root: Node, line0: int, col0: int) -> Node:
    pt = (line0, col0)
    # Smallest *named* node spanning this point is usually what you want for “symbol at caret”
    n = root.named_descendant_for_point_range(pt, pt)
    return n if n is not None else root.descendant_for_point_range(pt, pt)


def lift_to_container(node: Node, *, stop_types: set[str]) -> Node:
    """
    Optional heuristic: lift leaf identifiers to a “meaningful container” like call/attribute/assignment.
    """
    cur = node
    while cur.parent is not None and cur.type not in stop_types:
        cur = cur.parent
    return cur


# ----------------------------
# Neighborhood construction
# ----------------------------
def parents(node: Node, *, max_depth: int = 8) -> list[Node]:
    out = []
    cur = node.parent
    while cur is not None and len(out) < max_depth:
        out.append(cur)
        cur = cur.parent
    return out


def children(node: Node, *, max_depth: int = 2, max_per_node: int = 25) -> list[Node]:
    """
    Depth-limited DFS over named children. Keep this small — “neighborhood view” should be cheap.
    """
    out: list[Node] = []

    def rec(n: Node, d: int) -> None:
        if d > max_depth:
            return
        for ch in n.named_children[:max_per_node]:
            out.append(ch)
            rec(ch, d + 1)

    rec(node, 1)
    return out


def sibling_chain(node: Node, *, direction: str, limit: int = 3) -> list[Node]:
    out = []
    cur = node
    for _ in range(limit):
        cur = cur.prev_named_sibling if direction == "prev" else cur.next_named_sibling
        if cur is None:
            break
        out.append(cur)
    return out


def statement_node(node: Node) -> Node:
    """
    Heuristic: climb until your parent is a block/module.
    That node is typically a “statement-ish” unit in tree-sitter-python.
    """
    cur = node
    while cur.parent is not None and cur.parent.type not in {"block", "module"}:
        cur = cur.parent
    return cur


def conjunction_peers(anchor: Node, *, max_levels: int = 6, max_peers: int = 20) -> list[dict[str, Any]]:
    """
    For each ancestor (until we hit block/module), return the other named children of that ancestor.
    This approximates “used in conjunction with” within expressions.
    """
    out: list[dict[str, Any]] = []
    cur = anchor
    for _ in range(max_levels):
        p = cur.parent
        if p is None or p.type in {"block", "module"}:
            break

        # peers = other named children of p besides cur
        peers = [ch for ch in p.named_children if ch != cur][:max_peers]
        out.append(
            {
                "container_type": p.type,
                "container_id": getattr(p, "id", None),
                "path_child_type": cur.type,
                "peers": peers,
            }
        )
        cur = p
    return out


def neighborhood_view(
    source: bytes,
    *,
    line0: int,
    col0: int,
    lift_containers: bool = True,
) -> dict[str, Any]:
    tree = PARSER.parse(source)
    root = tree.root_node

    leaf = node_at_point(root, line0, col0)

    # Optional: lift to something more explanatory than the bare identifier
    container_stops = {
        "call",
        "attribute",
        "assignment",
        "import_statement",
        "import_from_statement",
        "return_statement",
        "if_statement",
        "for_statement",
        "while_statement",
        "with_statement",
        "binary_operator",
        "comparison_operator",
        "subscript",
    }
    anchor = lift_to_container(leaf, stop_types=container_stops) if lift_containers else leaf

    stmt = statement_node(anchor)

    view = {
        "anchor": asdict(node_info(source, anchor)),
        "leaf": asdict(node_info(source, leaf)),
        "parents": [asdict(node_info(source, n)) for n in parents(anchor)],
        "children": [asdict(node_info(source, n)) for n in children(anchor)],
        "siblings": {
            "prev": [asdict(node_info(source, n)) for n in sibling_chain(anchor, direction="prev")],
            "next": [asdict(node_info(source, n)) for n in sibling_chain(anchor, direction="next")],
        },
        "statement": asdict(node_info(source, stmt)),
        "statement_siblings": {
            "prev": [asdict(node_info(source, n)) for n in sibling_chain(stmt, direction="prev")],
            "next": [asdict(node_info(source, n)) for n in sibling_chain(stmt, direction="next")],
        },
        "conjunction": [
            {
                **lvl,
                "container": asdict(node_info(source, next(n for n in parents(anchor, max_depth=20) if getattr(n, "id", None) == lvl["container_id"]))),
                "peers": [asdict(node_info(source, p)) for p in lvl["peers"]],
            }
            for lvl in conjunction_peers(anchor)
        ],
    }

    return view


# ----------------------------
# Example usage
# ----------------------------
if __name__ == "__main__":
    src = b"""
def f(x):
    y = g(x, 1) + h(x)
    return y
"""
    # Point at the `x` inside g(x, 1)
    v = neighborhood_view(src, line0=2, col0=10)
    # print(v)  # feed this into your cq “neighbors” formatter
    print(v["anchor"]["type"], v["anchor"]["text"])
```

Why these pieces work (and are stable in the binding):

* `Node.parent`, `prev_named_sibling/next_named_sibling`, and `named_children` are first-class `Node` attributes. ([Tree-sitter][1])
* You can locate a node at a cursor using `named_descendant_for_point_range` / `descendant_for_point_range`. ([Tree-sitter][1])
* “Field labels” come from `field_name_for_child` / `child_by_field_name` and are extremely helpful for explaining roles like `function:` vs `arguments:`. ([Tree-sitter][1])
* The `Parser` consumes bytes (or a read callback) and produces a `Tree`. ([Tree-sitter][4])

---

## Optional “extra enrichment” you’ll probably want next

### A) Stable IDs for caching / incremental updates

`Node.id` is unique within a tree and can remain the same across incremental re-parses when a node is reused. That’s very handy for “neighborhood caches keyed by (file_hash, node_id)”. ([Tree-sitter][1])

### B) Same-identifier “local scope co-occurrence” (still syntactic)

Use a tree-sitter `Query` to capture `(identifier) @id` inside the nearest enclosing `function_definition`/`class_definition`/`module`, then filter by `node.text == leaf.text`. The py-tree-sitter README shows the `Query` + `QueryCursor.captures()` pattern. ([GitHub][2])

---

Internal references you already have in-project:  



[1]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Node.html "Node — py-tree-sitter 0.25.2 documentation"
[2]: https://github.com/tree-sitter/py-tree-sitter "GitHub - tree-sitter/py-tree-sitter: Python bindings to the Tree-sitter parsing library"
[3]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.TreeCursor.html "TreeCursor — py-tree-sitter 0.25.2 documentation"
[4]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Parser.html?utm_source=chatgpt.com "Parser — py-tree-sitter 0.25.2 documentation"
