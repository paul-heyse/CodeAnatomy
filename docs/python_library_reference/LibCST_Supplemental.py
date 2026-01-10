Below is a **single “ingest runner” script** that does what you asked in the most practical “best-in-class” way:

* **Tree-sitter parse (bytes)** → emit a **maximal node table** + build an **IntervalTree** index on `[start_byte, end_byte)`
* **LibCST parse_module(bytes)** (fidelity) → wrap in **MetadataWrapper** + traverse once with **metadata dependencies** (byte spans / line spans / parent / expr ctx / qualified names)
* **Join during ingest**: for each LibCST node’s **byte span**, find the “best” containing Tree-sitter node via the interval index (half-open spans) and emit a **crosswalk table**
* **Stream to Arrow IPC**: write **RecordBatch streams** (bounded memory, file-by-file batches) and optionally materialize a **joined/enriched** batch using **PyArrow join**.

This follows the core invariants you’ve been emphasizing: byte spans as the canonical join key, LibCST bytes parsing for encoding fidelity, and half-open span semantics in the interval index.    

---

```python
#!/usr/bin/env python3
"""
Maximalist Tree-sitter + LibCST → Arrow IPC streams, with intervaltree span-join during ingest.

Dependencies (typical):
  pip install pyarrow intervaltree libcst tree-sitter tree-sitter-python

Notes:
- IntervalTree semantics are half-open [begin, end). Never insert end <= begin.
- LibCST: prefer parse_module(bytes) for encoding fidelity; MetadataWrapper drives providers.
- Tree-sitter: parse bytes; walk with TreeCursor to avoid child(i) costs; store start/end bytes & points.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

import pyarrow as pa
import pyarrow.ipc as ipc

from intervaltree import IntervalTree

import libcst as cst
from libcst.metadata import (
    MetadataWrapper,
    PositionProvider,
    ByteSpanPositionProvider,
    WhitespaceInclusivePositionProvider,
    ParentNodeProvider,
    ExpressionContextProvider,
    ScopeProvider,
    QualifiedNameProvider,
)

from tree_sitter import Language, Parser, LANGUAGE_VERSION, MIN_COMPATIBLE_LANGUAGE_VERSION

try:
    import tree_sitter_python as tspython  # pip install tree-sitter-python
except ImportError as e:
    raise SystemExit(
        "Missing tree-sitter Python grammar. Install with: pip install tree-sitter-python"
    ) from e


# -------------------------
# Schemas (Arrow)
# -------------------------

QN_STRUCT = pa.struct(
    [
        pa.field("name", pa.large_string(), nullable=False),
        pa.field("source", pa.large_string(), nullable=False),
    ]
)

TS_NODES_SCHEMA = pa.schema(
    [
        pa.field("file_id", pa.large_string(), nullable=False),
        pa.field("ts_node_id", pa.int64(), nullable=False),

        pa.field("kind", pa.large_string(), nullable=False),
        pa.field("kind_id", pa.int32(), nullable=False),
        pa.field("grammar_id", pa.int32(), nullable=False),

        pa.field("is_named", pa.bool_(), nullable=False),
        pa.field("is_error", pa.bool_(), nullable=False),
        pa.field("has_error", pa.bool_(), nullable=False),
        pa.field("is_missing", pa.bool_(), nullable=False),
        pa.field("is_extra", pa.bool_(), nullable=False),
        pa.field("has_changes", pa.bool_(), nullable=False),

        pa.field("start_byte", pa.int64(), nullable=False),
        pa.field("end_byte", pa.int64(), nullable=False),
        pa.field("start_row", pa.int32(), nullable=False),
        pa.field("start_col", pa.int32(), nullable=False),
        pa.field("end_row", pa.int32(), nullable=False),
        pa.field("end_col", pa.int32(), nullable=False),

        pa.field("parse_state", pa.int32(), nullable=False),
        pa.field("next_parse_state", pa.int32(), nullable=False),

        # traversal metadata (handy for analytics/debug)
        pa.field("depth", pa.int32(), nullable=False),
        pa.field("field_name", pa.large_string(), nullable=True),
    ]
)

TS_PARSE_MANIFEST_SCHEMA = pa.schema(
    [
        pa.field("file_id", pa.large_string(), nullable=False),

        pa.field("lang_name", pa.large_string(), nullable=False),
        pa.field("lang_semantic_version", pa.large_string(), nullable=True),
        pa.field("lang_abi_version", pa.int32(), nullable=False),

        pa.field("binding_lang_version_max", pa.int32(), nullable=False),
        pa.field("binding_lang_version_min", pa.int32(), nullable=False),

        pa.field("root_has_error", pa.bool_(), nullable=False),
        pa.field("node_count_total", pa.int64(), nullable=False),
        pa.field("node_count_named", pa.int64(), nullable=False),
    ]
)

CST_PARSE_MANIFEST_SCHEMA = pa.schema(
    [
        pa.field("file_id", pa.large_string(), nullable=False),
        pa.field("ok", pa.bool_(), nullable=False),

        # success fields
        pa.field("encoding", pa.large_string(), nullable=True),
        pa.field("default_newline", pa.large_string(), nullable=True),
        pa.field("default_indent", pa.large_string(), nullable=True),
        pa.field("has_trailing_newline", pa.bool_(), nullable=True),
        pa.field("future_imports", pa.list_(pa.large_string()), nullable=True),

        # error fields (LibCST ParserSyntaxError)
        pa.field("error_message", pa.large_string(), nullable=True),
        pa.field("raw_line", pa.int32(), nullable=True),
        pa.field("raw_column", pa.int32(), nullable=True),
        pa.field("editor_line", pa.int32(), nullable=True),
        pa.field("editor_column", pa.int32(), nullable=True),
        pa.field("context", pa.large_string(), nullable=True),
    ]
)

CST_NODES_SCHEMA = pa.schema(
    [
        pa.field("file_id", pa.large_string(), nullable=False),
        pa.field("cst_node_id", pa.int64(), nullable=False),
        pa.field("parent_cst_node_id", pa.int64(), nullable=True),

        pa.field("kind", pa.large_string(), nullable=False),

        # canonical join span (bytes, half-open)
        pa.field("start_byte", pa.int64(), nullable=False),
        pa.field("end_byte", pa.int64(), nullable=False),

        # UI span (line/col) - semantic
        pa.field("start_line", pa.int32(), nullable=False),
        pa.field("start_col", pa.int32(), nullable=False),
        pa.field("end_line", pa.int32(), nullable=False),
        pa.field("end_col", pa.int32(), nullable=False),

        # UI span including whitespace (optional but maximalist)
        pa.field("ws_start_line", pa.int32(), nullable=True),
        pa.field("ws_start_col", pa.int32(), nullable=True),
        pa.field("ws_end_line", pa.int32(), nullable=True),
        pa.field("ws_end_col", pa.int32(), nullable=True),

        # semantic-ish extras
        pa.field("expr_ctx", pa.large_string(), nullable=True),

        # QualifiedNameProvider can return multiple; store as list<struct{name,source}>
        pa.field("qnames", pa.list_(QN_STRUCT), nullable=True),
    ]
)

# Crosswalk: join key is (file_id, cst_node_id) -> ts_node_id
XWALK_SCHEMA = pa.schema(
    [
        pa.field("file_id", pa.large_string(), nullable=False),
        pa.field("cst_node_id", pa.int64(), nullable=False),
        pa.field("ts_node_id", pa.int64(), nullable=True),
        pa.field("join_method", pa.large_string(), nullable=False),   # "at_begin" | "overlap" | "none"
        pa.field("confidence", pa.float32(), nullable=False),         # heuristic confidence
    ]
)

# Optional: an enriched join output showing PyArrow joins during ingest
CST_TS_ENRICHED_SCHEMA = pa.schema(
    [
        pa.field("file_id", pa.large_string(), nullable=False),
        pa.field("cst_node_id", pa.int64(), nullable=False),
        pa.field("kind_cst", pa.large_string(), nullable=False),
        pa.field("start_byte", pa.int64(), nullable=False),
        pa.field("end_byte", pa.int64(), nullable=False),

        pa.field("ts_node_id", pa.int64(), nullable=True),
        pa.field("kind_ts", pa.large_string(), nullable=True),
        pa.field("ts_start_byte", pa.int64(), nullable=True),
        pa.field("ts_end_byte", pa.int64(), nullable=True),
    ]
)


# -------------------------
# Streaming writer (IPC)
# -------------------------

class IPCStreamWriter:
    """
    Simple RecordBatch stream writer that writes one batch at a time.
    We write per-file batches to keep peak memory bounded.
    """
    def __init__(self, path: Path, schema: pa.Schema) -> None:
        self.path = path
        self.schema = schema
        self._sink = pa.OSFile(str(path), mode="wb")
        self._writer = ipc.new_stream(self._sink, schema)

    def write_table(self, table: pa.Table) -> None:
        # Ensure schema matches (cheap cast if needed).
        if table.schema != self.schema:
            table = table.cast(self.schema)
        for batch in table.to_batches(max_chunksize=128_000):
            self._writer.write_batch(batch)

    def close(self) -> None:
        self._writer.close()
        self._sink.close()


# -------------------------
# Utils
# -------------------------

def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def file_id_for(path: Path, content_sha256: str) -> str:
    # Stable id: hash(path + content hash) → 16-byte hex (32 chars)
    h = hashlib.blake2b(digest_size=16)
    h.update(str(path).encode("utf-8"))
    h.update(b"\x1f")
    h.update(content_sha256.encode("ascii"))
    return h.hexdigest()

def iter_py_files(root: Path) -> Iterable[Path]:
    for p in root.rglob("*.py"):
        # Skip very common vendor/cache dirs; adjust to your repo policies.
        parts = set(p.parts)
        if ".git" in parts or ".venv" in parts or "__pycache__" in parts:
            continue
        yield p


# -------------------------
# Tree-sitter extraction
# -------------------------

@dataclass(frozen=True)
class TSExtract:
    nodes: list[dict[str, Any]]
    manifest: dict[str, Any]
    index: IntervalTree
    # optional: lightweight ts lookup for enrichment
    ts_lookup: dict[int, tuple[str, int, int]]  # ts_node_id -> (kind, start_byte, end_byte)

def build_ts_parser() -> tuple[Language, Parser]:
    lang = Language(tspython.language())
    parser = Parser(lang)
    return lang, parser

def ts_walk_nodes(root_node) -> Iterable[tuple[Any, Optional[str], int]]:
    """
    Preorder traversal using TreeCursor (no recursion, low allocations).
    Yields: (node, field_name, depth)
    """
    cursor = root_node.walk()
    reached_root = False
    while not reached_root:
        yield (cursor.node, cursor.field_name, cursor.depth)
        if cursor.goto_first_child():
            continue
        if cursor.goto_next_sibling():
            continue
        while True:
            if not cursor.goto_parent():
                reached_root = True
                break
            if cursor.goto_next_sibling():
                break

def extract_tree_sitter(lang: Language, parser: Parser, file_id: str, src: bytes) -> TSExtract:
    tree = parser.parse(src)
    root = tree.root_node

    nodes: list[dict[str, Any]] = []
    index = IntervalTree()
    ts_lookup: dict[int, tuple[str, int, int]] = {}

    total = 0
    named = 0

    for node, field_name, depth in ts_walk_nodes(root):
        total += 1
        if node.is_named:
            named += 1

        sb = int(node.start_byte)
        eb = int(node.end_byte)

        # Insert into interval index only if span is valid.
        # IntervalTree uses half-open [begin,end); end must be > begin.
        if eb > sb:
            # store enough info to pick "best" container: (len, -depth)
            index.addi(sb, eb, (int(node.id), eb - sb, int(depth), str(node.type)))

        ts_node_id = int(node.id)
        ts_lookup[ts_node_id] = (str(node.type), sb, eb)

        sp = node.start_point
        ep = node.end_point

        nodes.append(
            {
                "file_id": file_id,
                "ts_node_id": ts_node_id,

                "kind": str(node.type),
                "kind_id": int(node.kind_id),
                "grammar_id": int(node.grammar_id),

                "is_named": bool(node.is_named),
                "is_error": bool(node.is_error),
                "has_error": bool(node.has_error),
                "is_missing": bool(node.is_missing),
                "is_extra": bool(node.is_extra),
                "has_changes": bool(node.has_changes),

                "start_byte": sb,
                "end_byte": eb,
                "start_row": int(sp.row),
                "start_col": int(sp.column),
                "end_row": int(ep.row),
                "end_col": int(ep.column),

                "parse_state": int(node.parse_state),
                "next_parse_state": int(node.next_parse_state),

                "depth": int(depth),
                "field_name": field_name,
            }
        )

    manifest = {
        "file_id": file_id,
        "lang_name": getattr(lang, "name", "unknown"),
        "lang_semantic_version": getattr(lang, "semantic_version", None),
        "lang_abi_version": int(getattr(lang, "abi_version", -1)),
        "binding_lang_version_max": int(LANGUAGE_VERSION),
        "binding_lang_version_min": int(MIN_COMPATIBLE_LANGUAGE_VERSION),
        "root_has_error": bool(root.has_error),
        "node_count_total": int(total),
        "node_count_named": int(named),
    }

    return TSExtract(nodes=nodes, manifest=manifest, index=index, ts_lookup=ts_lookup)


# -------------------------
# LibCST extraction (metadata-dependent visitor)
# -------------------------

def best_ts_container_for_span(ts_index: IntervalTree, start_b: int, end_b: int) -> tuple[Optional[int], str, float]:
    """
    Return (ts_node_id, join_method, confidence) for the smallest TS interval that contains [start_b,end_b).

    Strategy:
      1) point query at start (fast): candidates covering start
      2) filter to those also covering end
      3) pick smallest span; tie-break by deepest node

    If none, fall back to overlap(start,end) then same filter.
    """
    if end_b <= start_b:
        return (None, "none", 0.0)

    # 1) Fast candidates: intervals containing the start point.
    hits = ts_index.at(start_b)
    containing = [iv for iv in hits if iv.end >= end_b and iv.begin <= start_b]

    if containing:
        best = min(containing, key=lambda iv: (iv.data[1], -iv.data[2]))  # (len, -depth)
        return (int(best.data[0]), "at_begin", 1.0)

    # 2) Fallback: any overlap, then filter to “contains”
    hits2 = ts_index.overlap(start_b, end_b)
    containing2 = [iv for iv in hits2 if iv.end >= end_b and iv.begin <= start_b]
    if containing2:
        best = min(containing2, key=lambda iv: (iv.data[1], -iv.data[2]))
        return (int(best.data[0]), "overlap", 0.7)

    return (None, "none", 0.0)

class CSTCollector(cst.CSTVisitor):
    """
    Collect a maximalist per-node record set plus a CST→TS crosswalk using byte spans.
    """
    METADATA_DEPENDENCIES = (
        PositionProvider,
        ByteSpanPositionProvider,
        WhitespaceInclusivePositionProvider,
        ParentNodeProvider,
        ExpressionContextProvider,
        ScopeProvider,            # required by QualifiedNameProvider
        QualifiedNameProvider,
    )

    def __init__(self, file_id: str, ts_index: IntervalTree) -> None:
        self.file_id = file_id
        self.ts_index = ts_index

        self._next_id = 0
        self._node_to_id: dict[cst.CSTNode, int] = {}

        self.nodes: list[dict[str, Any]] = []
        self.xwalk: list[dict[str, Any]] = []

    def on_visit(self, node: cst.CSTNode) -> bool | None:
        # Assign deterministic per-file id in pre-order
        cst_id = self._next_id
        self._next_id += 1
        self._node_to_id[node] = cst_id

        kind = type(node).__name__

        # Byte span (canonical join key)
        span = self.get_metadata(ByteSpanPositionProvider, node, None)
        if span is None:
            return True

        start_b = int(span.start)
        end_b = int(span.start + span.length)

        if end_b <= start_b:
            # Degenerate span: skip, but continue traversal.
            return True

        # Line/col span (semantic)
        pos = self.get_metadata(PositionProvider, node, None)
        if pos is None:
            return True

        # Whitespace-inclusive span (optional)
        ws = self.get_metadata(WhitespaceInclusivePositionProvider, node, None)

        # Parent id via ParentNodeProvider + our id map (parent visited first)
        parent = self.get_metadata(ParentNodeProvider, node, None)
        parent_id = self._node_to_id.get(parent) if parent is not None else None

        # Expression context (LOAD/STORE/DEL) for some node types
        expr_ctx = None
        try:
            ctx = self.get_metadata(ExpressionContextProvider, node, None)
            if ctx is not None:
                expr_ctx = str(ctx)  # enum-ish
        except Exception:
            expr_ctx = None

        # Qualified names (can be multiple)
        qnames_val = None
        try:
            qnames = self.get_metadata(QualifiedNameProvider, node, None)
            if qnames:
                qnames_val = [{"name": q.name, "source": str(q.source)} for q in sorted(qnames, key=lambda x: (x.name, str(x.source)))]
        except Exception:
            qnames_val = None

        # Join to Tree-sitter via intervaltree
        ts_id, join_method, conf = best_ts_container_for_span(self.ts_index, start_b, end_b)

        self.nodes.append(
            {
                "file_id": self.file_id,
                "cst_node_id": int(cst_id),
                "parent_cst_node_id": int(parent_id) if parent_id is not None else None,
                "kind": kind,

                "start_byte": start_b,
                "end_byte": end_b,

                "start_line": int(pos.start.line),
                "start_col": int(pos.start.column),
                "end_line": int(pos.end.line),
                "end_col": int(pos.end.column),

                "ws_start_line": int(ws.start.line) if ws is not None else None,
                "ws_start_col": int(ws.start.column) if ws is not None else None,
                "ws_end_line": int(ws.end.line) if ws is not None else None,
                "ws_end_col": int(ws.end.column) if ws is not None else None,

                "expr_ctx": expr_ctx,
                "qnames": qnames_val,
            }
        )

        self.xwalk.append(
            {
                "file_id": self.file_id,
                "cst_node_id": int(cst_id),
                "ts_node_id": int(ts_id) if ts_id is not None else None,
                "join_method": join_method,
                "confidence": float(conf),
            }
        )

        return True


def extract_libcst(file_id: str, src: bytes, ts_index: IntervalTree) -> tuple[dict[str, Any], Optional[pa.Table], Optional[pa.Table]]:
    """
    Returns:
      (parse_manifest_row, cst_nodes_table_or_none, xwalk_table_or_none)

    If parsing fails, tables are None, but manifest records error details.
    """
    try:
        mod = cst.parse_module(src)  # bytes-first for fidelity
    except cst.ParserSyntaxError as ex:
        manifest = {
            "file_id": file_id,
            "ok": False,
            "encoding": None,
            "default_newline": None,
            "default_indent": None,
            "has_trailing_newline": None,
            "future_imports": None,
            "error_message": ex.message,
            "raw_line": int(ex.raw_line),
            "raw_column": int(ex.raw_column),
            "editor_line": int(ex.editor_line),
            "editor_column": int(ex.editor_column),
            "context": ex.context,
        }
        return manifest, None, None

    # Parse ok → manifest fields
    manifest = {
        "file_id": file_id,
        "ok": True,
        "encoding": getattr(mod, "encoding", None),
        "default_newline": getattr(mod, "default_newline", None),
        "default_indent": getattr(mod, "default_indent", None),
        "has_trailing_newline": bool(getattr(mod, "has_trailing_newline", False)),
        "future_imports": list(getattr(mod, "future_imports", ())),
        "error_message": None,
        "raw_line": None,
        "raw_column": None,
        "editor_line": None,
        "editor_column": None,
        "context": None,
    }

    # MetadataWrapper drives providers; visitor sees wrapper.module nodes.
    wrapper = MetadataWrapper(mod)  # deep-copies by default; safe identity semantics.
    vis = CSTCollector(file_id=file_id, ts_index=ts_index)
    wrapper.visit(vis)

    cst_tbl = pa.Table.from_pylist(vis.nodes, schema=CST_NODES_SCHEMA) if vis.nodes else pa.table([], schema=CST_NODES_SCHEMA)
    xw_tbl = pa.Table.from_pylist(vis.xwalk, schema=XWALK_SCHEMA) if vis.xwalk else pa.table([], schema=XWALK_SCHEMA)
    return manifest, cst_tbl, xw_tbl


# -------------------------
# Optional: Arrow join during ingest (per file)
# -------------------------

def build_enriched_cst_ts(
    cst_nodes: pa.Table,
    xwalk: pa.Table,
    ts_nodes: pa.Table,
) -> pa.Table:
    """
    Demonstrates a pure-PyArrow join path:

      cst_nodes LEFT JOIN xwalk ON (file_id,cst_node_id)
                LEFT JOIN ts_nodes ON (file_id,ts_node_id)

    This is optional; you can always persist cst_nodes + ts_nodes + xwalk and join later.
    """
    # Reduce ts_nodes to the columns we want to attach
    ts_small = ts_nodes.select(["file_id", "ts_node_id", "kind", "start_byte", "end_byte"]).rename_columns(
        ["file_id", "ts_node_id", "kind_ts", "ts_start_byte", "ts_end_byte"]
    )

    # Join 1: CST -> XWALK
    cx = cst_nodes.join(
        xwalk,
        keys=["file_id", "cst_node_id"],
        join_type="left outer",
    )

    # Join 2: (CST+X) -> TS
    cxt = cx.join(
        ts_small,
        keys=["file_id", "ts_node_id"],
        join_type="left outer",
    )

    # Normalize to a compact enriched schema
    out = cxt.select(
        [
            "file_id",
            "cst_node_id",
            pa.compute.if_else(pa.scalar(True), cxt["kind"], cxt["kind"]).cast(pa.large_string()).rename("kind_cst"),
            "start_byte",
            "end_byte",
            "ts_node_id",
            "kind_ts",
            "ts_start_byte",
            "ts_end_byte",
        ]
    ).cast(CST_TS_ENRICHED_SCHEMA)

    return out


# -------------------------
# Main runner
# -------------------------

@dataclass(frozen=True)
class RunConfig:
    repo_root: Path
    out_dir: Path

def run(cfg: RunConfig) -> None:
    cfg.out_dir.mkdir(parents=True, exist_ok=True)

    # Writers (Arrow IPC streams)
    ts_nodes_w = IPCStreamWriter(cfg.out_dir / "ts_nodes.arrow", TS_NODES_SCHEMA)
    ts_manifest_w = IPCStreamWriter(cfg.out_dir / "ts_parse_manifest.arrow", TS_PARSE_MANIFEST_SCHEMA)
    cst_manifest_w = IPCStreamWriter(cfg.out_dir / "cst_parse_manifest.arrow", CST_PARSE_MANIFEST_SCHEMA)
    cst_nodes_w = IPCStreamWriter(cfg.out_dir / "cst_nodes.arrow", CST_NODES_SCHEMA)
    xwalk_w = IPCStreamWriter(cfg.out_dir / "cst_to_ts_xwalk.arrow", XWALK_SCHEMA)

    enriched_w = IPCStreamWriter(cfg.out_dir / "cst_ts_enriched.arrow", CST_TS_ENRICHED_SCHEMA)

    lang, parser = build_ts_parser()

    for path in iter_py_files(cfg.repo_root):
        src = path.read_bytes()
        ch = sha256_hex(src)
        fid = file_id_for(path.relative_to(cfg.repo_root), ch)

        # 1) Tree-sitter
        ts = extract_tree_sitter(lang, parser, fid, src)
        ts_nodes_tbl = pa.Table.from_pylist(ts.nodes, schema=TS_NODES_SCHEMA) if ts.nodes else pa.table([], schema=TS_NODES_SCHEMA)
        ts_manifest_tbl = pa.Table.from_pylist([ts.manifest], schema=TS_PARSE_MANIFEST_SCHEMA)

        ts_nodes_w.write_table(ts_nodes_tbl)
        ts_manifest_w.write_table(ts_manifest_tbl)

        # 2) LibCST (+ join during ingest)
        cst_manifest_row, cst_tbl, xw_tbl = extract_libcst(fid, src, ts.index)
        cst_manifest_tbl = pa.Table.from_pylist([cst_manifest_row], schema=CST_PARSE_MANIFEST_SCHEMA)
        cst_manifest_w.write_table(cst_manifest_tbl)

        if cst_tbl is None or xw_tbl is None:
            # Parse failed; still keep tree-sitter outputs.
            continue

        cst_nodes_w.write_table(cst_tbl)
        xwalk_w.write_table(xw_tbl)

        # 3) Optional “enriched” join output using PyArrow joins per file.
        #    If you prefer zero extra work, skip this and join later using the three base tables.
        enriched = build_enriched_cst_ts(cst_tbl, xw_tbl, ts_nodes_tbl)
        enriched_w.write_table(enriched)

    # Close writers
    for w in (ts_nodes_w, ts_manifest_w, cst_manifest_w, cst_nodes_w, xwalk_w, enriched_w):
        w.close()


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo-root", required=True, type=Path, help="Path to repo root")
    ap.add_argument("--out-dir", required=True, type=Path, help="Output directory for Arrow IPC streams")
    ns = ap.parse_args(argv)

    run(RunConfig(repo_root=ns.repo_root.resolve(), out_dir=ns.out_dir.resolve()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
```

---

## Why this is “best-in-class” for your stated goal

* **LibCST bytes-first parse** (encoding fidelity + stable byte spans) and metadata-driven extraction are explicitly aligned with your LibCST ingest rules.  
* **Tree-sitter traversal uses TreeCursor**, which is the low-allocation pattern you want at scale, and it captures parse-state + error/missing flags for maximal diagnostics.  
* **IntervalTree is used as the ingest-time span join accelerator**, honoring half-open spans `[begin,end)` and avoiding degenerate intervals. 
* **Arrow IPC stream writing** keeps peak memory bounded and produces Arrow-native artifacts that you can scan/join later. Arrow’s nested types (like `list<struct<...>>`) are first-class, so you can keep “maximalist” metadata columns without flattening. 

---

## Reading + joining later (pure PyArrow)

If you skip the `cst_ts_enriched.arrow` output, you can still join from the three base streams:

```python
import pyarrow as pa
import pyarrow.ipc as ipc

ts = ipc.open_stream("out/ts_nodes.arrow").read_all()
cst = ipc.open_stream("out/cst_nodes.arrow").read_all()
xw = ipc.open_stream("out/cst_to_ts_xwalk.arrow").read_all()

cst_x = cst.join(xw, keys=["file_id","cst_node_id"], join_type="left outer")
cst_x_ts = cst_x.join(ts.select(["file_id","ts_node_id","kind"]), keys=["file_id","ts_node_id"], join_type="left outer")
```

(That’s exactly why the ingest-time `xwalk` is such a high-leverage artifact: it turns interval joins into equality joins.)

---

If you want, I can extend the same script to also emit **LibCST ScopeProvider “access/assignment” tables** (separate Arrow streams) and/or **Tree-sitter QueryCursor capture tables** (query-pack driven extracts), while keeping the ingest streaming and join-safe.
