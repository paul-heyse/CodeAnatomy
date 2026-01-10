"""Extract Python bytecode artifacts into Arrow tables."""

from __future__ import annotations

import dis
import types as pytypes
from collections.abc import Sequence
from dataclasses import dataclass

import pyarrow as pa

from .repo_scan import stable_id

SCHEMA_VERSION = 1


@dataclass(frozen=True)
class BytecodeExtractOptions:
    """
    Bytecode extraction options.

    - adaptive=False is the canonical stream for stable semantics across runs
      (per your recipe). :contentReference[oaicite:17]{index=17}
    - include_cfg_derivations implements the blocks/CFG edges derivation recipe.
      :contentReference[oaicite:18]{index=18}
    """

    optimize: int = 0
    dont_inherit: bool = True
    adaptive: bool = False
    include_cfg_derivations: bool = True
    terminator_opnames: Sequence[str] = ("RETURN_VALUE", "RETURN_CONST", "RAISE_VARARGS", "RERAISE")


@dataclass(frozen=True)
class BytecodeExtractResult:
    """Extracted bytecode tables for code units, instructions, and edges."""

    py_bc_code_units: pa.Table
    py_bc_instructions: pa.Table
    py_bc_exception_table: pa.Table
    py_bc_blocks: pa.Table
    py_bc_cfg_edges: pa.Table
    py_bc_errors: pa.Table


CODE_UNITS_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("code_unit_id", pa.string()),
        ("parent_code_unit_id", pa.string()),
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("qualpath", pa.string()),
        ("co_name", pa.string()),
        ("firstlineno", pa.int32()),
        ("argcount", pa.int32()),
        ("posonlyargcount", pa.int32()),
        ("kwonlyargcount", pa.int32()),
        ("nlocals", pa.int32()),
        ("flags", pa.int32()),
        ("stacksize", pa.int32()),
        ("code_len", pa.int32()),
    ]
)

INSTR_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("instr_id", pa.string()),
        ("code_unit_id", pa.string()),
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("instr_index", pa.int32()),
        ("offset", pa.int32()),
        ("opname", pa.string()),
        ("opcode", pa.int32()),
        ("arg", pa.int32()),
        ("argval_str", pa.string()),
        ("argrepr", pa.string()),
        ("is_jump_target", pa.bool_()),
        ("jump_target_offset", pa.int32()),
        ("starts_line", pa.int32()),
        ("pos_start_line", pa.int32()),
        ("pos_end_line", pa.int32()),
        ("pos_start_col", pa.int32()),
        ("pos_end_col", pa.int32()),
    ]
)

EXC_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("exc_entry_id", pa.string()),
        ("code_unit_id", pa.string()),
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("exc_index", pa.int32()),
        ("start_offset", pa.int32()),
        ("end_offset", pa.int32()),
        ("target_offset", pa.int32()),
        ("depth", pa.int32()),
        ("lasti", pa.bool_()),
    ]
)

BLOCKS_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("block_id", pa.string()),
        ("code_unit_id", pa.string()),
        ("start_offset", pa.int32()),
        ("end_offset", pa.int32()),
        ("kind", pa.string()),
    ]
)

CFG_EDGES_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("edge_id", pa.string()),
        ("code_unit_id", pa.string()),
        ("src_block_id", pa.string()),
        ("dst_block_id", pa.string()),
        ("kind", pa.string()),  # next|jump|branch|exc
        ("cond_instr_id", pa.string()),
        ("exc_index", pa.int32()),
    ]
)

ERRORS_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("error_type", pa.string()),
        ("message", pa.string()),
    ]
)


def _jump_target(ins: dis.Instruction) -> int | None:
    jt = getattr(ins, "jump_target", None)
    if isinstance(jt, int):
        return jt
    jt = getattr(ins, "jump_target_offset", None)
    if isinstance(jt, int):
        return jt
    if ins.opcode in dis.hasjabs or ins.opcode in dis.hasjrel:
        if isinstance(ins.argval, int):
            return int(ins.argval)
    return None


def _is_unconditional_jump(opname: str) -> bool:
    if not opname.startswith("JUMP"):
        return False
    # heuristic: "IF" jumps are conditional, FOR_ITER is conditional-ish
    return ("IF" not in opname) and (opname != "FOR_ITER")


def _iter_code_objects(co: pytypes.CodeType, parent: pytypes.CodeType | None = None):
    yield co, parent
    for c in co.co_consts:
        if isinstance(c, pytypes.CodeType):
            yield from _iter_code_objects(c, co)


def extract_bytecode(
    repo_files: pa.Table, options: BytecodeExtractOptions | None = None
) -> BytecodeExtractResult:
    """Extract bytecode tables from repository files.

    Returns
    -------
    BytecodeExtractResult
        Tables for bytecode code units, instructions, exception data, and edges.
    """
    options = options or BytecodeExtractOptions()

    cu_rows: list[dict] = []
    ins_rows: list[dict] = []
    exc_rows: list[dict] = []
    blk_rows: list[dict] = []
    edge_rows: list[dict] = []
    err_rows: list[dict] = []

    for rf in repo_files.to_pylist():
        file_id = rf["file_id"]
        path = rf["path"]
        file_sha256 = rf.get("file_sha256")
        text = rf.get("text")
        if not text:
            b = rf.get("bytes")
            if b is not None:
                try:
                    text = b.decode(rf.get("encoding") or "utf-8", errors="replace")
                except Exception:
                    text = None
        if not text:
            continue

        try:
            top = compile(
                text,
                path,
                "exec",
                dont_inherit=options.dont_inherit,
                optimize=options.optimize,
            )
        except Exception as e:
            err_rows.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "file_id": file_id,
                    "path": path,
                    "file_sha256": file_sha256,
                    "error_type": type(e).__name__,
                    "message": str(e),
                }
            )
            continue

        # Assign deterministic code_unit_id + qualpath
        co_to_id: dict[int, str] = {}
        co_to_qual: dict[int, str] = {}
        co_to_parent_id: dict[int, str | None] = {}

        def _qual_for(co: pytypes.CodeType, parent_qual: str | None) -> str:
            name = co.co_name
            if parent_qual is None:
                return "<module>"
            # disambiguation: include firstlineno to reduce collisions
            return f"{parent_qual}.{name}@{co.co_firstlineno}"

        for co, parent in _iter_code_objects(top):
            parent_id = None
            parent_qual = None
            if parent is not None:
                parent_id = co_to_id.get(id(parent))
                parent_qual = co_to_qual.get(id(parent))
            qual = _qual_for(co, parent_qual)
            cu_id = stable_id("bc_code", file_id, qual, str(co.co_firstlineno), co.co_name)
            co_to_id[id(co)] = cu_id
            co_to_qual[id(co)] = qual
            co_to_parent_id[id(co)] = parent_id

            cu_rows.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "code_unit_id": cu_id,
                    "parent_code_unit_id": parent_id,
                    "file_id": file_id,
                    "path": path,
                    "file_sha256": file_sha256,
                    "qualpath": qual,
                    "co_name": co.co_name,
                    "firstlineno": int(co.co_firstlineno or 0),
                    "argcount": int(getattr(co, "co_argcount", 0)),
                    "posonlyargcount": int(getattr(co, "co_posonlyargcount", 0)),
                    "kwonlyargcount": int(getattr(co, "co_kwonlyargcount", 0)),
                    "nlocals": int(getattr(co, "co_nlocals", 0)),
                    "flags": int(getattr(co, "co_flags", 0)),
                    "stacksize": int(getattr(co, "co_stacksize", 0)),
                    "code_len": len(co.co_code),
                }
            )

        # Extract per code unit: instructions + exception table (+ blocks/cfg)
        for co, _parent in _iter_code_objects(top):
            cu_id = co_to_id[id(co)]

            insns = list(dis.get_instructions(co, adaptive=options.adaptive))
            by_off = {int(i.offset): idx for idx, i in enumerate(insns)}

            # Exception table
            try:
                bc = dis.Bytecode(co)
                exc_entries = list(getattr(bc, "exception_entries", []))
            except Exception:
                exc_entries = []

            for k, ex in enumerate(exc_entries):
                exc_rows.append(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "exc_entry_id": stable_id("bc_exc", cu_id, str(k)),
                        "code_unit_id": cu_id,
                        "file_id": file_id,
                        "path": path,
                        "file_sha256": file_sha256,
                        "exc_index": int(k),
                        "start_offset": int(getattr(ex, "start", 0)),
                        "end_offset": int(getattr(ex, "end", 0)),
                        "target_offset": int(getattr(ex, "target", 0)),
                        "depth": int(getattr(ex, "depth", 0)),
                        "lasti": bool(getattr(ex, "lasti", False)),
                    }
                )

            # Instructions
            for idx, ins in enumerate(insns):
                jt = _jump_target(ins)
                pos = getattr(ins, "positions", None)

                pos_start_line = getattr(pos, "lineno", None) if pos else None
                pos_end_line = getattr(pos, "end_lineno", None) if pos else None
                pos_start_col = getattr(pos, "col_offset", None) if pos else None
                pos_end_col = getattr(pos, "end_col_offset", None) if pos else None

                ins_rows.append(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "instr_id": stable_id("bc_insn", cu_id, str(idx), str(ins.offset)),
                        "code_unit_id": cu_id,
                        "file_id": file_id,
                        "path": path,
                        "file_sha256": file_sha256,
                        "instr_index": int(idx),
                        "offset": int(ins.offset),
                        "opname": ins.opname,
                        "opcode": int(ins.opcode),
                        "arg": int(ins.arg) if ins.arg is not None else None,
                        "argval_str": str(ins.argval) if ins.argval is not None else None,
                        "argrepr": ins.argrepr,
                        "is_jump_target": bool(ins.is_jump_target),
                        "jump_target_offset": int(jt) if jt is not None else None,
                        "starts_line": int(ins.starts_line)
                        if ins.starts_line is not None
                        else None,
                        "pos_start_line": int(pos_start_line)
                        if pos_start_line is not None
                        else None,
                        "pos_end_line": int(pos_end_line) if pos_end_line is not None else None,
                        "pos_start_col": int(pos_start_col) if pos_start_col is not None else None,
                        "pos_end_col": int(pos_end_col) if pos_end_col is not None else None,
                    }
                )

            if not options.include_cfg_derivations:
                continue

            # Derive blocks + edges (Step 1-4 recipe)
            # Boundaries: 0, jump targets, after terminators, exception start/end/target, end of code
            boundaries: set[int] = {0, len(co.co_code)}
            for ins in insns:
                jt = _jump_target(ins)
                if jt is not None:
                    boundaries.add(int(jt))
                if ins.opname in options.terminator_opnames or _is_unconditional_jump(ins.opname):
                    # boundary at next instruction start
                    nxt_idx = by_off.get(int(ins.offset), -1) + 1
                    if 0 <= nxt_idx < len(insns):
                        boundaries.add(int(insns[nxt_idx].offset))

            for ex in exc_entries:
                boundaries.add(int(getattr(ex, "start", 0)))
                boundaries.add(int(getattr(ex, "end", 0)))
                boundaries.add(int(getattr(ex, "target", 0)))

            # Keep only boundaries that align to instruction offsets or end-of-code
            ins_offsets = set(int(i.offset) for i in insns)
            filtered = sorted(b for b in boundaries if (b in ins_offsets) or (b == len(co.co_code)))

            # Build blocks
            blocks: list[tuple[int, int]] = []
            for i in range(len(filtered) - 1):
                s = filtered[i]
                e = filtered[i + 1]
                if s < e:
                    blocks.append((s, e))

            # offset -> block_id
            off_to_block: dict[int, str] = {}
            target_offsets = {int(getattr(ex, "target", 0)) for ex in exc_entries}

            for s, e in blocks:
                kind = "entry" if s == 0 else ("handler" if s in target_offsets else "normal")
                block_id = stable_id("bc_block", cu_id, str(s), str(e))
                blk_rows.append(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "block_id": block_id,
                        "code_unit_id": cu_id,
                        "start_offset": int(s),
                        "end_offset": int(e),
                        "kind": kind,
                    }
                )
                # Map each instruction offset in [s,e) to this block
                for off in sorted(o for o in ins_offsets if s <= o < e):
                    off_to_block[off] = block_id

            # Normal edges
            # Identify last instruction of each block and emit fallthrough/jump edges
            # Note: block boundaries are instruction offsets; last insn is max offset in [s,e).
            for s, e in blocks:
                ins_in_block = [i for i in insns if s <= int(i.offset) < e]
                if not ins_in_block:
                    continue
                last = ins_in_block[-1]
                src = off_to_block[int(ins_in_block[0].offset)]
                last_id = stable_id(
                    "bc_insn", cu_id, str(by_off[int(last.offset)]), str(last.offset)
                )

                jt = _jump_target(last)
                if jt is not None and jt in off_to_block:
                    dst = off_to_block[int(jt)]
                    kind = "jump" if _is_unconditional_jump(last.opname) else "branch"
                    edge_rows.append(
                        {
                            "schema_version": SCHEMA_VERSION,
                            "edge_id": stable_id("bc_cfg", cu_id, src, dst, kind, str(last.offset)),
                            "code_unit_id": cu_id,
                            "src_block_id": src,
                            "dst_block_id": dst,
                            "kind": kind,
                            "cond_instr_id": last_id if kind == "branch" else None,
                            "exc_index": None,
                        }
                    )

                    # fallthrough for conditional branches
                    if kind == "branch":
                        # next block = the one starting at e (if exists)
                        next_block = off_to_block.get(e)
                        if next_block is not None:
                            edge_rows.append(
                                {
                                    "schema_version": SCHEMA_VERSION,
                                    "edge_id": stable_id(
                                        "bc_cfg", cu_id, src, next_block, "next", str(e)
                                    ),
                                    "code_unit_id": cu_id,
                                    "src_block_id": src,
                                    "dst_block_id": next_block,
                                    "kind": "next",
                                    "cond_instr_id": last_id,
                                    "exc_index": None,
                                }
                            )
                    continue

                # terminators: no fallthrough
                if last.opname in options.terminator_opnames:
                    continue

                # otherwise fallthrough
                next_block = off_to_block.get(e)
                if next_block is not None:
                    edge_rows.append(
                        {
                            "schema_version": SCHEMA_VERSION,
                            "edge_id": stable_id("bc_cfg", cu_id, src, next_block, "next", str(e)),
                            "code_unit_id": cu_id,
                            "src_block_id": src,
                            "dst_block_id": next_block,
                            "kind": "next",
                            "cond_instr_id": None,
                            "exc_index": None,
                        }
                    )

            # Exception edges
            for k, ex in enumerate(exc_entries):
                start = int(getattr(ex, "start", 0))
                end = int(getattr(ex, "end", 0))
                target = int(getattr(ex, "target", 0))
                handler = off_to_block.get(target)
                if handler is None:
                    continue

                # all blocks that overlap [start,end)
                for s, e in blocks:
                    if not (e <= start or s >= end):
                        src = off_to_block.get(s)
                        if src is None:
                            continue
                        edge_rows.append(
                            {
                                "schema_version": SCHEMA_VERSION,
                                "edge_id": stable_id("bc_cfg", cu_id, src, handler, "exc", str(k)),
                                "code_unit_id": cu_id,
                                "src_block_id": src,
                                "dst_block_id": handler,
                                "kind": "exc",
                                "cond_instr_id": None,
                                "exc_index": int(k),
                            }
                        )

    return BytecodeExtractResult(
        py_bc_code_units=pa.Table.from_pylist(cu_rows, schema=CODE_UNITS_SCHEMA),
        py_bc_instructions=pa.Table.from_pylist(ins_rows, schema=INSTR_SCHEMA),
        py_bc_exception_table=pa.Table.from_pylist(exc_rows, schema=EXC_SCHEMA),
        py_bc_blocks=pa.Table.from_pylist(blk_rows, schema=BLOCKS_SCHEMA),
        py_bc_cfg_edges=pa.Table.from_pylist(edge_rows, schema=CFG_EDGES_SCHEMA),
        py_bc_errors=pa.Table.from_pylist(err_rows, schema=ERRORS_SCHEMA),
    )
