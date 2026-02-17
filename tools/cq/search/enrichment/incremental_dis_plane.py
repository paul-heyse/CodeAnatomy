"""Incremental bytecode enrichment plane."""

from __future__ import annotations

from types import CodeType
from typing import Final

from tools.cq.introspection.bytecode_index import (
    InstructionFact,
    extract_instruction_facts,
    parse_exception_table,
)
from tools.cq.introspection.cfg_builder import build_cfg

_DEF_OP_PREFIXES: tuple[str, ...] = (
    "STORE_",
    "DELETE_",
)
_USE_OP_PREFIXES: tuple[str, ...] = ("LOAD_",)
_MAX_ARGVAL_DEPTH: Final[int] = 3


def _coerce_count(value: object) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0
    return 0


def _iter_code_objects(root: CodeType) -> list[CodeType]:
    found: list[CodeType] = [root]
    stack: list[CodeType] = [root]
    while stack:
        current = stack.pop()
        for item in current.co_consts:
            if not isinstance(item, CodeType):
                continue
            found.append(item)
            stack.append(item)
    return found


def _positions_payload(instr: InstructionFact) -> dict[str, int | None]:
    line, col, end_line, end_col = instr.positions
    return {
        "line": line,
        "col": col,
        "end_line": end_line,
        "end_col": end_col,
    }


def _binding_name_from_instruction(instr: InstructionFact) -> str | None:
    argval = instr.argval
    if isinstance(argval, str) and argval:
        return argval
    return None


def _normalize_instruction_argval(value: object, *, _depth: int = 0) -> object:
    if _depth >= _MAX_ARGVAL_DEPTH:
        return {"kind": "truncated", "type": type(value).__name__}

    normalized: object
    if value is None or isinstance(value, (bool, int, float, str)):
        normalized = value
    elif isinstance(value, CodeType):
        normalized = {
            "kind": "code",
            "name": value.co_name,
            "qualname": getattr(value, "co_qualname", value.co_name),
            "argcount": value.co_argcount,
            "kwonlyargcount": value.co_kwonlyargcount,
            "firstlineno": value.co_firstlineno,
        }
    elif isinstance(value, (bytes, bytearray, memoryview)):
        normalized = {
            "kind": type(value).__name__,
            "len": len(value),
        }
    elif isinstance(value, complex):
        normalized = str(value)
    elif isinstance(value, (list, tuple)):
        normalized = [_normalize_instruction_argval(item, _depth=_depth + 1) for item in value]
    elif isinstance(value, dict):
        normalized = {
            str(key): _normalize_instruction_argval(item, _depth=_depth + 1)
            for key, item in value.items()
        }
    elif isinstance(value, (set, frozenset)):
        normalized_items = [
            _normalize_instruction_argval(item, _depth=_depth + 1) for item in value
        ]
        normalized = sorted(normalized_items, key=repr)
    else:
        normalized = repr(value)
    return normalized


def _event_kind(opname: str) -> str | None:
    if opname.startswith(_DEF_OP_PREFIXES):
        return "def"
    if opname.startswith(_USE_OP_PREFIXES):
        return "use"
    return None


def _binding_id(qualname: str, binding_name: str | None) -> str | None:
    if not binding_name:
        return None
    return f"{qualname}:{binding_name}"


def _defuse_events(
    *,
    qualname: str,
    instructions: list[InstructionFact],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for instr in instructions:
        event = _event_kind(instr.baseopname or instr.opname)
        if event is None:
            continue
        binding_name = _binding_name_from_instruction(instr)
        rows.append(
            {
                "code_qualname": qualname,
                "offset": instr.offset,
                "event": event,
                "opname": instr.opname,
                "baseopname": instr.baseopname,
                "binding_name": binding_name,
                "binding_id": _binding_id(qualname, binding_name),
                "positions": _positions_payload(instr),
            }
        )
    return rows


def _reaching_def_edges(defuse_events: list[dict[str, object]]) -> list[dict[str, object]]:
    last_defs: dict[str, dict[str, object]] = {}
    edges: list[dict[str, object]] = []
    for event in defuse_events:
        binding_id = event.get("binding_id")
        if not isinstance(binding_id, str) or not binding_id:
            continue
        if event.get("event") == "def":
            last_defs[binding_id] = event
            continue
        if event.get("event") != "use":
            continue
        source = last_defs.get(binding_id)
        if source is None:
            continue
        edges.append(
            {
                "binding_id": binding_id,
                "source_offset": source.get("offset"),
                "target_offset": event.get("offset"),
                "source_qualname": source.get("code_qualname"),
                "target_qualname": event.get("code_qualname"),
            }
        )
    return edges


def build_dis_bundle(code: CodeType, *, anchor_name: str | None = None) -> dict[str, object]:
    """Build full incremental bytecode-plane bundle from a code object tree.

    Returns:
        dict[str, object]: Bytecode-plane payload including CFG, def/use, and metrics.
    """
    instruction_facts_rows: list[dict[str, object]] = []
    cfg_rows: list[dict[str, object]] = []
    exception_rows: list[dict[str, object]] = []
    defuse_rows: list[dict[str, object]] = []

    code_objects = _iter_code_objects(code)
    for current in code_objects:
        qualname = str(current.co_name or "<module>")
        instructions = extract_instruction_facts(current)
        cfg = build_cfg(current)
        exceptions = parse_exception_table(current)
        defuse = _defuse_events(qualname=qualname, instructions=instructions)
        defuse_rows.extend(defuse)

        instruction_facts_rows.extend(
            {
                "code_qualname": qualname,
                "offset": instr.offset,
                "opcode": instr.opcode,
                "opname": instr.opname,
                "baseopname": instr.baseopname,
                "arg": instr.arg,
                "argval": _normalize_instruction_argval(instr.argval),
                "argrepr": instr.argrepr,
                "is_jump_target": instr.is_jump_target,
                "positions": _positions_payload(instr),
                "stack_effect": instr.stack_effect,
                "binding_name": _binding_name_from_instruction(instr),
            }
            for instr in instructions
        )
        cfg_rows.append(
            {
                "code_qualname": qualname,
                "blocks_n": cfg.block_count,
                "edges_n": cfg.edge_count,
                "entry_block": cfg.entry_block,
                "exit_blocks": list(cfg.exit_blocks),
                "exception_edges_n": sum(1 for edge in cfg.edges if edge.edge_type == "exception"),
            }
        )
        exception_rows.extend(
            {
                "code_qualname": qualname,
                "start": entry.start,
                "end": entry.end,
                "target": entry.target,
                "depth": entry.depth,
                "lasti": entry.lasti,
            }
            for entry in exceptions
        )

    reaching_def_dfg = _reaching_def_edges(defuse_rows)
    anchor_defs = 0
    anchor_uses = 0
    if isinstance(anchor_name, str) and anchor_name:
        for row in defuse_rows:
            if row.get("binding_name") != anchor_name:
                continue
            if row.get("event") == "def":
                anchor_defs += 1
            elif row.get("event") == "use":
                anchor_uses += 1

    return {
        "instruction_facts": instruction_facts_rows,
        "cfg": {
            "functions": cfg_rows,
            "functions_count": len(cfg_rows),
            "blocks_n": sum(_coerce_count(row.get("blocks_n")) for row in cfg_rows),
            "edges_n": sum(_coerce_count(row.get("edges_n")) for row in cfg_rows),
            "exc_edges_n": sum(_coerce_count(row.get("exception_edges_n")) for row in cfg_rows),
        },
        "exception_regions": exception_rows,
        "defuse_events": defuse_rows,
        "defuse_dfg": reaching_def_dfg,
        "anchor_metrics": {
            "anchor_symbol": anchor_name,
            "anchor_defs": anchor_defs,
            "anchor_uses": anchor_uses,
            "instruction_count": len(instruction_facts_rows),
            "defuse_events_count": len(defuse_rows),
            "codes_count": len(code_objects),
        },
    }


__all__ = ["build_dis_bundle"]
