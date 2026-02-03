"""Unit tests for bytecode instruction byte spans."""

from __future__ import annotations

from collections.abc import Iterable

from extract.coordination.context import FileContext
from extract.extractors.bytecode_extract import BytecodeExtractOptions, _bytecode_file_row


def _line_start_bytes(data: bytes) -> list[int]:
    starts = [0]
    for idx, value in enumerate(data):
        if value == 0x0A:
            starts.append(idx + 1)
    return starts


def _byte_offset(data: bytes, line0: int, col: int) -> int:
    starts = _line_start_bytes(data)
    return starts[line0] + col


def _as_mapping(value: object) -> dict[str, object]:
    assert isinstance(value, dict)
    return value


def _require_int(value: object) -> int:
    assert isinstance(value, int)
    return value


def _iter_instructions(row: dict[str, object]) -> list[dict[str, object]]:
    code_objects = row.get("code_objects")
    if not isinstance(code_objects, Iterable) or isinstance(code_objects, (str, bytes)):
        return []
    instructions: list[dict[str, object]] = []
    for code_obj in code_objects:
        if not isinstance(code_obj, dict):
            continue
        instrs = code_obj.get("instructions")
        if not isinstance(instrs, Iterable) or isinstance(instrs, (str, bytes)):
            continue
        instructions.extend(instr for instr in instrs if isinstance(instr, dict))
    return instructions


def test_bytecode_instruction_byte_span() -> None:
    code = "def foo():\n    x = 1\n    return x\n"
    data = code.encode("utf-8")
    file_ctx = FileContext(
        file_id="file-1",
        path="foo.py",
        abs_path=None,
        file_sha256=None,
        encoding="utf-8",
        text=code,
        data=data,
    )
    options = BytecodeExtractOptions()

    row = _bytecode_file_row(file_ctx, options=options, cache=None, cache_ttl=None)

    assert row is not None
    row_map = _as_mapping(row)
    instructions = _iter_instructions(row_map)
    assert instructions

    target: dict[str, object] | None = None
    for instr in instructions:
        span = instr.get("span")
        if not isinstance(span, dict):
            continue
        byte_span = span.get("byte_span")
        start = span.get("start")
        if not isinstance(byte_span, dict) or not isinstance(start, dict):
            continue
        if start.get("line0") is None or start.get("col") is None:
            continue
        if byte_span.get("byte_start") is None:
            continue
        target = instr
        break

    assert target is not None
    span = _as_mapping(target["span"])
    assert span["col_unit"] == "byte"
    byte_span = _as_mapping(span["byte_span"])
    start = _as_mapping(span["start"])

    expected_start = _byte_offset(
        data,
        _require_int(start.get("line0")),
        _require_int(start.get("col")),
    )
    assert byte_span["byte_start"] == expected_start

    end = span.get("end")
    if isinstance(end, dict) and end.get("line0") is not None and end.get("col") is not None:
        expected_end = _byte_offset(
            data,
            _require_int(end.get("line0")),
            _require_int(end.get("col")),
        )
        assert byte_span["byte_len"] == max(0, expected_end - expected_start)
