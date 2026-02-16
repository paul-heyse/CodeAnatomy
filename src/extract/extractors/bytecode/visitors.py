"""Bytecode visitor context contracts."""

from __future__ import annotations

import dis
import types as pytypes
from collections.abc import Sequence
from dataclasses import dataclass

from extract.extractors.bytecode.setup import BytecodeFileContext


@dataclass(frozen=True)
class InstructionData:
    """Instruction data for a compiled code object."""

    instructions: list[dis.Instruction]
    index_by_offset: dict[int, int]


@dataclass(frozen=True)
class CodeUnitKey:
    """Key fields identifying a code unit."""

    qualpath: str
    co_name: str
    firstlineno: int


@dataclass(frozen=True)
class CodeUnitContext:
    """Per-code-unit extraction context."""

    code_unit_key: CodeUnitKey
    file_ctx: BytecodeFileContext
    code: pytypes.CodeType
    instruction_data: InstructionData
    exc_entries: Sequence[object]
    code_len: int


__all__ = ["CodeUnitContext", "CodeUnitKey", "InstructionData"]
