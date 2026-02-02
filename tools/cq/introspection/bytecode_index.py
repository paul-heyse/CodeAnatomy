"""Bytecode instruction indexing and analysis.

Provides InstructionFact extraction from Python code objects for
bytecode-level queries and analysis.
"""

from __future__ import annotations

import dis
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import CodeType


@dataclass(frozen=True)
class InstructionFact:
    """Single bytecode instruction fact.

    Attributes
    ----------
    offset
        Byte offset of instruction
    opcode
        Numeric opcode
    opname
        Instruction name (e.g., LOAD_GLOBAL)
    baseopname
        Base instruction name for specialized opcodes
    arg
        Instruction argument (raw)
    argval
        Resolved argument value
    argrepr
        Human-readable argument representation
    is_jump_target
        Whether this instruction is a jump target
    positions
        Source position info (line, col, end_line, end_col)
    stack_effect
        Stack effect of instruction (push - pop)
    """

    offset: int
    opcode: int
    opname: str
    baseopname: str
    arg: int | None
    argval: object
    argrepr: str
    is_jump_target: bool
    positions: tuple[int | None, int | None, int | None, int | None]
    stack_effect: int


def extract_instruction_facts(code: CodeType) -> list[InstructionFact]:
    """Extract instruction facts from a code object.

    Parameters
    ----------
    code
        Python code object to analyze.

    Returns
    -------
    list[InstructionFact]
        List of instruction facts for all bytecode instructions.
    """
    facts: list[InstructionFact] = []

    for instr in dis.get_instructions(code, show_caches=False):
        # Get stack effect - may raise ValueError for some opcodes
        try:
            effect = dis.stack_effect(instr.opcode, instr.arg)
        except ValueError:
            effect = 0

        # Get base opname (same as opname for non-specialized)
        baseopname = instr.opname
        if hasattr(dis, "_specialized_opnames"):
            # Python 3.13+ has specialized opcodes
            specialized = getattr(dis, "_specialized_opnames", {})
            if instr.opname in specialized:
                baseopname = specialized[instr.opname]

        # Build positions tuple
        positions = (
            instr.positions.lineno if instr.positions else None,
            instr.positions.col_offset if instr.positions else None,
            instr.positions.end_lineno if instr.positions else None,
            instr.positions.end_col_offset if instr.positions else None,
        )

        facts.append(
            InstructionFact(
                offset=instr.offset,
                opcode=instr.opcode,
                opname=instr.opname,
                baseopname=baseopname,
                arg=instr.arg,
                argval=instr.argval,
                argrepr=instr.argrepr,
                is_jump_target=instr.is_jump_target,
                positions=positions,
                stack_effect=effect,
            )
        )

    return facts


@dataclass
class BytecodeIndex:
    """Index of bytecode instructions for efficient queries.

    Supports multiple lookup strategies for finding instructions
    by various attributes.
    """

    instructions: list[InstructionFact]
    _by_opname: dict[str, list[InstructionFact]] | None = None
    _by_offset: dict[int, InstructionFact] | None = None
    _jump_targets: list[InstructionFact] | None = None

    @classmethod
    def from_code(cls, code: CodeType) -> BytecodeIndex:
        """Create index from code object.

        Parameters
        ----------
        code
            Python code object.

        Returns
        -------
        BytecodeIndex
            Indexed bytecode instructions.
        """
        instructions = extract_instruction_facts(code)
        return cls(instructions=instructions)

    @property
    def by_opname(self) -> dict[str, list[InstructionFact]]:
        """Get instructions grouped by opname."""
        if self._by_opname is None:
            self._by_opname = {}
            for instr in self.instructions:
                if instr.opname not in self._by_opname:
                    self._by_opname[instr.opname] = []
                self._by_opname[instr.opname].append(instr)
        return self._by_opname

    @property
    def by_offset(self) -> dict[int, InstructionFact]:
        """Get instruction by offset."""
        if self._by_offset is None:
            self._by_offset = {i.offset: i for i in self.instructions}
        return self._by_offset

    @property
    def jump_targets(self) -> list[InstructionFact]:
        """Get instructions that are jump targets."""
        if self._jump_targets is None:
            self._jump_targets = [i for i in self.instructions if i.is_jump_target]
        return self._jump_targets

    def filter_by_opname(self, pattern: str, *, regex: bool = False) -> list[InstructionFact]:
        """Filter instructions by opname.

        Parameters
        ----------
        pattern
            Opname or regex pattern.
        regex
            If True, treat pattern as regex.

        Returns
        -------
        list[InstructionFact]
            Matching instructions.
        """
        if regex:
            import re

            compiled = re.compile(pattern)
            return [i for i in self.instructions if compiled.search(i.opname)]
        return self.by_opname.get(pattern, [])

    def filter_by_stack_effect(
        self,
        *,
        min_effect: int | None = None,
        max_effect: int | None = None,
    ) -> list[InstructionFact]:
        """Filter instructions by stack effect.

        Parameters
        ----------
        min_effect
            Minimum stack effect (inclusive).
        max_effect
            Maximum stack effect (inclusive).

        Returns
        -------
        list[InstructionFact]
            Matching instructions.
        """
        results = []
        for instr in self.instructions:
            if min_effect is not None and instr.stack_effect < min_effect:
                continue
            if max_effect is not None and instr.stack_effect > max_effect:
                continue
            results.append(instr)
        return results


@dataclass(frozen=True)
class ExceptionEntry:
    """Exception table entry for a code object.

    Attributes
    ----------
    start
        Start offset of protected range
    end
        End offset of protected range
    target
        Handler target offset
    depth
        Stack depth at handler entry
    lasti
        Whether to push last instruction offset
    """

    start: int
    end: int
    target: int
    depth: int
    lasti: bool


def parse_exception_table(code: CodeType) -> list[ExceptionEntry]:
    """Parse exception table from code object.

    Parameters
    ----------
    code
        Python code object.

    Returns
    -------
    list[ExceptionEntry]
        Exception table entries.

    Notes
    -----
    Uses dis.findlinestarts internals and exception table parsing
    available in Python 3.11+.
    """
    entries: list[ExceptionEntry] = []

    # Python 3.11+ has co_exceptiontable
    if hasattr(code, "co_exceptiontable") and code.co_exceptiontable:
        # Use dis module to parse exception table if available
        try:
            # Python 3.13+ has _parse_exception_table
            if hasattr(dis, "_parse_exception_table"):
                # Access internal parser
                parse_func = dis._parse_exception_table
                for entry in parse_func(code):
                    entries.append(
                        ExceptionEntry(
                            start=entry.start,
                            end=entry.end,
                            target=entry.target,
                            depth=entry.depth,
                            lasti=entry.lasti,
                        )
                    )
        except (AttributeError, TypeError):
            pass

    return entries
