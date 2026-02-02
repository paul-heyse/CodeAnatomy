"""Control Flow Graph (CFG) construction from bytecode.

Provides basic block identification and CFG edge construction
for Python code objects.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import CodeType

from tools.cq.introspection.bytecode_index import (
    BytecodeIndex,
    InstructionFact,
    parse_exception_table,
)


@dataclass
class BasicBlock:
    """A basic block in the control flow graph.

    Attributes
    ----------
    id
        Unique block identifier
    start_offset
        Byte offset of first instruction
    end_offset
        Byte offset of last instruction
    instructions
        Instructions in this block
    predecessors
        IDs of predecessor blocks
    successors
        IDs of successor blocks
    is_entry
        Whether this is the entry block
    is_exit
        Whether this block exits (return/raise)
    """

    id: int
    start_offset: int
    end_offset: int
    instructions: list[InstructionFact] = field(default_factory=list)
    predecessors: list[int] = field(default_factory=list)
    successors: list[int] = field(default_factory=list)
    is_entry: bool = False
    is_exit: bool = False


@dataclass
class CFGEdge:
    """Edge in the control flow graph.

    Attributes
    ----------
    source
        Source block ID
    target
        Target block ID
    edge_type
        Type of edge (fallthrough, jump, exception)
    """

    source: int
    target: int
    edge_type: str  # "fallthrough", "jump", "exception"


@dataclass
class CFG:
    """Control Flow Graph for a code object.

    Attributes
    ----------
    blocks
        Basic blocks indexed by ID
    edges
        CFG edges
    entry_block
        ID of entry block
    exit_blocks
        IDs of exit blocks
    """

    blocks: dict[int, BasicBlock]
    edges: list[CFGEdge]
    entry_block: int
    exit_blocks: list[int]

    @property
    def block_count(self) -> int:
        """Get number of basic blocks."""
        return len(self.blocks)

    @property
    def edge_count(self) -> int:
        """Get number of edges."""
        return len(self.edges)

    def get_block_at_offset(self, offset: int) -> BasicBlock | None:
        """Get block containing instruction at offset."""
        for block in self.blocks.values():
            if block.start_offset <= offset <= block.end_offset:
                return block
        return None

    def to_mermaid(self) -> str:
        """Generate Mermaid flowchart representation.

        Returns
        -------
        str
            Mermaid flowchart diagram code.
        """
        lines = ["graph TD"]

        # Define blocks
        for block_id, block in sorted(self.blocks.items()):
            label_parts = [f"B{block_id}"]
            if block.is_entry:
                label_parts.append("entry")
            if block.is_exit:
                label_parts.append("exit")
            label_parts.append(f"off={block.start_offset}")

            label = " ".join(label_parts)
            shape = "((" + label + "))" if block.is_entry or block.is_exit else "[" + label + "]"
            lines.append(f"    B{block_id}{shape}")

        # Define edges
        for edge in self.edges:
            style = ""
            if edge.edge_type == "exception":
                style = " -.-> "
            elif edge.edge_type == "jump":
                style = " --> "
            else:
                style = " --> "
            lines.append(f"    B{edge.source}{style}B{edge.target}")

        return "\n".join(lines)


# Jump instruction opcodes
JUMP_OPCODES = frozenset(
    {
        "JUMP_FORWARD",
        "JUMP_BACKWARD",
        "JUMP_ABSOLUTE",
        "JUMP_IF_TRUE_OR_POP",
        "JUMP_IF_FALSE_OR_POP",
        "POP_JUMP_IF_TRUE",
        "POP_JUMP_IF_FALSE",
        "POP_JUMP_IF_NONE",
        "POP_JUMP_IF_NOT_NONE",
        "FOR_ITER",
        "SEND",
        # Python 3.12+
        "JUMP_IF_TRUE",
        "JUMP_IF_FALSE",
    }
)

# Unconditional jump opcodes (no fallthrough)
UNCONDITIONAL_JUMP_OPCODES = frozenset(
    {
        "JUMP_FORWARD",
        "JUMP_BACKWARD",
        "JUMP_ABSOLUTE",
    }
)

# Exit opcodes (terminate execution path)
EXIT_OPCODES = frozenset(
    {
        "RETURN_VALUE",
        "RETURN_CONST",
        "RAISE_VARARGS",
        "RERAISE",
    }
)


def _find_block_starts(index: BytecodeIndex) -> set[int]:
    """Find offsets where basic blocks start.

    Block starts occur at:
    - Offset 0 (entry)
    - Jump targets
    - Instructions following jumps/returns
    """
    starts: set[int] = {0}

    instructions = index.instructions
    for i, instr in enumerate(instructions):
        # Jump target is a block start
        if instr.is_jump_target:
            starts.add(instr.offset)

        # Instruction after jump/exit is block start
        if instr.opname in JUMP_OPCODES | EXIT_OPCODES:
            if i + 1 < len(instructions):
                starts.add(instructions[i + 1].offset)

        # Jump target offset is block start
        if instr.opname in JUMP_OPCODES:
            if isinstance(instr.argval, int):
                starts.add(instr.argval)

    return starts


def build_cfg(code: CodeType) -> CFG:
    """Build control flow graph from code object.

    Parameters
    ----------
    code
        Python code object.

    Returns
    -------
    CFG
        Control flow graph.
    """
    index = BytecodeIndex.from_code(code)
    instructions = index.instructions

    if not instructions:
        # Empty code - single empty block
        block = BasicBlock(id=0, start_offset=0, end_offset=0, is_entry=True, is_exit=True)
        return CFG(blocks={0: block}, edges=[], entry_block=0, exit_blocks=[0])

    # Find block boundaries
    block_starts = _find_block_starts(index)
    sorted_starts = sorted(block_starts)

    # Create blocks
    blocks: dict[int, BasicBlock] = {}
    offset_to_block: dict[int, int] = {}

    for block_id, start_offset in enumerate(sorted_starts):
        # Find end offset (last instruction before next block or end)
        end_offset = start_offset
        for instr in instructions:
            if instr.offset >= start_offset:
                if block_id + 1 < len(sorted_starts) and instr.offset >= sorted_starts[block_id + 1]:
                    break
                end_offset = instr.offset

        block = BasicBlock(
            id=block_id,
            start_offset=start_offset,
            end_offset=end_offset,
            is_entry=(start_offset == 0),
        )

        # Collect instructions for this block
        for instr in instructions:
            if start_offset <= instr.offset <= end_offset:
                block.instructions.append(instr)
                offset_to_block[instr.offset] = block_id

        blocks[block_id] = block

    # Build edges
    edges: list[CFGEdge] = []
    exit_blocks: list[int] = []

    for block_id, block in blocks.items():
        if not block.instructions:
            continue

        last_instr = block.instructions[-1]

        # Check for exit
        if last_instr.opname in EXIT_OPCODES:
            block.is_exit = True
            exit_blocks.append(block_id)
            continue

        # Add fallthrough edge (if not unconditional jump)
        if last_instr.opname not in UNCONDITIONAL_JUMP_OPCODES:
            next_block_id = block_id + 1
            if next_block_id in blocks:
                edges.append(
                    CFGEdge(
                        source=block_id,
                        target=next_block_id,
                        edge_type="fallthrough",
                    )
                )
                block.successors.append(next_block_id)
                blocks[next_block_id].predecessors.append(block_id)

        # Add jump edge
        if last_instr.opname in JUMP_OPCODES:
            if isinstance(last_instr.argval, int):
                target_offset = last_instr.argval
                if target_offset in offset_to_block:
                    target_block_id = offset_to_block[target_offset]
                    edges.append(
                        CFGEdge(
                            source=block_id,
                            target=target_block_id,
                            edge_type="jump",
                        )
                    )
                    if target_block_id not in block.successors:
                        block.successors.append(target_block_id)
                    if block_id not in blocks[target_block_id].predecessors:
                        blocks[target_block_id].predecessors.append(block_id)

    # Add exception edges
    exc_table = parse_exception_table(code)
    for entry in exc_table:
        if entry.target in offset_to_block:
            target_block_id = offset_to_block[entry.target]
            # Find blocks in protected range
            for block_id, block in blocks.items():
                if entry.start <= block.start_offset < entry.end:
                    edges.append(
                        CFGEdge(
                            source=block_id,
                            target=target_block_id,
                            edge_type="exception",
                        )
                    )

    return CFG(
        blocks=blocks,
        edges=edges,
        entry_block=0,
        exit_blocks=exit_blocks,
    )
