"""Control Flow Graph (CFG) construction from bytecode.

Provides basic block identification and CFG edge construction
for Python code objects.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from types import CodeType

CfgEdgeType = Literal["fallthrough", "jump", "exception"]

from tools.cq.introspection.bytecode_index import (
    BytecodeIndex,
    InstructionFact,
    parse_exception_table,
)


@dataclass
class BasicBlock:
    """A basic block in the control flow graph.

    Attributes:
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

    Attributes:
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
    edge_type: CfgEdgeType


@dataclass
class CFG:
    """Control Flow Graph for a code object.

    Attributes:
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
        """Get number of basic blocks.

        Returns:
        -------
        int
            Count of basic blocks.
        """
        return len(self.blocks)

    @property
    def edge_count(self) -> int:
        """Get number of edges.

        Returns:
        -------
        int
            Count of edges.
        """
        return len(self.edges)

    def get_block_at_offset(self, offset: int) -> BasicBlock | None:
        """Get block containing instruction at offset.

        Returns:
        -------
        BasicBlock | None
            Matching block if found.
        """
        for block in self.blocks.values():
            if block.start_offset <= offset <= block.end_offset:
                return block
        return None

    def to_mermaid(self) -> str:
        """Generate Mermaid flowchart representation.

        Returns:
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

    Returns:
    -------
    set[int]
        Offsets that begin basic blocks.
    """
    starts: set[int] = {0}

    instructions = index.instructions
    for i, instr in enumerate(instructions):
        # Jump target is a block start
        if instr.is_jump_target:
            starts.add(instr.offset)

        # Instruction after jump/exit is block start
        if instr.opname in JUMP_OPCODES | EXIT_OPCODES and i + 1 < len(instructions):
            starts.add(instructions[i + 1].offset)

        # Jump target offset is block start
        if instr.opname in JUMP_OPCODES and isinstance(instr.argval, int):
            starts.add(instr.argval)

    return starts


def _build_empty_cfg() -> CFG:
    block = BasicBlock(id=0, start_offset=0, end_offset=0, is_entry=True, is_exit=True)
    return CFG(blocks={0: block}, edges=[], entry_block=0, exit_blocks=[0])


def _find_block_end(
    instructions: list[InstructionFact],
    sorted_starts: list[int],
    block_id: int,
    start_offset: int,
) -> int:
    end_offset = start_offset
    next_start = sorted_starts[block_id + 1] if block_id + 1 < len(sorted_starts) else None
    for instr in instructions:
        if instr.offset < start_offset:
            continue
        if next_start is not None and instr.offset >= next_start:
            break
        end_offset = instr.offset
    return end_offset


def _collect_block_instructions(
    instructions: list[InstructionFact],
    start_offset: int,
    end_offset: int,
    block_id: int,
    offset_to_block: dict[int, int],
) -> list[InstructionFact]:
    block_instructions: list[InstructionFact] = []
    for instr in instructions:
        if start_offset <= instr.offset <= end_offset:
            block_instructions.append(instr)
            offset_to_block[instr.offset] = block_id
    return block_instructions


def _build_blocks(
    instructions: list[InstructionFact],
    sorted_starts: list[int],
) -> tuple[dict[int, BasicBlock], dict[int, int]]:
    blocks: dict[int, BasicBlock] = {}
    offset_to_block: dict[int, int] = {}
    for block_id, start_offset in enumerate(sorted_starts):
        end_offset = _find_block_end(instructions, sorted_starts, block_id, start_offset)
        block = BasicBlock(
            id=block_id,
            start_offset=start_offset,
            end_offset=end_offset,
            is_entry=(start_offset == 0),
        )
        block.instructions.extend(
            _collect_block_instructions(
                instructions,
                start_offset,
                end_offset,
                block_id,
                offset_to_block,
            )
        )
        blocks[block_id] = block
    return blocks, offset_to_block


def _add_fallthrough_edge(
    block_id: int,
    block: BasicBlock,
    blocks: dict[int, BasicBlock],
    edges: list[CFGEdge],
) -> None:
    last_instr = block.instructions[-1]
    if last_instr.opname in UNCONDITIONAL_JUMP_OPCODES:
        return
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


def _add_jump_edge(
    block_id: int,
    block: BasicBlock,
    offset_to_block: dict[int, int],
    blocks: dict[int, BasicBlock],
    edges: list[CFGEdge],
) -> None:
    last_instr = block.instructions[-1]
    if last_instr.opname in JUMP_OPCODES and isinstance(last_instr.argval, int):
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


def _build_edges(
    blocks: dict[int, BasicBlock],
    offset_to_block: dict[int, int],
) -> tuple[list[CFGEdge], list[int]]:
    edges: list[CFGEdge] = []
    exit_blocks: list[int] = []
    for block_id, block in blocks.items():
        if not block.instructions:
            continue
        last_instr = block.instructions[-1]
        if last_instr.opname in EXIT_OPCODES:
            block.is_exit = True
            exit_blocks.append(block_id)
            continue
        _add_fallthrough_edge(block_id, block, blocks, edges)
        _add_jump_edge(block_id, block, offset_to_block, blocks, edges)
    return edges, exit_blocks


def _add_exception_edges(
    code: CodeType,
    blocks: dict[int, BasicBlock],
    offset_to_block: dict[int, int],
    edges: list[CFGEdge],
) -> None:
    exc_table = parse_exception_table(code)
    for entry in exc_table:
        target_block_id = offset_to_block.get(entry.target)
        if target_block_id is None:
            continue
        for block_id, block in blocks.items():
            if entry.start <= block.start_offset < entry.end:
                edges.append(
                    CFGEdge(
                        source=block_id,
                        target=target_block_id,
                        edge_type="exception",
                    )
                )


def build_cfg(code: CodeType) -> CFG:
    """Build control flow graph from code object.

    Parameters
    ----------
    code
        Python code object.

    Returns:
    -------
    CFG
        Control flow graph.
    """
    index = BytecodeIndex.from_code(code)
    instructions = index.instructions

    if not instructions:
        return _build_empty_cfg()

    block_starts = _find_block_starts(index)
    sorted_starts = sorted(block_starts)
    blocks, offset_to_block = _build_blocks(instructions, sorted_starts)
    edges, exit_blocks = _build_edges(blocks, offset_to_block)
    _add_exception_edges(code, blocks, offset_to_block, edges)

    return CFG(blocks=blocks, edges=edges, entry_block=0, exit_blocks=exit_blocks)
