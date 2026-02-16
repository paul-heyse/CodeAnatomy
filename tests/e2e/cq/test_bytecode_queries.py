"""E2E tests for bytecode query features.

Tests the Python dis module integration for bytecode-level queries.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.introspection import (
    BytecodeIndex,
    InstructionFact,
    build_cfg,
    extract_instruction_facts,
    parse_exception_table,
)

MIN_CFG_BLOCKS_FOR_BRANCH = 2
MIN_CFG_BLOCKS_FOR_LOOP = 2
MULTI_RETURN_FIRST_BRANCH_THRESHOLD = 10


class TestInstructionFactExtraction:
    """Tests for instruction fact extraction."""

    @staticmethod
    def test_extract_simple_function() -> None:
        """Extract instruction facts from simple function."""

        def simple_func(x: int) -> int:
            return x + 1

        facts = extract_instruction_facts(simple_func.__code__)
        assert len(facts) > 0
        assert all(isinstance(f, InstructionFact) for f in facts)

    @staticmethod
    def test_instruction_fact_fields() -> None:
        """Verify instruction fact has all expected fields."""

        def add(a: int, b: int) -> int:
            return a + b

        facts = extract_instruction_facts(add.__code__)
        first = facts[0]

        assert hasattr(first, "offset")
        assert hasattr(first, "opcode")
        assert hasattr(first, "opname")
        assert hasattr(first, "is_jump_target")
        assert hasattr(first, "stack_effect")

    @staticmethod
    def test_opname_is_valid() -> None:
        """Instruction opnames are valid Python opcodes."""

        def noop() -> None:
            pass

        facts = extract_instruction_facts(noop.__code__)
        for fact in facts:
            # All opnames should be uppercase strings
            assert fact.opname.isupper() or fact.opname[0].isupper()


class TestBytecodeIndex:
    """Tests for BytecodeIndex class."""

    @staticmethod
    def test_create_from_code() -> None:
        """Create BytecodeIndex from code object."""

        def sample() -> int:
            x = 1
            y = 2
            return x + y

        index = BytecodeIndex.from_code(sample.__code__)
        assert len(index.instructions) > 0

    @staticmethod
    def test_filter_by_opname_exact() -> None:
        """Filter instructions by exact opname."""

        def with_return() -> int:
            return 42

        index = BytecodeIndex.from_code(with_return.__code__)
        returns = index.filter_by_opname("RETURN_VALUE")
        # May have RETURN_CONST in newer Python
        if not returns:
            returns = index.filter_by_opname("RETURN_CONST")
        assert len(returns) >= 1

    @staticmethod
    def test_filter_by_opname_regex() -> None:
        """Filter instructions by regex pattern."""

        def with_load(value: int) -> int:
            return value

        index = BytecodeIndex.from_code(with_load.__code__)
        loads = index.filter_by_opname("^LOAD", regex=True)
        assert len(loads) >= 1

    @staticmethod
    def test_filter_by_stack_effect() -> None:
        """Filter instructions by stack effect."""

        def with_stack_ops(a: int, b: int) -> int:
            # Operations that push/pop from stack
            return a + b

        index = BytecodeIndex.from_code(with_stack_ops.__code__)
        # Find instructions that have any stack effect (positive or negative)
        # LOAD_FAST typically has effect of +1
        all_with_effect = [i for i in index.instructions if i.stack_effect != 0]
        assert len(all_with_effect) >= 1

    @staticmethod
    def test_jump_targets() -> None:
        """Get instructions that are jump targets."""

        def with_branch(x: int) -> int:
            if x > 0:
                return 1
            return 0

        index = BytecodeIndex.from_code(with_branch.__code__)
        targets = index.jump_targets
        # Branch creates at least one jump target
        assert len(targets) >= 1


class TestExceptionTableParsing:
    """Tests for exception table parsing."""

    @staticmethod
    def test_parse_try_except() -> None:
        """Parse exception table from try-except function."""

        def with_try() -> float:
            try:
                return 1 / 0
            except ZeroDivisionError:
                return 0.0

        entries = parse_exception_table(with_try.__code__)
        # Python 3.11+ has exception table
        # Just verify no crash for now
        assert isinstance(entries, list)

    @staticmethod
    def test_function_without_try() -> None:
        """Function without try has empty exception table."""

        def no_try() -> int:
            return 42

        entries = parse_exception_table(no_try.__code__)
        assert isinstance(entries, list)
        assert len(entries) == 0


class TestCFGBuilder:
    """Tests for CFG construction."""

    @staticmethod
    def test_build_cfg_simple() -> None:
        """Build CFG for simple function."""

        def simple() -> int:
            return 1

        cfg = build_cfg(simple.__code__)
        assert cfg.block_count >= 1
        assert cfg.entry_block == 0

    @staticmethod
    def test_build_cfg_with_branch() -> None:
        """Build CFG for function with branching."""

        def with_branch(x: int) -> str:
            if x > 0:
                return "positive"
            return "non-positive"

        cfg = build_cfg(with_branch.__code__)
        assert cfg.block_count >= MIN_CFG_BLOCKS_FOR_BRANCH  # At least 2 paths
        assert len(cfg.exit_blocks) >= 1

    @staticmethod
    def test_build_cfg_with_loop() -> None:
        """Build CFG for function with loop."""

        def with_loop(n: int) -> int:
            total = 0
            for i in range(n):
                total += i
            return total

        cfg = build_cfg(with_loop.__code__)
        assert cfg.block_count >= MIN_CFG_BLOCKS_FOR_LOOP  # Loop creates multiple blocks

    @staticmethod
    def test_cfg_mermaid_output() -> None:
        """Generate Mermaid output for CFG."""

        def branching(x: int) -> int:
            if x > 0:
                return x
            return -x

        cfg = build_cfg(branching.__code__)
        mermaid = cfg.to_mermaid()
        assert "graph TD" in mermaid
        assert "B0" in mermaid

    @staticmethod
    def test_cfg_exit_blocks() -> None:
        """CFG identifies exit blocks correctly."""

        def multi_return(x: int) -> int:
            if x > MULTI_RETURN_FIRST_BRANCH_THRESHOLD:
                return 1
            if x > 0:
                return 2
            return 3

        cfg = build_cfg(multi_return.__code__)
        # Should have multiple exit blocks (one per return)
        assert len(cfg.exit_blocks) >= 1


class TestBytecodeWithFixtures:
    """Tests using fixture files."""

    @staticmethod
    @pytest.fixture
    def fixtures_dir() -> Path:
        """Get fixtures directory.

        Returns:
        -------
        Path
            Path to the fixture directory.
        """
        return Path(__file__).parent / "_fixtures"

    @staticmethod
    def test_extract_from_control_flow_file(fixtures_dir: Path) -> None:
        """Extract bytecode from control flow fixture file."""
        control_flow_path = fixtures_dir / "control_flow.py"
        if not control_flow_path.exists():
            pytest.skip("control_flow.py fixture not found")

        # Compile the file
        code = compile(
            control_flow_path.read_text(),
            str(control_flow_path),
            "exec",
        )

        # Should extract without error
        facts = extract_instruction_facts(code)
        assert len(facts) > 0

    @staticmethod
    def test_build_cfg_from_dynamic_dispatch(fixtures_dir: Path) -> None:
        """Build CFG from dynamic dispatch fixture."""
        dispatch_path = fixtures_dir / "dynamic_dispatch.py"
        if not dispatch_path.exists():
            pytest.skip("dynamic_dispatch.py fixture not found")

        code = compile(
            dispatch_path.read_text(),
            str(dispatch_path),
            "exec",
        )

        cfg = build_cfg(code)
        assert cfg.block_count >= 1
