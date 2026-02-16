"""Direct coverage for extractor setup/visitor split modules."""

from __future__ import annotations

import dis

from extract.coordination.context import FileContext
from extract.extractors.ast.setup import AstExtractOptions, _format_feature_version
from extract.extractors.ast.visitors import _AstWalkAccumulator
from extract.extractors.bytecode.setup import BytecodeExtractOptions, BytecodeFileContext
from extract.extractors.bytecode.visitors import CodeUnitContext, CodeUnitKey, InstructionData
from extract.extractors.cst.setup import CstExtractOptions
from extract.extractors.cst.visitors import CSTExtractContext
from extract.extractors.tree_sitter.setup import TreeSitterExtractOptions
from extract.extractors.tree_sitter.visitors import _QueryStats


def _sample_file_context() -> FileContext:
    source = "x = 1\n"
    return FileContext(
        file_id="file-1",
        path="pkg/mod.py",
        abs_path=None,
        file_sha256=None,
        encoding="utf-8",
        text=source,
        data=source.encode("utf-8"),
    )


def test_ast_setup_and_visitors_split_contracts() -> None:
    """Verify AST split modules expose compatible setup/visitor contracts."""
    options = AstExtractOptions()
    assert options.mode == "exec"
    assert _format_feature_version((3, 13)) == "3.13"

    rows = _AstWalkAccumulator()
    rows.nodes.append({"kind": "Module"})
    result = rows.to_result()
    assert result.nodes == [{"kind": "Module"}]


def test_bytecode_setup_and_visitors_split_contracts() -> None:
    """Verify bytecode split modules expose compatible setup/visitor contracts."""
    file_ctx = _sample_file_context()
    bytecode_ctx = BytecodeFileContext(file_ctx=file_ctx, options=BytecodeExtractOptions())
    identity = bytecode_ctx.identity_row()
    assert identity["file_id"] == "file-1"

    code = compile("x = 1", "pkg/mod.py", "exec")
    instructions = list(dis.get_instructions(code, adaptive=False))
    instr_data = InstructionData(
        instructions=instructions,
        index_by_offset={ins.offset: idx for idx, ins in enumerate(instructions)},
    )
    unit_ctx = CodeUnitContext(
        code_unit_key=CodeUnitKey(
            qualpath="module", co_name=code.co_name, firstlineno=code.co_firstlineno
        ),
        file_ctx=bytecode_ctx,
        code=code,
        instruction_data=instr_data,
        exc_entries=(),
        code_len=len(code.co_code),
    )
    assert unit_ctx.code_unit_key.qualpath == "module"


def test_cst_setup_and_visitors_split_contracts() -> None:
    """Verify CST split modules expose compatible setup/visitor contracts."""
    options = CstExtractOptions()
    context = CSTExtractContext.build(options)
    assert context.options is options
    assert context.manifest_rows == []
    assert context.error_rows == []


def test_tree_sitter_setup_and_visitors_split_contracts() -> None:
    """Verify tree-sitter split modules expose setup/visitor contracts."""
    options = TreeSitterExtractOptions()
    assert options.include_nodes is True
    stats = _QueryStats()
    assert stats.match_count == 0
    assert stats.capture_count == 0
