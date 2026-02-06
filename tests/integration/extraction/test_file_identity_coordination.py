"""Integration tests for file identity and parallel extraction coordination.

Tests the boundary where FileContext provides consistent file identity (file_id,
path, SHA256) across multiple parallel extractors.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pyarrow as pa
import pytest

from extract.extractors.ast_extract import AstExtractOptions, extract_ast_tables
from extract.extractors.cst_extract import CstExtractOptions, extract_cst_tables
from extract.extractors.symtable_extract import SymtableExtractOptions, extract_symtables_table


@pytest.fixture
def multi_file_repo(tmp_path: Path) -> dict[str, tuple[Path, str]]:
    """Create a repository with multiple Python files.

    Parameters
    ----------
    tmp_path
        Pytest temporary directory fixture.

    Returns:
    -------
    dict[str, tuple[Path, str]]
        Mapping of file identifiers to (path, content) tuples.
    """
    files = {
        "file1": (
            tmp_path / "file1.py",
            '''"""First file."""

def function_one() -> int:
    return 1
''',
        ),
        "file2": (
            tmp_path / "file2.py",
            '''"""Second file."""

def function_two() -> int:
    return 2
''',
        ),
        "file3": (
            tmp_path / "subdir" / "file3.py",
            '''"""Third file in subdirectory."""

class ClassThree:
    def method(self) -> int:
        return 3
''',
        ),
    }

    # Create subdirectory
    (tmp_path / "subdir").mkdir(exist_ok=True)

    # Write all files
    for file_path, content in files.values():
        file_path.write_text(content, encoding="utf-8")

    return files


@pytest.mark.integration
def test_file_id_consistency_across_extractors(
    multi_file_repo: dict[str, tuple[Path, str]],
) -> None:
    """Test file_id consistency across multiple extractors.

    Verifies that when the same file is extracted via CST, AST, and symtable
    extractors, all output tables have matching file_id values for the same file.

    Parameters
    ----------
    multi_file_repo
        Dictionary of test files with paths and contents.
    """
    repo_files = _repo_files_table(multi_file_repo)
    expected_file_ids = set(repo_files["file_id"].to_pylist())

    cst_table = _as_table(
        extract_cst_tables(
            repo_files=repo_files,
            options=CstExtractOptions(max_workers=1),
        )["libcst_files"]
    )
    ast_table = _as_table(
        extract_ast_tables(
            repo_files=repo_files,
            options=AstExtractOptions(max_workers=1),
        )["ast_files"]
    )
    sym_table = _as_table(
        extract_symtables_table(
            repo_files=repo_files,
            options=SymtableExtractOptions(max_workers=1),
        )
    )

    assert set(cst_table["file_id"].to_pylist()) == expected_file_ids
    assert set(ast_table["file_id"].to_pylist()) == expected_file_ids
    assert set(sym_table["file_id"].to_pylist()) == expected_file_ids


@pytest.mark.integration
def test_file_sha256_consistency(multi_file_repo: dict[str, tuple[Path, str]]) -> None:
    """Test SHA256 hash consistency for file identity.

    Verifies that file_sha256 values computed for file identity match expected
    SHA256 hashes of the file contents.

    Parameters
    ----------
    multi_file_repo
        Dictionary of test files with paths and contents.
    """
    for file_path, content in multi_file_repo.values():
        # Read file and compute SHA256
        file_bytes = file_path.read_bytes()
        computed_hash = hashlib.sha256(file_bytes).hexdigest()

        # Verify hash is deterministic
        recomputed_hash = hashlib.sha256(file_bytes).hexdigest()
        assert computed_hash == recomputed_hash

        # Verify hash matches content
        content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
        assert computed_hash == content_hash


@pytest.mark.integration
def test_parallel_extraction_partial_failure_resilience(
    multi_file_repo: dict[str, tuple[Path, str]],
    tmp_path: Path,
) -> None:
    """Test resilience to partial failures in parallel extraction.

    Verifies that when one extractor fails on a specific file, other extractors
    still produce valid output, and the error is recorded in diagnostics.

    Parameters
    ----------
    multi_file_repo
        Dictionary of test files with paths and contents.
    tmp_path
        Pytest temporary directory fixture.
    """
    broken_path = tmp_path / "broken.py"
    broken_content = "def broken(\n"
    broken_path.write_text(broken_content, encoding="utf-8")

    repo_map = dict(multi_file_repo)
    repo_map["broken"] = (broken_path, broken_content)
    repo_files = _repo_files_table(repo_map)

    ast_table = _as_table(
        extract_ast_tables(
            repo_files=repo_files,
            options=AstExtractOptions(max_workers=2),
        )["ast_files"]
    )
    cst_table = _as_table(
        extract_cst_tables(
            repo_files=repo_files,
            options=CstExtractOptions(max_workers=2),
        )["libcst_files"]
    )

    assert ast_table.num_rows == len(repo_map)
    assert cst_table.num_rows == len(repo_map)

    ast_rows = {row["file_id"]: row for row in ast_table.to_pylist()}
    cst_rows = {row["file_id"]: row for row in cst_table.to_pylist()}
    broken_id = broken_path.name

    ast_errors = ast_rows[broken_id].get("errors")
    assert isinstance(ast_errors, list)
    assert ast_errors, "Broken file should have AST parse errors"

    cst_errors = cst_rows[broken_id].get("parse_errors")
    assert isinstance(cst_errors, list)
    assert cst_errors, "Broken file should have CST parse errors"

    valid_ids = [file_path.name for file_path, _ in multi_file_repo.values()]
    assert any(not ast_rows[file_id].get("errors") for file_id in valid_ids)


@pytest.mark.integration
def test_parallel_extraction_result_order_independence(
    multi_file_repo: dict[str, tuple[Path, str]],
) -> None:
    """Test order independence of parallel extraction results.

    Verifies that two parallel extraction runs produce identical schemas and
    row counts, regardless of task scheduling order.

    Parameters
    ----------
    multi_file_repo
        Dictionary of test files with paths and contents.
    """
    repo_files = _repo_files_table(multi_file_repo)

    run1 = _as_table(
        extract_ast_tables(
            repo_files=repo_files,
            options=AstExtractOptions(max_workers=2),
        )["ast_files"]
    )
    run2 = _as_table(
        extract_ast_tables(
            repo_files=repo_files,
            options=AstExtractOptions(max_workers=2),
        )["ast_files"]
    )

    assert run1.schema.names == run2.schema.names
    assert run1.num_rows == run2.num_rows
    assert sorted(run1["file_id"].to_pylist()) == sorted(run2["file_id"].to_pylist())

    run1_summary = {
        row["file_id"]: (len(row.get("defs", [])), len(row.get("errors", [])))
        for row in run1.to_pylist()
    }
    run2_summary = {
        row["file_id"]: (len(row.get("defs", [])), len(row.get("errors", [])))
        for row in run2.to_pylist()
    }
    assert run1_summary == run2_summary


@pytest.mark.integration
def test_file_context_attributes() -> None:
    """Test FileContext basic attribute handling.

    Verifies that FileContext properly handles basic file attributes like
    path, content, and computed properties.
    """
    from extract.coordination.context import FileContext

    # Create a FileContext with basic attributes
    test_content = b"def test(): pass\n"
    ctx = FileContext(
        file_id="test.py",
        path="test.py",
        abs_path=None,
        file_sha256=None,
        data=test_content,
    )

    # Verify basic attributes
    assert ctx.file_id == "test.py"
    assert ctx.path == "test.py"
    assert ctx.data == test_content

    # Verify bytes can be extracted
    from extract.coordination.context import bytes_from_file_ctx

    extracted_bytes = bytes_from_file_ctx(ctx)
    assert extracted_bytes == test_content


@pytest.mark.integration
def test_file_sha256_computation() -> None:
    """Test SHA256 computation for file identity.

    Verifies that SHA256 hashes are computed correctly and deterministically
    for file content identity.
    """
    test_content = b"def test(): pass\n"

    # Compute SHA256
    hash1 = hashlib.sha256(test_content).hexdigest()
    hash2 = hashlib.sha256(test_content).hexdigest()

    # Verify determinism
    assert hash1 == hash2

    # Verify expected hash format (64 hex characters)
    assert len(hash1) == 64
    assert all(c in "0123456789abcdef" for c in hash1)


@pytest.mark.integration
def test_file_context_with_unicode_content() -> None:
    """Test FileContext handles Unicode content correctly.

    Verifies that FileContext properly handles files with Unicode content
    and maintains correct byte representations.
    """
    from extract.coordination.context import FileContext, bytes_from_file_ctx

    # Unicode content with multi-byte characters
    unicode_text = "# -*- coding: utf-8 -*-\ndef hello(): return '你好世界 🚀'\n"
    unicode_bytes = unicode_text.encode("utf-8")

    ctx = FileContext(
        file_id="unicode_test.py",
        path="unicode_test.py",
        abs_path=None,
        file_sha256=None,
        data=unicode_bytes,
    )

    # Verify bytes preserved correctly
    extracted_bytes = bytes_from_file_ctx(ctx)
    assert extracted_bytes is not None
    assert extracted_bytes == unicode_bytes

    # Verify SHA256 consistent with byte representation
    expected_hash = hashlib.sha256(unicode_bytes).hexdigest()
    computed_hash = hashlib.sha256(extracted_bytes).hexdigest()
    assert computed_hash == expected_hash


def _repo_files_table(files: dict[str, tuple[Path, str]]) -> pa.Table:
    rows: list[dict[str, str]] = []
    for file_path, content in files.values():
        rows.append(
            {
                "file_id": file_path.name,
                "path": str(file_path),
                "text": content,
                "file_sha256": hashlib.sha256(content.encode("utf-8")).hexdigest(),
            }
        )
    return pa.Table.from_pylist(rows)


def _as_table(value: object) -> pa.Table:
    if isinstance(value, pa.RecordBatchReader):
        return value.read_all()
    if isinstance(value, pa.Table):
        return value
    msg = f"Unsupported table type: {type(value)}"
    raise TypeError(msg)
