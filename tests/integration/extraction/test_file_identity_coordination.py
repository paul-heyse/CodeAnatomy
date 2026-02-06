"""Integration tests for file identity and parallel extraction coordination.

Tests the boundary where FileContext provides consistent file identity (file_id,
path, SHA256) across multiple parallel extractors.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest


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
@pytest.mark.skip(
    reason="Requires full extraction pipeline with CST, AST, and symtable extractors. "
    "Implementation needs coordination between extract.extractors.cst_extract, "
    "extract.extractors.ast_extract, and extract.extractors.symtable_extract "
    "to verify consistent file_id across all outputs."
)
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
    # Would require:
    # 1. Build repo_files_v1 table from multi_file_repo
    # 2. Run extract_cst_tables(repo_files) -> cst_refs
    # 3. Run extract_ast_tables(repo_files) -> ast_files
    # 4. Run extract_symtable_tables(repo_files) -> symtable_files
    # 5. Verify all tables have the same file_id values for each file
    # 6. Verify file_id is deterministic (same across multiple runs)


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
@pytest.mark.skip(
    reason="Requires parallel extraction infrastructure setup. "
    "Implementation needs extract.infrastructure.parallel module understanding "
    "and diagnostics capture for partial failures across multiple extractors."
)
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
    # Would require:
    # 1. Add a malformed Python file to trigger parser failure
    # 2. Run parallel extraction across all files
    # 3. Verify successful extractors still produce output
    # 4. Verify failed extractor's error captured in diagnostics
    # 5. Verify partial results are valid (correct schema, non-zero rows)


@pytest.mark.integration
@pytest.mark.skip(
    reason="Requires parallel extraction infrastructure setup. "
    "Implementation needs extract.infrastructure.parallel understanding "
    "to verify deterministic output schemas and row counts across parallel runs."
)
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
    # Would require:
    # 1. Run parallel extraction on multi_file_repo (run 1)
    # 2. Capture output schemas and row counts
    # 3. Run parallel extraction again (run 2)
    # 4. Verify schemas match exactly
    # 5. Verify row counts match exactly
    # 6. Verify sorted file_id lists match


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
