"""E2E tests for extraction to CPG output chain.

Tests full pipeline behavior from extraction through semantic compilation to final CPG outputs.
These tests verify the complete end-to-end data flow using the public API entry point.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from deltalake import DeltaTable

from graph import GraphProductBuildRequest, build_graph_product


@pytest.fixture
def minimal_python_repo(tmp_path: Path) -> Path:
    """Create a minimal Python repository for testing.

    Creates a single-file repository with a simple function definition.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory fixture.

    Returns:
    -------
    Path
        Path to the test repository root.
    """
    repo = tmp_path / "test_repo"
    repo.mkdir()
    (repo / "example.py").write_text("def hello() -> str:\n    return 'world'\n")
    return repo


@pytest.fixture
def empty_file_repo(tmp_path: Path) -> Path:
    """Create a repository with an empty Python file.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory fixture.

    Returns:
    -------
    Path
        Path to the test repository root.
    """
    repo = tmp_path / "empty_repo"
    repo.mkdir()
    (repo / "empty.py").write_text("")
    return repo


@pytest.mark.e2e
@pytest.mark.skip(
    reason=(
        "Requires full pipeline infrastructure including SCIP indexer, "
        "registered UDFs, and complete extractor chain"
    )
)
def test_single_file_repo_produces_cpg_nodes(minimal_python_repo: Path) -> None:
    """Full pipeline on 1-file repo produces valid CPG nodes.

    Verifies that:
    - Pipeline completes successfully
    - cpg_nodes output has positive row count
    - Node rows contain required fields: entity_id, path, bstart, bend
    - All required fields have non-null values

    Parameters
    ----------
    minimal_python_repo : Path
        Test repository with single Python file.
    """
    request = GraphProductBuildRequest(repo_root=minimal_python_repo)
    result = build_graph_product(request)

    # Verify pipeline completed with output
    assert result.cpg_nodes.rows > 0, "Expected at least one CPG node"

    # Read back the Delta table to inspect rows
    nodes_table = DeltaTable(str(result.cpg_nodes.paths.data))
    nodes_df = nodes_table.to_pyarrow_table()

    # Verify required columns exist
    schema = nodes_df.schema
    assert "entity_id" in schema.names, "Missing entity_id column"
    assert "path" in schema.names, "Missing path column"
    assert "bstart" in schema.names, "Missing bstart column"
    assert "bend" in schema.names, "Missing bend column"

    # Verify no null values in required columns
    assert nodes_df["entity_id"].null_count == 0, "entity_id has null values"
    assert nodes_df["path"].null_count == 0, "path has null values"
    assert nodes_df["bstart"].null_count == 0, "bstart has null values"
    assert nodes_df["bend"].null_count == 0, "bend has null values"

    # Verify path references the test file
    paths = nodes_df["path"].to_pylist()
    assert any("example.py" in str(p) for p in paths), "Expected nodes referencing example.py"


@pytest.mark.e2e
@pytest.mark.skip(
    reason=(
        "Requires full pipeline infrastructure including SCIP indexer, "
        "registered UDFs, and complete extractor chain"
    )
)
def test_entity_ids_are_deterministic(minimal_python_repo: Path) -> None:
    """Build twice; verify identical entity_id values.

    Verifies that:
    - Running the pipeline twice produces the same entity IDs
    - Entity IDs are deterministic and reproducible
    - Order-independent comparison (sorted lists match)

    Parameters
    ----------
    minimal_python_repo : Path
        Test repository with single Python file.
    """
    request = GraphProductBuildRequest(repo_root=minimal_python_repo)

    # First build
    result1 = build_graph_product(request)
    nodes1_table = DeltaTable(str(result1.cpg_nodes.paths.data))
    nodes1_df = nodes1_table.to_pyarrow_table()
    entity_ids1 = sorted(nodes1_df["entity_id"].to_pylist())

    # Second build
    result2 = build_graph_product(request)
    nodes2_table = DeltaTable(str(result2.cpg_nodes.paths.data))
    nodes2_df = nodes2_table.to_pyarrow_table()
    entity_ids2 = sorted(nodes2_df["entity_id"].to_pylist())

    # Verify identical entity IDs
    assert entity_ids1 == entity_ids2, "Entity IDs should be deterministic across builds"
    assert len(entity_ids1) > 0, "Expected at least one entity ID"


@pytest.mark.e2e
@pytest.mark.skip(
    reason=(
        "Requires full pipeline infrastructure including SCIP indexer, "
        "registered UDFs, and complete extractor chain"
    )
)
def test_byte_spans_within_file_bounds(minimal_python_repo: Path) -> None:
    """All byte spans are within file bounds.

    Verifies that:
    - All bstart values are >= 0
    - All bend values are >= bstart
    - All bend values are <= file size
    - No out-of-bounds byte spans exist

    Parameters
    ----------
    minimal_python_repo : Path
        Test repository with single Python file.
    """
    request = GraphProductBuildRequest(repo_root=minimal_python_repo)
    result = build_graph_product(request)

    # Read CPG nodes
    nodes_table = DeltaTable(str(result.cpg_nodes.paths.data))
    nodes_df = nodes_table.to_pyarrow_table()

    # Get file sizes for validation
    file_sizes: dict[str, int] = {}
    for py_file in minimal_python_repo.rglob("*.py"):
        file_sizes[str(py_file)] = py_file.stat().st_size

    # Verify byte span constraints
    for i in range(len(nodes_df)):
        path = str(nodes_df["path"][i].as_py())
        bstart = nodes_df["bstart"][i].as_py()
        bend = nodes_df["bend"][i].as_py()

        # Verify bstart is non-negative
        assert bstart >= 0, f"Invalid bstart {bstart} < 0 at row {i}"

        # Verify bend is at least bstart
        assert bend >= bstart, f"Invalid span: bend {bend} < bstart {bstart} at row {i}"

        # bend <= file size (when path is known)
        for known_path, size in file_sizes.items():
            if known_path.endswith(path) or path.endswith(known_path):
                assert bend <= size, f"bend {bend} exceeds file size {size} for {path} at row {i}"
                break


@pytest.mark.e2e
@pytest.mark.skip(
    reason=(
        "Requires full pipeline infrastructure including SCIP indexer, "
        "registered UDFs, and complete extractor chain"
    )
)
def test_graceful_degradation_empty_file(empty_file_repo: Path) -> None:
    """Empty Python file included; pipeline completes with correct schema.

    Verifies graceful degradation when processing empty files:
    - Pipeline completes without errors
    - Output maintains correct schema
    - Empty file either produces no nodes or produces file-level node
    - No malformed or invalid rows

    Parameters
    ----------
    empty_file_repo : Path
        Test repository with empty Python file.
    """
    request = GraphProductBuildRequest(repo_root=empty_file_repo)
    result = build_graph_product(request)

    # Verify pipeline completed (may have 0 rows for empty file)
    assert result.cpg_nodes.rows >= 0, "Expected non-negative row count"

    # Read back the Delta table
    nodes_table = DeltaTable(str(result.cpg_nodes.paths.data))
    nodes_df = nodes_table.to_pyarrow_table()

    # Verify schema is correct even if no rows
    schema = nodes_df.schema
    assert "entity_id" in schema.names, "Missing entity_id column"
    assert "path" in schema.names, "Missing path column"
    assert "bstart" in schema.names, "Missing bstart column"
    assert "bend" in schema.names, "Missing bend column"

    # If there are rows referencing the empty file, verify they are valid
    for i in range(len(nodes_df)):
        path = str(nodes_df["path"][i].as_py())
        if "empty.py" in path:
            bstart = nodes_df["bstart"][i].as_py()
            bend = nodes_df["bend"][i].as_py()

            # Even for empty files, spans must be valid (0-0 or similar)
            assert bstart >= 0, f"Invalid bstart {bstart} for empty file"
            assert bend >= bstart, f"Invalid span for empty file: {bstart}-{bend}"
            assert bend == 0, "Expected bend=0 for empty file"
