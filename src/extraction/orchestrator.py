"""Standalone extraction orchestrator.

Replace Hamilton DAG extraction with direct staged execution.
Each extractor runs as a plain Python function producing Arrow tables,
which are written to Delta storage.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING

import msgspec

from obs.otel.metrics import record_error, record_stage_duration
from obs.otel.scopes import SCOPE_EXTRACT
from obs.otel.tracing import stage_span

if TYPE_CHECKING:
    import pyarrow as pa

    from engine.runtime_profile import RuntimeProfileSpec
    from extract.session import ExtractSession

logger = logging.getLogger(__name__)


class ExtractionResult(msgspec.Struct, frozen=True):
    """Result of running the extraction pipeline."""

    delta_locations: dict[str, str]
    errors: list[dict[str, object]]
    timing: dict[str, float]


def run_extraction(
    repo_root: Path,
    work_dir: Path,
    *,
    scip_index_config: object | None = None,
    scip_identity_overrides: object | None = None,
    tree_sitter_enabled: bool = True,
    max_workers: int = 6,
) -> ExtractionResult:
    """Run the full extraction pipeline with staged execution.

    Parameters
    ----------
    repo_root
        Repository root to extract from.
    work_dir
        Working directory for Delta output tables.
    scip_index_config
        Optional SCIP indexing configuration.
    scip_identity_overrides
        Optional SCIP identity overrides.
    tree_sitter_enabled
        Whether to enable tree-sitter extraction.
    max_workers
        Maximum parallel workers for Stage 1.

    Returns:
    -------
    ExtractionResult
        Delta locations, errors, and timing data.
    """
    delta_locations: dict[str, str] = {}
    errors: list[dict[str, object]] = []
    timing: dict[str, float] = {}
    extract_dir = work_dir / "extract"
    extract_dir.mkdir(parents=True, exist_ok=True)

    # Track overall extraction phase timing
    extraction_start = time.monotonic()

    # Stage 0: repo_scan (sequential, prerequisite for all others)
    t0 = time.monotonic()
    try:
        with stage_span(
            "extraction.repo_scan",
            stage="extraction",
            scope_name=SCOPE_EXTRACT,
            attributes={"extractor": "repo_scan"},
        ):
            repo_files = _run_repo_scan(repo_root)
        timing["repo_scan"] = time.monotonic() - t0
        delta_locations["repo_files"] = _write_delta(
            repo_files, extract_dir / "repo_files", "repo_files"
        )
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        timing["repo_scan"] = time.monotonic() - t0
        errors.append({"extractor": "repo_scan", "error": str(exc)})
        record_error("extraction", type(exc).__name__)
        logger.warning("repo_scan failed: %s", exc)
        # Cannot proceed without repo_files
        return ExtractionResult(
            delta_locations=delta_locations,
            errors=errors,
            timing=timing,
        )

    # Stage 1: parallel extractors (all depend on repo_files)
    stage1_extractors = _build_stage1_extractors(
        repo_root=repo_root,
        repo_files=repo_files,
        scip_index_config=scip_index_config,
        scip_identity_overrides=scip_identity_overrides,
        tree_sitter_enabled=tree_sitter_enabled,
    )

    t1 = time.monotonic()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {name: executor.submit(fn) for name, fn in stage1_extractors.items()}
        for name, future in futures.items():
            try:
                with stage_span(
                    f"extraction.{name}",
                    stage="extraction",
                    scope_name=SCOPE_EXTRACT,
                    attributes={"extractor": name},
                ):
                    result_table = future.result()
                timing[name] = time.monotonic() - t1
                delta_locations[name] = _write_delta(result_table, extract_dir / name, name)
            except (OSError, RuntimeError, TypeError, ValueError) as exc:
                timing[name] = time.monotonic() - t1
                errors.append({"extractor": name, "error": str(exc)})
                record_error("extraction", type(exc).__name__)
                logger.warning("Extractor %s failed: %s", name, exc)

    # Stage 2: python_imports (depends on ast_imports, cst_imports, ts_imports)
    t2 = time.monotonic()
    try:
        with stage_span(
            "extraction.python_imports",
            stage="extraction",
            scope_name=SCOPE_EXTRACT,
            attributes={"extractor": "python_imports"},
        ):
            python_imports = _run_python_imports(delta_locations)
        timing["python_imports"] = time.monotonic() - t2
        delta_locations["python_imports"] = _write_delta(
            python_imports, extract_dir / "python_imports", "python_imports"
        )
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        timing["python_imports"] = time.monotonic() - t2
        errors.append({"extractor": "python_imports", "error": str(exc)})
        record_error("extraction", type(exc).__name__)
        logger.warning("python_imports failed: %s", exc)

    # Stage 3: python_external (depends on python_imports)
    t3 = time.monotonic()
    try:
        with stage_span(
            "extraction.python_external",
            stage="extraction",
            scope_name=SCOPE_EXTRACT,
            attributes={"extractor": "python_external"},
        ):
            python_external = _run_python_external(delta_locations, repo_root)
        timing["python_external"] = time.monotonic() - t3
        delta_locations["python_external"] = _write_delta(
            python_external, extract_dir / "python_external", "python_external"
        )
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        timing["python_external"] = time.monotonic() - t3
        errors.append({"extractor": "python_external", "error": str(exc)})
        record_error("extraction", type(exc).__name__)
        logger.warning("python_external failed: %s", exc)

    # Record overall extraction phase duration
    extraction_elapsed = time.monotonic() - extraction_start
    extraction_status = "ok" if not errors else "error"
    record_stage_duration("extraction", extraction_elapsed, status=extraction_status)

    return ExtractionResult(
        delta_locations=delta_locations,
        errors=errors,
        timing=timing,
    )


def _write_delta(table: pa.Table, location: Path, name: str) -> str:
    """Write an Arrow table to a Delta table location.

    Parameters
    ----------
    table
        Arrow table to write.
    location
        Delta table location path.
    name
        Table name for logging.

    Returns:
    -------
    str
        Location path as a string.
    """
    import deltalake

    location.mkdir(parents=True, exist_ok=True)
    loc_str = str(location)
    deltalake.write_deltalake(loc_str, table, mode="overwrite")
    logger.info("Wrote %d rows to %s at %s", table.num_rows, name, loc_str)
    return loc_str


def _run_repo_scan(repo_root: Path) -> pa.Table:
    """Run repo scan extraction.

    Parameters
    ----------
    repo_root
        Repository root to scan.

    Returns:
    -------
    pa.Table
        Repo files table.

    Raises:
        ValueError: If repo scan produces no output.
    """
    from engine.runtime_profile import resolve_runtime_profile
    from engine.session_factory import build_engine_session
    from extract.coordination.context import ExtractExecutionContext
    from extract.python.scope import PythonScopePolicy
    from extract.scanning.repo_scan import RepoScanOptions, scan_repo_tables
    from extract.scanning.repo_scope import RepoScopeOptions
    from extract.session import ExtractSession

    # Build runtime profile and session
    runtime_spec = resolve_runtime_profile("default")
    engine_session = build_engine_session(runtime_spec=runtime_spec)
    extract_session = ExtractSession(engine_session=engine_session)

    # Build scan options directly (no Hamilton dependency)
    scope_policy = RepoScopeOptions(
        python_scope=PythonScopePolicy(),
        include_globs=(),
        exclude_globs=(),
        include_untracked=True,
        include_submodules=False,
        include_worktrees=False,
        follow_symlinks=False,
    )
    options = RepoScanOptions(
        repo_id="extraction_orchestrator",
        scope_policy=scope_policy,
        max_files=None,
        changed_only=False,
    )

    # Execute repo scan
    exec_ctx = ExtractExecutionContext(
        session=extract_session,
        runtime_spec=runtime_spec,
    )

    outputs = scan_repo_tables(
        str(repo_root),
        options=options,
        context=exec_ctx,
        prefer_reader=False,
    )

    # Extract repo_files table (key is with _v1 suffix)
    repo_files = outputs.get("repo_files_v1")
    if repo_files is None:
        msg = "repo_scan did not produce repo_files_v1 output"
        raise ValueError(msg)

    # Coerce to Arrow table
    return _coerce_to_table(repo_files)


def _build_stage1_extractors(
    *,
    repo_root: Path,
    repo_files: pa.Table,
    scip_index_config: object | None,
    scip_identity_overrides: object | None,
    tree_sitter_enabled: bool,
) -> dict[str, Callable[[], pa.Table]]:
    """Build Stage 1 extractor callables.

    Parameters
    ----------
    repo_root
        Repository root.
    repo_files
        Repo files table from Stage 0.
    scip_index_config
        Optional SCIP indexing configuration.
    scip_identity_overrides
        Optional SCIP identity overrides.
    tree_sitter_enabled
        Whether to enable tree-sitter extraction.

    Returns:
    -------
    dict[str, Callable[[], pa.Table]]
        Mapping of extractor name to callable.
    """
    from engine.runtime_profile import resolve_runtime_profile
    from engine.session_factory import build_engine_session
    from extract.session import ExtractSession

    # Build session properly
    runtime_spec = resolve_runtime_profile("default")
    engine_session = build_engine_session(runtime_spec=runtime_spec)
    extract_session = ExtractSession(engine_session=engine_session)

    extractors: dict[str, Callable[[], pa.Table]] = {
        "ast_imports": lambda: _extract_ast(repo_files, extract_session, runtime_spec),
        "ast_symbols": lambda: _extract_ast(repo_files, extract_session, runtime_spec),
        "cst_imports": lambda: _extract_cst(repo_files, extract_session, runtime_spec),
        "cst_symbols": lambda: _extract_cst(repo_files, extract_session, runtime_spec),
        "bytecode_files": lambda: _extract_bytecode(repo_files, extract_session, runtime_spec),
        "symtable_files": lambda: _extract_symtable(repo_files, extract_session, runtime_spec),
    }

    if tree_sitter_enabled:
        extractors["ts_imports"] = lambda: _extract_tree_sitter(
            repo_files, extract_session, runtime_spec
        )
        extractors["ts_symbols"] = lambda: _extract_tree_sitter(
            repo_files, extract_session, runtime_spec
        )

    if scip_index_config is not None:
        extractors["scip_symbols"] = lambda: _extract_scip(
            repo_root,
            scip_index_config,
            scip_identity_overrides,
            extract_session,
            runtime_spec,
        )

    return extractors


def _extract_ast(
    repo_files: pa.Table,
    extract_session: ExtractSession,
    runtime_spec: RuntimeProfileSpec,
) -> pa.Table:
    """Extract AST tables.

    Parameters
    ----------
    repo_files
        Repo files table.
    extract_session
        Extract session.
    runtime_spec
        Runtime profile specification.

    Returns:
    -------
    pa.Table
        AST imports/symbols table.

    Raises:
        ValueError: If extraction produces no outputs.
    """
    from extract.extractors.ast_extract import extract_ast_tables

    outputs = extract_ast_tables(
        repo_files=repo_files,
        session=extract_session,
        profile=runtime_spec.name,
        prefer_reader=False,
    )

    # Merge ast_imports and ast_symbols outputs
    ast_imports = outputs.get("ast_imports")
    ast_symbols = outputs.get("ast_symbols")

    if ast_imports is not None:
        return _coerce_to_table(ast_imports)
    if ast_symbols is not None:
        return _coerce_to_table(ast_symbols)

    msg = "ast extraction produced no outputs"
    raise ValueError(msg)


def _extract_cst(
    repo_files: pa.Table,
    extract_session: ExtractSession,
    runtime_spec: RuntimeProfileSpec,
) -> pa.Table:
    """Extract CST tables.

    Parameters
    ----------
    repo_files
        Repo files table.
    extract_session
        Extract session.
    runtime_spec
        Runtime profile specification.

    Returns:
    -------
    pa.Table
        CST imports/symbols table.

    Raises:
        ValueError: If extraction produces no outputs.
    """
    from extract.extractors.cst_extract import extract_cst_tables

    outputs = extract_cst_tables(
        repo_files=repo_files,
        session=extract_session,
        profile=runtime_spec.name,
        prefer_reader=False,
    )

    # Merge cst_imports and cst_symbols outputs
    cst_imports = outputs.get("cst_imports")
    cst_symbols = outputs.get("cst_symbols")

    if cst_imports is not None:
        return _coerce_to_table(cst_imports)
    if cst_symbols is not None:
        return _coerce_to_table(cst_symbols)

    msg = "cst extraction produced no outputs"
    raise ValueError(msg)


def _extract_tree_sitter(
    repo_files: pa.Table,
    extract_session: ExtractSession,
    runtime_spec: RuntimeProfileSpec,
) -> pa.Table:
    """Extract tree-sitter tables.

    Parameters
    ----------
    repo_files
        Repo files table.
    extract_session
        Extract session.
    runtime_spec
        Runtime profile specification.

    Returns:
    -------
    pa.Table
        Tree-sitter imports/symbols table.

    Raises:
        ValueError: If extraction produces no outputs.
    """
    from extract.extractors.tree_sitter.extract import extract_ts_tables

    outputs = extract_ts_tables(
        repo_files=repo_files,
        session=extract_session,
        profile=runtime_spec.name,
        prefer_reader=False,
    )

    # Merge ts_imports and ts_symbols outputs
    ts_imports = outputs.get("ts_imports")
    ts_symbols = outputs.get("ts_symbols")

    if ts_imports is not None:
        return _coerce_to_table(ts_imports)
    if ts_symbols is not None:
        return _coerce_to_table(ts_symbols)

    msg = "tree-sitter extraction produced no outputs"
    raise ValueError(msg)


def _extract_bytecode(
    repo_files: pa.Table,
    extract_session: ExtractSession,
    runtime_spec: RuntimeProfileSpec,
) -> pa.Table:
    """Extract bytecode table.

    Parameters
    ----------
    repo_files
        Repo files table.
    extract_session
        Extract session.
    runtime_spec
        Runtime profile specification.

    Returns:
    -------
    pa.Table
        Bytecode files table.
    """
    from extract.extractors.bytecode_extract import extract_bytecode_table

    table = extract_bytecode_table(
        repo_files=repo_files,
        session=extract_session,
        profile=runtime_spec.name,
        prefer_reader=False,
    )

    return _coerce_to_table(table)


def _extract_symtable(
    repo_files: pa.Table,
    extract_session: ExtractSession,
    runtime_spec: RuntimeProfileSpec,
) -> pa.Table:
    """Extract symtable table.

    Parameters
    ----------
    repo_files
        Repo files table.
    extract_session
        Extract session.
    runtime_spec
        Runtime profile specification.

    Returns:
    -------
    pa.Table
        Symtable files table.
    """
    from extract.extractors.symtable_extract import extract_symtables_table

    table = extract_symtables_table(
        repo_files=repo_files,
        session=extract_session,
        profile=runtime_spec.name,
        prefer_reader=False,
    )

    return _coerce_to_table(table)


def _extract_scip(
    repo_root: Path,
    scip_index_config: object,
    scip_identity_overrides: object | None,  # noqa: ARG001
    extract_session: ExtractSession,
    runtime_spec: RuntimeProfileSpec,
) -> pa.Table:
    """Extract SCIP tables.

    Parameters
    ----------
    repo_root
        Repository root.
    scip_index_config
        SCIP indexing configuration.
    scip_identity_overrides
        Optional SCIP identity overrides.
    extract_session
        Extract session.
    runtime_spec
        Runtime profile specification.

    Returns:
    -------
    pa.Table
        SCIP symbols table.

    Raises:
        ValueError: If extraction produces no outputs.
    """
    # Build SCIP extract options
    from extract.extractors.scip.extract import (
        ScipExtractContext,
        ScipExtractOptions,
        extract_scip_tables,
    )

    scip_extract_options = ScipExtractOptions()

    # Extract scip_index_path from config
    scip_index_path = getattr(scip_index_config, "scip_index_path", None)

    context = ScipExtractContext(
        scip_index_path=scip_index_path,
        repo_root=str(repo_root),
        session=extract_session,
        runtime_spec=runtime_spec,
    )

    outputs = extract_scip_tables(
        context=context,
        options=scip_extract_options,
        prefer_reader=False,
    )

    scip_symbols = outputs.get("scip_symbols")
    if scip_symbols is None:
        msg = "scip extraction produced no scip_symbols output"
        raise ValueError(msg)

    return _coerce_to_table(scip_symbols)


def _run_python_imports(delta_locations: dict[str, str]) -> pa.Table:
    """Run python_imports extraction.

    Parameters
    ----------
    delta_locations
        Mapping of dataset name to Delta location.

    Returns:
    -------
    pa.Table
        Python imports table.

    Raises:
        ValueError: If extraction produces no output.
    """
    from engine.runtime_profile import resolve_runtime_profile
    from engine.session_factory import build_engine_session
    from extract.extractors.imports_extract import extract_python_imports_tables
    from extract.session import ExtractSession

    # Load inputs from Delta locations
    ast_imports = _load_delta_table(delta_locations.get("ast_imports"))
    cst_imports = _load_delta_table(delta_locations.get("cst_imports"))
    ts_imports = _load_delta_table(delta_locations.get("ts_imports"))

    # Create extract session
    runtime_spec = resolve_runtime_profile("default")
    engine_session = build_engine_session(runtime_spec=runtime_spec)
    extract_session = ExtractSession(engine_session=engine_session)

    outputs = extract_python_imports_tables(
        ast_imports=ast_imports,
        cst_imports=cst_imports,
        ts_imports=ts_imports,
        session=extract_session,
        profile=runtime_spec.name,
        prefer_reader=False,
    )

    python_imports = outputs.get("python_imports")
    if python_imports is None:
        msg = "python_imports extraction produced no output"
        raise ValueError(msg)

    return _coerce_to_table(python_imports)


def _run_python_external(delta_locations: dict[str, str], repo_root: Path) -> pa.Table:
    """Run python_external extraction.

    Parameters
    ----------
    delta_locations
        Mapping of dataset name to Delta location.
    repo_root
        Repository root path.

    Returns:
    -------
    pa.Table
        Python external table.

    Raises:
        ValueError: If python_imports table is missing or extraction produces no output.
    """
    from engine.runtime_profile import resolve_runtime_profile
    from engine.session_factory import build_engine_session
    from extract.extractors.external_scope import extract_python_external_tables
    from extract.session import ExtractSession

    # Load python_imports from Delta location
    python_imports_table = _load_delta_table(delta_locations.get("python_imports"))
    if python_imports_table is None:
        msg = "python_imports table is required for python_external extraction"
        raise ValueError(msg)

    # Create extract session
    runtime_spec = resolve_runtime_profile("default")
    engine_session = build_engine_session(runtime_spec=runtime_spec)
    extract_session = ExtractSession(engine_session=engine_session)

    outputs = extract_python_external_tables(
        python_imports=python_imports_table,
        repo_root=str(repo_root),
        session=extract_session,
        profile=runtime_spec.name,
        prefer_reader=False,
    )

    python_external = outputs.get("python_external")
    if python_external is None:
        msg = "python_external extraction produced no output"
        raise ValueError(msg)

    return _coerce_to_table(python_external)


def _load_delta_table(location: str | None) -> pa.Table | None:
    """Load an Arrow table from a Delta location.

    Parameters
    ----------
    location
        Delta table location path.

    Returns:
    -------
    pa.Table | None
        Arrow table or None if location is None.
    """
    if location is None:
        return None

    import deltalake

    delta_table = deltalake.DeltaTable(location)
    return delta_table.to_pyarrow_table()


def _coerce_to_table(value: object) -> pa.Table:
    """Coerce a value to an Arrow table.

    Parameters
    ----------
    value
        Value to coerce.

    Returns:
    -------
    pa.Table
        Arrow table.

    Raises:
        TypeError: If value cannot be coerced to a table.
    """
    import pyarrow as pa

    if isinstance(value, pa.Table):
        return value

    # Try to_arrow_table() method
    to_arrow = getattr(value, "to_arrow_table", None)
    if callable(to_arrow):
        result = to_arrow()
        if isinstance(result, pa.Table):
            return result

    # Try read_all() for RecordBatchReader
    read_all = getattr(value, "read_all", None)
    if callable(read_all):
        result = read_all()
        if isinstance(result, pa.Table):
            return result

    msg = f"Cannot coerce {type(value).__name__} to Arrow table"
    raise TypeError(msg)


__all__ = [
    "ExtractionResult",
    "run_extraction",
]
