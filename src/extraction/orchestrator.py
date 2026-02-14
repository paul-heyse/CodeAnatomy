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
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import msgspec

from extraction.contracts import (
    RunExtractionRequestV1,
    resolve_semantic_input_locations,
    with_compat_aliases,
)
from extraction.options import ExtractionRunOptions, normalize_extraction_options
from obs.otel.metrics import record_error, record_stage_duration
from obs.otel.scopes import SCOPE_EXTRACT
from obs.otel.tracing import stage_span

if TYPE_CHECKING:
    import pyarrow as pa

    from extract.session import ExtractSession
    from extraction.runtime_profile import RuntimeProfileSpec

logger = logging.getLogger(__name__)


class ExtractionResult(msgspec.Struct, frozen=True):
    """Result of running the extraction pipeline."""

    delta_locations: dict[str, str]
    semantic_input_locations: dict[str, str]
    errors: list[dict[str, object]]
    timing: dict[str, float]


@dataclass
class _ExtractionRunState:
    delta_locations: dict[str, str]
    semantic_input_locations: dict[str, str]
    errors: list[dict[str, object]]
    timing: dict[str, float]


@dataclass(frozen=True)
class _Stage1ExecutionRequest:
    repo_root: Path
    repo_files: pa.Table
    extract_dir: Path
    scip_index_config: object | None
    scip_identity_overrides: object | None
    tree_sitter_enabled: bool
    max_workers: int


def run_extraction(request: RunExtractionRequestV1) -> ExtractionResult:
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
    options
        Additional extraction options (repo scope/incremental controls).

    Returns:
    -------
    ExtractionResult
        Delta locations, errors, and timing data.

    Raises:
    -------
    ValueError
        If required Stage 0 repo scan outputs are missing.
    """
    repo_root = Path(request.repo_root)
    extract_dir = Path(request.work_dir) / "extract"
    extract_dir.mkdir(parents=True, exist_ok=True)
    state = _ExtractionRunState(
        delta_locations={},
        semantic_input_locations={},
        errors=[],
        timing={},
    )
    resolved_options = normalize_extraction_options(
        request.options,
        default_tree_sitter_enabled=request.tree_sitter_enabled,
        default_max_workers=request.max_workers,
    )
    extraction_start = time.monotonic()
    repo_files = _run_repo_scan_with_fallback(
        repo_root=repo_root,
        extract_dir=extract_dir,
        options=resolved_options,
        state=state,
    )
    if repo_files is None:
        return _materialize_extraction_result(state)
    _run_parallel_stage1_extractors(
        _Stage1ExecutionRequest(
            repo_root=repo_root,
            repo_files=repo_files,
            extract_dir=extract_dir,
            scip_index_config=request.scip_index_config,
            scip_identity_overrides=request.scip_identity_overrides,
            tree_sitter_enabled=resolved_options.tree_sitter_enabled,
            max_workers=resolved_options.max_workers,
        ),
        state=state,
    )
    _run_python_imports_stage(extract_dir=extract_dir, state=state)
    _run_python_external_stage(repo_root=repo_root, extract_dir=extract_dir, state=state)
    _finalize_extraction_state(state=state, extraction_start=extraction_start)
    return _materialize_extraction_result(state)


def _materialize_extraction_result(state: _ExtractionRunState) -> ExtractionResult:
    return ExtractionResult(
        delta_locations=state.delta_locations,
        semantic_input_locations=state.semantic_input_locations,
        errors=state.errors,
        timing=state.timing,
    )


def _record_repo_scan_outputs(
    *,
    outputs: dict[str, pa.Table],
    extract_dir: Path,
    state: _ExtractionRunState,
) -> pa.Table:
    repo_files = _require_repo_scan_table(outputs, "repo_files_v1")
    for name, table in sorted(outputs.items()):
        state.delta_locations[name] = _write_delta(table, extract_dir / name, name)
    return repo_files


def _run_repo_scan_with_fallback(
    *,
    repo_root: Path,
    extract_dir: Path,
    options: ExtractionRunOptions,
    state: _ExtractionRunState,
) -> pa.Table | None:
    t0 = time.monotonic()
    try:
        with stage_span(
            "extraction.repo_scan",
            stage="extraction",
            scope_name=SCOPE_EXTRACT,
            attributes={"extractor": "repo_scan"},
        ):
            outputs = _run_repo_scan(repo_root, options=options)
        state.timing["repo_scan"] = time.monotonic() - t0
        return _record_repo_scan_outputs(outputs=outputs, extract_dir=extract_dir, state=state)
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        state.timing["repo_scan"] = time.monotonic() - t0
        state.errors.append({"extractor": "repo_scan", "error": str(exc)})
        record_error("extraction", type(exc).__name__)
        logger.warning("repo_scan failed: %s", exc)
    return _run_repo_scan_fallback_stage(
        repo_root=repo_root,
        extract_dir=extract_dir,
        options=options,
        state=state,
    )


def _run_repo_scan_fallback_stage(
    *,
    repo_root: Path,
    extract_dir: Path,
    options: ExtractionRunOptions,
    state: _ExtractionRunState,
) -> pa.Table | None:
    t0 = time.monotonic()
    try:
        with stage_span(
            "extraction.repo_scan_fallback",
            stage="extraction",
            scope_name=SCOPE_EXTRACT,
            attributes={"extractor": "repo_scan_fallback"},
        ):
            outputs = _run_repo_scan_fallback(repo_root, options=options)
        state.timing["repo_scan_fallback"] = time.monotonic() - t0
        repo_files = _record_repo_scan_outputs(
            outputs=outputs, extract_dir=extract_dir, state=state
        )
        logger.warning(
            "Using non-git repo scan fallback with %d discovered files",
            repo_files.num_rows,
        )
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        state.timing["repo_scan_fallback"] = time.monotonic() - t0
        state.errors.append({"extractor": "repo_scan_fallback", "error": str(exc)})
        record_error("extraction", type(exc).__name__)
        logger.warning("repo_scan_fallback failed: %s", exc)
        return None
    else:
        return repo_files


def _run_parallel_stage1_extractors(
    request: _Stage1ExecutionRequest,
    *,
    state: _ExtractionRunState,
) -> None:
    extractors = _build_stage1_extractors(
        repo_root=request.repo_root,
        repo_files=request.repo_files,
        scip_index_config=request.scip_index_config,
        scip_identity_overrides=request.scip_identity_overrides,
        tree_sitter_enabled=request.tree_sitter_enabled,
    )
    stage_start = time.monotonic()
    with ThreadPoolExecutor(max_workers=request.max_workers) as executor:
        futures = {name: executor.submit(fn) for name, fn in extractors.items()}
        for name, future in futures.items():
            try:
                with stage_span(
                    f"extraction.{name}",
                    stage="extraction",
                    scope_name=SCOPE_EXTRACT,
                    attributes={"extractor": name},
                ):
                    result_table = future.result()
                state.timing[name] = time.monotonic() - stage_start
                state.delta_locations[name] = _write_delta(
                    result_table,
                    request.extract_dir / name,
                    name,
                )
            except (OSError, RuntimeError, TypeError, ValueError) as exc:
                state.timing[name] = time.monotonic() - stage_start
                state.errors.append({"extractor": name, "error": str(exc)})
                record_error("extraction", type(exc).__name__)
                logger.warning("Extractor %s failed: %s", name, exc)


def _run_python_imports_stage(*, extract_dir: Path, state: _ExtractionRunState) -> None:
    t0 = time.monotonic()
    try:
        with stage_span(
            "extraction.python_imports",
            stage="extraction",
            scope_name=SCOPE_EXTRACT,
            attributes={"extractor": "python_imports"},
        ):
            python_imports = _run_python_imports(state.delta_locations)
        state.timing["python_imports"] = time.monotonic() - t0
        state.delta_locations["python_imports"] = _write_delta(
            python_imports,
            extract_dir / "python_imports",
            "python_imports",
        )
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        state.timing["python_imports"] = time.monotonic() - t0
        state.errors.append({"extractor": "python_imports", "error": str(exc)})
        record_error("extraction", type(exc).__name__)
        logger.warning("python_imports failed: %s", exc)


def _run_python_external_stage(
    *,
    repo_root: Path,
    extract_dir: Path,
    state: _ExtractionRunState,
) -> None:
    t0 = time.monotonic()
    try:
        with stage_span(
            "extraction.python_external",
            stage="extraction",
            scope_name=SCOPE_EXTRACT,
            attributes={"extractor": "python_external"},
        ):
            python_external = _run_python_external(state.delta_locations, repo_root)
        state.timing["python_external"] = time.monotonic() - t0
        state.delta_locations["python_external_interfaces"] = _write_delta(
            python_external,
            extract_dir / "python_external_interfaces",
            "python_external_interfaces",
        )
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        state.timing["python_external"] = time.monotonic() - t0
        state.errors.append({"extractor": "python_external", "error": str(exc)})
        record_error("extraction", type(exc).__name__)
        logger.warning("python_external failed: %s", exc)


def _finalize_extraction_state(*, state: _ExtractionRunState, extraction_start: float) -> None:
    state.delta_locations = with_compat_aliases(state.delta_locations)
    state.semantic_input_locations = resolve_semantic_input_locations(state.delta_locations)
    state.delta_locations.update(state.semantic_input_locations)
    extraction_elapsed = time.monotonic() - extraction_start
    extraction_status = "ok" if not state.errors else "error"
    record_stage_duration("extraction", extraction_elapsed, status=extraction_status)


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


def _require_repo_scan_table(
    repo_scan_outputs: dict[str, pa.Table],
    table_name: str,
) -> pa.Table:
    table = repo_scan_outputs.get(table_name)
    if table is None:
        msg = f"repo_scan did not produce {table_name} output"
        raise ValueError(msg)
    return table


def _run_repo_scan(
    repo_root: Path,
    *,
    options: ExtractionRunOptions,
) -> dict[str, pa.Table]:
    """Run repo scan extraction.

    Parameters
    ----------
    repo_root
        Repository root to scan.

    Returns:
    -------
    dict[str, pa.Table]
        Repo scan tables keyed by dataset name.
    """
    from extract.coordination.context import ExtractExecutionContext
    from extract.python.scope import PythonScopePolicy
    from extract.scanning.repo_scan import RepoScanOptions, scan_repo_tables
    from extract.scanning.repo_scope import RepoScopeOptions
    from extract.session import ExtractSession
    from extraction.engine_session_factory import build_engine_session
    from extraction.runtime_profile import resolve_runtime_profile

    # Build runtime profile and session
    runtime_spec = resolve_runtime_profile("default")
    engine_session = build_engine_session(runtime_spec=runtime_spec)
    extract_session = ExtractSession(engine_session=engine_session)

    scope_policy = RepoScopeOptions(
        python_scope=PythonScopePolicy(),
        include_globs=options.include_globs,
        exclude_globs=options.exclude_globs,
        include_untracked=options.include_untracked,
        include_submodules=options.include_submodules,
        include_worktrees=options.include_worktrees,
        follow_symlinks=options.follow_symlinks,
    )
    scan_options = RepoScanOptions(
        repo_id="extraction_orchestrator",
        scope_policy=scope_policy,
        max_files=None,
        diff_base_ref=options.diff_base_ref,
        diff_head_ref=options.diff_head_ref,
        changed_only=options.changed_only,
    )

    # Execute repo scan
    exec_ctx = ExtractExecutionContext(
        session=extract_session,
        runtime_spec=runtime_spec,
    )

    outputs = scan_repo_tables(
        str(repo_root),
        options=scan_options,
        context=exec_ctx,
        prefer_reader=False,
    )

    return {name: _coerce_to_table(value) for name, value in outputs.items()}


def _run_repo_scan_fallback(
    repo_root: Path,
    *,
    options: ExtractionRunOptions,
) -> dict[str, pa.Table]:
    """Fallback repo scan for non-git workdirs.

    Produces a minimal ``repo_files_v1`` table by walking Python files directly
    from the filesystem while honoring include/exclude globs.

    Args:
        repo_root: Repository root to scan.
        options: Extraction include/exclude scope options.

    Returns:
        dict[str, pyarrow.Table]: Single-table mapping containing ``repo_files_v1``.

    Raises:
        ValueError: If no Python source files are found under the requested scope.
    """
    import pyarrow as pa

    from datafusion_engine.hashing import stable_id
    from utils.file_io import detect_encoding
    from utils.hashing import hash_file_sha256

    include_globs = tuple(options.include_globs) if options.include_globs else ("**/*.py",)
    exclude_globs = tuple(options.exclude_globs)

    rows: list[dict[str, object]] = []
    seen_paths: set[str] = set()
    for include_glob in include_globs:
        for path in sorted(repo_root.glob(include_glob)):
            if not path.is_file():
                continue
            rel_path = path.relative_to(repo_root).as_posix()
            if rel_path in seen_paths:
                continue
            if exclude_globs and any(Path(rel_path).match(pattern) for pattern in exclude_globs):
                continue
            if not rel_path.endswith(".py"):
                continue
            seen_paths.add(rel_path)

            try:
                stat_result = path.stat()
                with path.open("rb") as handle:
                    sample = handle.read(8192)
            except OSError:
                continue

            try:
                file_sha256: str | None = hash_file_sha256(path)
            except OSError:
                file_sha256 = None

            rows.append(
                {
                    "file_id": stable_id("file", "extraction_orchestrator", rel_path),
                    "path": rel_path,
                    "abs_path": str(path.resolve()),
                    "size_bytes": int(stat_result.st_size),
                    "mtime_ns": int(stat_result.st_mtime_ns),
                    "file_sha256": file_sha256,
                    "encoding": detect_encoding(sample, default="utf-8"),
                }
            )

    repo_files_schema = pa.schema(
        [
            pa.field("file_id", pa.string()),
            pa.field("path", pa.string()),
            pa.field("abs_path", pa.string()),
            pa.field("size_bytes", pa.int64()),
            pa.field("mtime_ns", pa.int64()),
            pa.field("file_sha256", pa.string()),
            pa.field("encoding", pa.string()),
        ]
    )
    repo_files = pa.Table.from_pylist(rows, schema=repo_files_schema)
    if repo_files.num_rows == 0:
        msg = "repo_scan_fallback found no Python source files"
        raise ValueError(msg)
    return {"repo_files_v1": repo_files}


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
    from extract.session import ExtractSession
    from extraction.engine_session_factory import build_engine_session
    from extraction.runtime_profile import resolve_runtime_profile

    # Build session properly
    runtime_spec = resolve_runtime_profile("default")
    engine_session = build_engine_session(runtime_spec=runtime_spec)
    extract_session = ExtractSession(engine_session=engine_session)

    extractors: dict[str, Callable[[], pa.Table]] = {
        "ast_files": lambda: _extract_ast(repo_files, extract_session, runtime_spec),
        "libcst_files": lambda: _extract_cst(repo_files, extract_session, runtime_spec),
        "bytecode_files_v1": lambda: _extract_bytecode(repo_files, extract_session, runtime_spec),
        "symtable_files_v1": lambda: _extract_symtable(repo_files, extract_session, runtime_spec),
    }

    if tree_sitter_enabled:
        extractors["tree_sitter_files"] = lambda: _extract_tree_sitter(
            repo_files, extract_session, runtime_spec
        )

    if scip_index_config is not None:
        extractors["scip_index"] = lambda: _extract_scip(
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
        AST files table.

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

    ast_files = outputs.get("ast_files")
    if ast_files is not None:
        return _coerce_to_table(ast_files)

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
        LibCST files table.

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

    cst_files = outputs.get("libcst_files")
    if cst_files is not None:
        return _coerce_to_table(cst_files)

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
        Tree-sitter files table.

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

    ts_files = outputs.get("tree_sitter_files")
    if ts_files is not None:
        return _coerce_to_table(ts_files)

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
        SCIP index table.

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

    # Extract scip_index_path from config (new canonical field with fallback)
    scip_index_path = getattr(scip_index_config, "index_path_override", None)
    if scip_index_path is None:
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

    scip_index = outputs.get("scip_index")
    if scip_index is None:
        msg = "scip extraction produced no scip_index output"
        raise ValueError(msg)

    return _coerce_to_table(scip_index)


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
    from extract.extractors.imports_extract import extract_python_imports_tables
    from extract.session import ExtractSession
    from extraction.engine_session_factory import build_engine_session
    from extraction.runtime_profile import resolve_runtime_profile

    # Load inputs from Delta locations (adapter-normalized dataset keys)
    ast_imports = _load_delta_table(delta_locations.get("ast_files"))
    cst_imports = _load_delta_table(delta_locations.get("libcst_files"))
    ts_imports = _load_delta_table(delta_locations.get("tree_sitter_files"))

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
    from extract.extractors.external_scope import extract_python_external_tables
    from extract.session import ExtractSession
    from extraction.engine_session_factory import build_engine_session
    from extraction.runtime_profile import resolve_runtime_profile

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

    python_external = outputs.get("python_external_interfaces")
    if python_external is None:
        msg = "python_external extraction produced no python_external_interfaces output"
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
