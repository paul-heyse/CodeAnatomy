"""Standalone extraction orchestrator.

Replace Hamilton DAG extraction with direct staged execution.
Each extractor runs as a plain Python function producing Arrow tables,
which are written to Delta storage.
"""

from __future__ import annotations

import importlib
import inspect
import logging
import time
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING

import msgspec

from extraction.contracts import (
    RunExtractionRequestV1,
    resolve_semantic_input_locations,
    with_compat_aliases,
)
from extraction.options import ExtractionRunOptions, normalize_extraction_options
from obs.otel import SCOPE_EXTRACT, record_error, record_stage_duration, stage_span

if TYPE_CHECKING:
    import pyarrow as pa
    from datafusion import SessionContext

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
    execution_bundle: _ExtractExecutionBundle
    scip_index_config: object | None
    scip_identity_overrides: object | None
    tree_sitter_enabled: bool
    max_workers: int


@dataclass(frozen=True)
class _ExtractExecutionBundle:
    runtime_spec: RuntimeProfileSpec
    extract_session: ExtractSession


@dataclass(frozen=True)
class _RepoFilesExtractorSpec:
    module_path: str
    function_name: str
    output_key: str | None
    error_label: str


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
    execution_bundle = _build_extract_execution_bundle()
    extraction_start = time.monotonic()
    repo_files = _run_repo_scan_with_fallback(
        repo_root=repo_root,
        extract_dir=extract_dir,
        execution_bundle=execution_bundle,
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
            execution_bundle=execution_bundle,
            scip_index_config=request.scip_index_config,
            scip_identity_overrides=request.scip_identity_overrides,
            tree_sitter_enabled=resolved_options.tree_sitter_enabled,
            max_workers=resolved_options.max_workers,
        ),
        state=state,
    )
    _run_python_imports_stage(
        extract_dir=extract_dir,
        execution_bundle=execution_bundle,
        state=state,
    )
    _run_python_external_stage(
        repo_root=repo_root,
        extract_dir=extract_dir,
        execution_bundle=execution_bundle,
        state=state,
    )
    _finalize_extraction_state(state=state, extraction_start=extraction_start)
    return _materialize_extraction_result(state)


def _materialize_extraction_result(state: _ExtractionRunState) -> ExtractionResult:
    return ExtractionResult(
        delta_locations=state.delta_locations,
        semantic_input_locations=state.semantic_input_locations,
        errors=state.errors,
        timing=state.timing,
    )


def _build_extract_execution_bundle(*, profile: str = "default") -> _ExtractExecutionBundle:
    """Build shared extraction session/runtime surfaces for a run.

    Returns:
        _ExtractExecutionBundle: Runtime profile and extract session bundle.
    """
    from extract.session import ExtractSession
    from extraction.engine_session_factory import build_engine_session
    from extraction.runtime_profile import resolve_runtime_profile

    runtime_spec = resolve_runtime_profile(profile)
    engine_session = build_engine_session(runtime_spec=runtime_spec)
    return _ExtractExecutionBundle(
        runtime_spec=runtime_spec,
        extract_session=ExtractSession(engine_session=engine_session),
    )


@cache
def _delta_write_ctx() -> SessionContext:
    """Return a cached native extraction session context for Delta writes."""
    from extraction.rust_session_bridge import build_extraction_session, extraction_session_payload

    return build_extraction_session(extraction_session_payload())


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
    execution_bundle: _ExtractExecutionBundle,
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
            scan_fn = _run_repo_scan
            if "execution_bundle" in inspect.signature(scan_fn).parameters:
                outputs = scan_fn(
                    repo_root,
                    options=options,
                    execution_bundle=execution_bundle,
                )
            else:
                outputs = scan_fn(
                    repo_root,
                    options=options,
                )
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
        execution_bundle=request.execution_bundle,
        scip_index_config=request.scip_index_config,
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


def _run_python_imports_stage(
    *,
    extract_dir: Path,
    execution_bundle: _ExtractExecutionBundle,
    state: _ExtractionRunState,
) -> None:
    t0 = time.monotonic()
    try:
        with stage_span(
            "extraction.python_imports",
            stage="extraction",
            scope_name=SCOPE_EXTRACT,
            attributes={"extractor": "python_imports"},
        ):
            imports_fn = _run_python_imports
            if "execution_bundle" in inspect.signature(imports_fn).parameters:
                python_imports = imports_fn(
                    state.delta_locations,
                    execution_bundle=execution_bundle,
                )
            else:
                python_imports = imports_fn(state.delta_locations)
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
    execution_bundle: _ExtractExecutionBundle,
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
            external_fn = _run_python_external
            if "execution_bundle" in inspect.signature(external_fn).parameters:
                python_external = external_fn(
                    state.delta_locations,
                    repo_root,
                    execution_bundle=execution_bundle,
                )
            else:
                python_external = external_fn(state.delta_locations, repo_root)
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
    from datafusion_engine.delta.transactions import write_transaction
    from datafusion_engine.delta.write_ipc_payload import (
        DeltaWriteRequestOptions,
        build_delta_write_request,
    )

    location.mkdir(parents=True, exist_ok=True)
    loc_str = str(location)
    request = build_delta_write_request(
        table_uri=loc_str,
        table=table,
        options=DeltaWriteRequestOptions(
            mode="overwrite",
            schema_mode="overwrite",
            storage_options=None,
        ),
    )
    write_transaction(_delta_write_ctx(), request=request)
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
    execution_bundle: _ExtractExecutionBundle | None = None,
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

    resolved_bundle = execution_bundle or _build_extract_execution_bundle()
    runtime_spec = resolved_bundle.runtime_spec
    extract_session = resolved_bundle.extract_session

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
    execution_bundle: _ExtractExecutionBundle,
    scip_index_config: object | None,
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
    tree_sitter_enabled
        Whether to enable tree-sitter extraction.

    Returns:
    -------
    dict[str, Callable[[], pa.Table]]
        Mapping of extractor name to callable.
    """
    runtime_spec = execution_bundle.runtime_spec
    extract_session = execution_bundle.extract_session

    extractors: dict[str, Callable[[], pa.Table]] = {
        "ast_files": lambda: _run_repo_files_output_extractor(
            repo_files=repo_files,
            extract_session=extract_session,
            runtime_spec=runtime_spec,
            spec=_RepoFilesExtractorSpec(
                module_path="extract.extractors.ast",
                function_name="extract_ast_tables",
                output_key="ast_files",
                error_label="ast",
            ),
        ),
        "libcst_files": lambda: _run_repo_files_output_extractor(
            repo_files=repo_files,
            extract_session=extract_session,
            runtime_spec=runtime_spec,
            spec=_RepoFilesExtractorSpec(
                module_path="extract.extractors.cst",
                function_name="extract_cst_tables",
                output_key="libcst_files",
                error_label="cst",
            ),
        ),
        "bytecode_files_v1": lambda: _run_repo_files_output_extractor(
            repo_files=repo_files,
            extract_session=extract_session,
            runtime_spec=runtime_spec,
            spec=_RepoFilesExtractorSpec(
                module_path="extract.extractors.bytecode",
                function_name="extract_bytecode_table",
                output_key=None,
                error_label="bytecode",
            ),
        ),
        "symtable_files_v1": lambda: _run_repo_files_output_extractor(
            repo_files=repo_files,
            extract_session=extract_session,
            runtime_spec=runtime_spec,
            spec=_RepoFilesExtractorSpec(
                module_path="extract.extractors.symtable_extract",
                function_name="extract_symtables_table",
                output_key=None,
                error_label="symtable",
            ),
        ),
    }

    if tree_sitter_enabled:
        extractors["tree_sitter_files"] = lambda: _run_repo_files_output_extractor(
            repo_files=repo_files,
            extract_session=extract_session,
            runtime_spec=runtime_spec,
            spec=_RepoFilesExtractorSpec(
                module_path="extract.extractors.tree_sitter",
                function_name="extract_ts_tables",
                output_key="tree_sitter_files",
                error_label="tree-sitter",
            ),
        )

    if scip_index_config is not None:
        extractors["scip_index"] = lambda: _extract_scip(
            repo_root,
            scip_index_config,
            extract_session,
            runtime_spec,
        )

    return extractors


def _run_repo_files_output_extractor(
    repo_files: pa.Table,
    extract_session: ExtractSession,
    runtime_spec: RuntimeProfileSpec,
    *,
    spec: _RepoFilesExtractorSpec,
) -> pa.Table:
    """Run a repo-files extractor and normalize its table output.

    Parameters
    ----------
    repo_files
        Repo files table.
    extract_session
        Extract session.
    runtime_spec
        Runtime profile specification.

    spec
        Import/function metadata for the extractor and optional output key.

    Returns:
    -------
    pa.Table
        Extractor output table.

    Raises:
        TypeError: If the extractor function is missing or output type is invalid.
        ValueError: If extractor output is missing or malformed.
    """
    module = importlib.import_module(spec.module_path)
    extractor = getattr(module, spec.function_name, None)
    if not callable(extractor):
        msg = f"{spec.module_path}.{spec.function_name} is not callable."
        raise TypeError(msg)
    result = extractor(
        repo_files=repo_files,
        session=extract_session,
        profile=runtime_spec.name,
        prefer_reader=False,
    )
    if spec.output_key is None:
        return _coerce_to_table(result)
    if not isinstance(result, Mapping):
        msg = f"{spec.error_label} extraction produced a non-mapping output payload."
        raise TypeError(msg)
    table = result.get(spec.output_key)
    if table is None:
        msg = f"{spec.error_label} extraction produced no {spec.output_key} output."
        raise ValueError(msg)
    return _coerce_to_table(table)


def _extract_scip(
    repo_root: Path,
    scip_index_config: object,
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


def _run_python_imports(
    delta_locations: dict[str, str],
    *,
    execution_bundle: _ExtractExecutionBundle | None = None,
) -> pa.Table:
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

    # Load inputs from Delta locations (adapter-normalized dataset keys)
    ast_imports = _load_delta_table(delta_locations.get("ast_files"))
    cst_imports = _load_delta_table(delta_locations.get("libcst_files"))
    ts_imports = _load_delta_table(delta_locations.get("tree_sitter_files"))

    resolved_bundle = execution_bundle or _build_extract_execution_bundle()
    runtime_spec = resolved_bundle.runtime_spec
    extract_session = resolved_bundle.extract_session

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


def _run_python_external(
    delta_locations: dict[str, str],
    repo_root: Path,
    *,
    execution_bundle: _ExtractExecutionBundle | None = None,
) -> pa.Table:
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

    # Load python_imports from Delta location
    python_imports_table = _load_delta_table(delta_locations.get("python_imports"))
    if python_imports_table is None:
        msg = "python_imports table is required for python_external extraction"
        raise ValueError(msg)

    resolved_bundle = execution_bundle or _build_extract_execution_bundle()
    runtime_spec = resolved_bundle.runtime_spec
    extract_session = resolved_bundle.extract_session

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
