"""Provide runtime stubs for datafusion_ext when Rust extensions are unavailable."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence

import msgspec
import pyarrow as pa
from datafusion import Expr, SessionContext, lit

IS_STUB: bool = True
_CACHE_FANOUT_THRESHOLD = 2


def plugin_library_path() -> str:
    """Return a placeholder plugin library path for stubs.

    Returns:
    -------
    str
        Empty string placeholder.
    """
    return ""


def plugin_manifest(path: str | None = None) -> dict[str, object]:
    """Return a stub plugin manifest payload.

    Returns:
    -------
    dict[str, object]
        Stub manifest payload.
    """
    _ = path
    return {"stub": True}


def _stub_expr(*values: object) -> Expr:
    """Return a placeholder DataFusion expression.

    Parameters
    ----------
    *values
        Values referenced to satisfy unused-argument checks.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    _ = values
    return lit(None)


class _StubPluginHandle:
    """Placeholder handle for DataFusion plugin stubs."""


def load_df_plugin(path: str) -> object:
    """Return a stub DataFusion plugin handle.

    Parameters
    ----------
    path
        Plugin library path.

    Returns:
    -------
    object
        Stub plugin handle instance.
    """
    _ = path
    return _StubPluginHandle()


def register_df_plugin_udfs(
    ctx: SessionContext,
    plugin: object,
    options_json: str | None = None,
) -> None:
    """Register stub plugin UDFs in a session context."""
    _ = (ctx, plugin, options_json)


def register_df_plugin_table_functions(ctx: SessionContext, plugin: object) -> None:
    """Register stub plugin table functions in a session context."""
    _ = (ctx, plugin)


def register_df_plugin_table_providers(
    ctx: SessionContext,
    plugin: object,
    table_names: object | None = None,
    options_json: object | None = None,
) -> None:
    """Register stub plugin table providers in a session context."""
    _ = (ctx, plugin, table_names, options_json)


def create_df_plugin_table_provider(
    plugin: object,
    provider_name: str,
    options_json: str | None = None,
) -> object:
    """Return a stub table provider capsule.

    Parameters
    ----------
    plugin
        Plugin handle placeholder.
    provider_name
        Provider name to register.
    options_json
        Optional provider options payload.

    Returns:
    -------
    object
        Stub table provider capsule.
    """
    _ = (plugin, provider_name, options_json)
    return object()


def register_df_plugin(
    ctx: SessionContext,
    plugin: object,
    table_names: object | None = None,
    options_json: object | None = None,
) -> None:
    """Register stub plugin UDFs and table providers."""
    _ = (ctx, plugin, table_names, options_json)


def install_function_factory(ctx: SessionContext, payload: bytes) -> None:
    """Install a stub FunctionFactory extension.

    Parameters
    ----------
    ctx
        DataFusion session context.
    payload
        Serialized policy payload.
    """
    _ = (ctx, payload)


def derive_function_factory_policy(
    snapshot: object,
    *,
    allow_async: bool = False,
) -> dict[str, object]:
    """Return a minimal policy payload for FunctionFactory derivation."""
    _ = snapshot
    return {
        "version": 1,
        "primitives": [],
        "prefer_named_arguments": True,
        "allow_async": bool(allow_async),
        "domain_operator_hooks": [],
    }


def udf_expr(name: str, *args: object, ctx: SessionContext | None = None) -> Expr:
    """Return a stub expression for udf_expr.

    Returns:
    -------
    Expr
        Stub expression instance.
    """
    _ = (name, ctx)
    return _stub_expr(*args)


def install_expr_planners(ctx: SessionContext, planners: object) -> None:
    """Install stub ExprPlanner extensions.

    Parameters
    ----------
    ctx
        DataFusion session context.
    planners
        Planner names or payload.
    """
    _ = (ctx, planners)


def install_planner_rules(ctx: SessionContext) -> None:
    """Install stub logical planner rules."""
    _ = ctx


def install_physical_rules(ctx: SessionContext) -> None:
    """Install stub physical optimizer rules."""
    _ = ctx


def delta_scan_config_from_session(ctx: SessionContext, *_args: object) -> dict[str, object]:
    """Return a stub Delta scan config payload.

    Returns:
    -------
    dict[str, object]
        Stub Delta scan config payload.
    """
    _ = ctx
    return {
        "scan_config_version": 1,
        "file_column_name": None,
        "enable_parquet_pushdown": False,
        "schema_force_view_types": False,
        "wrap_partition_values": False,
        "has_schema": False,
        "schema_ipc": None,
    }


def capabilities_snapshot() -> dict[str, object]:
    """Return a stub capabilities snapshot payload.

    Returns:
    -------
    dict[str, object]
        Stub capabilities snapshot payload.
    """
    return {
        "stub": True,
        "datafusion_version": None,
        "arrow_version": None,
        "plugin_abi": {"major": None, "minor": None},
        "udf_registry": {
            "scalar": 0,
            "aggregate": 0,
            "window": 0,
            "table": 0,
            "custom": 0,
            "hash": "",
        },
    }


def session_context_contract_probe(ctx: SessionContext) -> dict[str, object]:
    """Return a stub SessionContext contract probe payload."""
    _ = ctx
    return {
        "ok": True,
        "plugin_abi": {"major": 1, "minor": 1},
        "metadata_cache_limit_bytes": 0,
        "metadata_cache_entries": 0,
        "metadata_cache_hits": 0,
        "list_files_cache_entries": 0,
        "statistics_cache_entries": 0,
        "udf_registry": {
            "scalar": 0,
            "aggregate": 0,
            "window": 0,
            "table": 0,
            "custom": 0,
            "hash": "",
        },
    }


def derive_cache_policies(payload: Mapping[str, object]) -> dict[str, str]:
    """Return deterministic cache-policy decisions from a graph payload."""
    graph = payload.get("graph")
    if not isinstance(graph, Mapping):
        return {}
    out_degree_payload = graph.get("out_degree")
    if not isinstance(out_degree_payload, Mapping):
        return {}
    outputs_payload = payload.get("outputs")
    output_names = (
        {str(item) for item in outputs_payload}
        if isinstance(outputs_payload, (list, tuple))
        else set()
    )
    overrides_payload = payload.get("cache_overrides")
    overrides: dict[str, str] = (
        {str(key): str(value) for key, value in overrides_payload.items()}
        if isinstance(overrides_payload, Mapping)
        else {}
    )
    workload_raw = payload.get("workload_class")
    workload = str(workload_raw).strip().lower() if workload_raw is not None else None
    if not workload:
        workload = None
    policies: dict[str, str] = {}
    for task_name, raw_out_degree in out_degree_payload.items():
        name = str(task_name)
        try:
            out_degree = int(raw_out_degree)
        except (TypeError, ValueError):
            out_degree = 0
        is_output = name in output_names
        if is_output:
            policy_value = "delta_output"
        elif out_degree > _CACHE_FANOUT_THRESHOLD:
            policy_value = "delta_staging"
        elif out_degree == 0:
            policy_value = "none"
        else:
            policy_value = "delta_staging"
        if (
            workload == "interactive_query"
            and policy_value == "delta_staging"
            and out_degree <= 1
            and not is_output
        ):
            policy_value = "none"
        if workload == "compile_replay" and policy_value == "delta_staging" and not is_output:
            policy_value = "none"
        override = overrides.get(name)
        if override in {"none", "delta_staging", "delta_output"}:
            policy_value = override
        policies[name] = policy_value
    return policies


def interval_align_table(
    left: object,
    right: object,
    payload: Mapping[str, object] | None = None,
) -> object:
    """Interval-align bridge surface backed by DataFusion kernel helpers in stub mode.

    Returns:
    -------
    object
        Interval-aligned table-like payload.
    """
    from datafusion_engine.arrow.coercion import to_arrow_table

    def _int_value(value: object) -> int | None:
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            text = value.strip()
            if text and text.lstrip("-").isdigit():
                return int(text)
        return None

    payload_map = dict(payload or {})
    mode = str(payload_map.get("mode") or "CONTAINED_BEST").upper()
    how = str(payload_map.get("how") or "inner").lower()
    left_path_col = str(payload_map.get("left_path_col") or "path")
    left_start_col = str(payload_map.get("left_start_col") or "bstart")
    left_end_col = str(payload_map.get("left_end_col") or "bend")
    right_path_col = str(payload_map.get("right_path_col") or "path")
    right_start_col = str(payload_map.get("right_start_col") or "bstart")
    right_end_col = str(payload_map.get("right_end_col") or "bend")
    select_left_raw = payload_map.get("select_left")
    select_left = (
        tuple(str(value) for value in select_left_raw)
        if isinstance(select_left_raw, Sequence)
        and not isinstance(select_left_raw, (str, bytes, bytearray))
        else ()
    )
    select_right_raw = payload_map.get("select_right")
    select_right = (
        tuple(str(value) for value in select_right_raw)
        if isinstance(select_right_raw, Sequence)
        and not isinstance(select_right_raw, (str, bytes, bytearray))
        else ()
    )
    emit_match_meta = bool(payload_map.get("emit_match_meta", True))
    match_kind_col = str(payload_map.get("match_kind_col") or "match_kind")
    match_score_col = str(payload_map.get("match_score_col") or "match_score")
    right_suffix = str(payload_map.get("right_suffix") or "__r")
    tie_breakers_raw = payload_map.get("tie_breakers")
    tie_breakers = (
        list(tie_breakers_raw)
        if isinstance(tie_breakers_raw, Sequence)
        and not isinstance(tie_breakers_raw, (str, bytes, bytearray))
        else []
    )

    left_table = to_arrow_table(left)
    right_table = to_arrow_table(right)
    left_rows = left_table.to_pylist()
    right_rows = right_table.to_pylist()
    left_cols = list(left_table.column_names)
    right_cols = list(right_table.column_names)
    left_keep = list(select_left) if select_left else left_cols
    right_keep = list(select_right) if select_right else right_cols

    used = set(left_keep) | ({match_kind_col, match_score_col} if emit_match_meta else set())
    right_aliases: list[tuple[str, str]] = []
    for name in right_keep:
        alias = f"{name}{right_suffix}" if name in used else name
        while alias in used:
            alias = f"{alias}_r"
        used.add(alias)
        right_aliases.append((name, alias))

    output_rows: list[dict[str, object]] = []
    for left_row in left_rows:
        left_path = left_row.get(left_path_col)
        left_start = _int_value(left_row.get(left_start_col))
        left_end = _int_value(left_row.get(left_end_col))

        candidates: list[tuple[float, dict[str, object]]] = []
        for right_row in right_rows:
            if right_row.get(right_path_col) != left_path:
                continue
            right_start = _int_value(right_row.get(right_start_col))
            right_end = _int_value(right_row.get(right_end_col))
            if left_start is None or left_end is None or right_start is None or right_end is None:
                continue
            if mode == "EXACT":
                matched = right_start == left_start and right_end == left_end
            elif mode == "OVERLAP_BEST":
                matched = right_start < left_end and right_end > left_start
            else:
                matched = right_start <= left_start and right_end >= left_end
            if not matched:
                continue
            overlap = max(0, min(left_end, right_end) - max(left_start, right_start))
            score = float(overlap)
            for tie_index, tie in enumerate(tie_breakers):
                if not isinstance(tie, Mapping):
                    continue
                col_name = str(tie.get("column") or "")
                order = str(tie.get("order") or "ascending").lower()
                tie_value = right_row.get(col_name, left_row.get(col_name))
                if isinstance(tie_value, (int, float)):
                    if order == "descending":
                        score += float(tie_value) / (10 ** (tie_index + 6))
                    else:
                        score -= float(tie_value) / (10 ** (tie_index + 6))
            candidates.append((score, right_row))

        best = max(candidates, key=lambda item: item[0])[1] if candidates else None
        if best is None and how != "left":
            continue

        out_row: dict[str, object] = {}
        for name in left_keep:
            out_row[name] = left_row.get(name)
        for source, alias in right_aliases:
            out_row[alias] = None if best is None else best.get(source)
        if emit_match_meta:
            if best is None:
                if left_path is None or left_start is None or left_end is None:
                    out_row[match_kind_col] = "NO_PATH_OR_SPAN"
                else:
                    out_row[match_kind_col] = "NO_MATCH"
                out_row[match_score_col] = None
            else:
                out_row[match_kind_col] = mode
                out_row[match_score_col] = max(
                    0.0,
                    float(
                        min(_int_value(best.get(right_end_col)) or 0, left_end or 0)
                        - max(_int_value(best.get(right_start_col)) or 0, left_start or 0)
                    ),
                )
        output_rows.append(out_row)

    if not output_rows:
        names = [*left_keep, *(alias for _, alias in right_aliases)]
        if emit_match_meta:
            names.extend((match_kind_col, match_score_col))
        return pa.table({name: pa.array([], type=pa.null()) for name in names})
    return pa.table(output_rows)


def extract_tree_sitter_batch(
    source: str,
    file_path: str,
    payload: Mapping[str, object] | None = None,
) -> object:
    """Tree-sitter extraction bridge surface backed by local tree-sitter parsing.

    Returns:
    -------
    object
        Mapping payload containing extracted tree-sitter rows.
    """
    from tree_sitter_language_pack import get_parser

    from extract.coordination.context import SpanSpec, span_dict

    payload_map = dict(payload or {})
    parser = get_parser("python")
    source_bytes = source.encode("utf-8")
    tree = parser.parse(source_bytes)
    if tree is None:
        return {
            "repo": "",
            "path": file_path,
            "file_id": str(payload_map.get("file_id") or file_path),
            "file_sha256": (
                str(payload_map["file_sha256"])
                if payload_map.get("file_sha256") is not None
                else None
            ),
            "nodes": list[object](),
            "edges": list[object](),
            "errors": list[object](),
            "missing": list[object](),
            "captures": list[object](),
            "defs": list[object](),
            "calls": list[object](),
            "imports": list[object](),
            "docstrings": list[object](),
            "stats": None,
            "attrs": list[object](),
        }

    node_rows: list[dict[str, object]] = []
    def_rows: list[dict[str, object]] = []
    call_rows: list[dict[str, object]] = []
    import_rows: list[dict[str, object]] = []
    capture_rows: list[dict[str, object]] = []
    stack: list[object] = [tree.root_node]
    while stack:
        node = stack.pop()
        if node is None:
            continue
        node_uid = len(node_rows)
        bstart = int(getattr(node, "start_byte", 0))
        bend = int(getattr(node, "end_byte", bstart))
        kind = str(getattr(node, "type", "unknown"))
        node_id = f"{payload_map.get('file_id') or file_path}:{kind}:{bstart}:{bend}"
        node_rows.append(
            {
                "node_id": node_id,
                "node_uid": node_uid,
                "parent_id": None,
                "kind": kind,
                "kind_id": 0,
                "grammar_id": 0,
                "grammar_name": "python",
                "span": span_dict(
                    SpanSpec(
                        start_line0=None,
                        start_col=None,
                        end_line0=None,
                        end_col=None,
                        end_exclusive=True,
                        col_unit="byte",
                        byte_start=bstart,
                        byte_len=max(0, bend - bstart),
                    )
                ),
                "flags": {
                    "is_named": bool(getattr(node, "is_named", True)),
                    "has_error": bool(getattr(node, "has_error", False)),
                    "is_error": bool(getattr(node, "is_error", False)),
                    "is_missing": bool(getattr(node, "is_missing", False)),
                    "is_extra": bool(getattr(node, "is_extra", False)),
                    "has_changes": False,
                },
                "attrs": [],
            }
        )
        if kind in {"function_definition", "class_definition"}:
            name_node = getattr(node, "child_by_field_name", lambda *_args: None)("name")
            name: str | None = None
            if name_node is not None:
                nstart = int(getattr(name_node, "start_byte", 0))
                nend = int(getattr(name_node, "end_byte", nstart))
                name = source_bytes[nstart:nend].decode("utf-8", errors="replace")
            def_rows.append(
                {
                    "node_id": node_id,
                    "parent_id": None,
                    "kind": kind,
                    "name": name,
                    "span": node_rows[-1]["span"],
                    "attrs": [],
                }
            )
        if kind == "call":
            callee = getattr(node, "child_by_field_name", lambda *_args: None)("function")
            callee_kind = (
                str(getattr(callee, "type", "unknown")) if callee is not None else "unknown"
            )
            callee_start = (
                int(getattr(callee, "start_byte", bstart)) if callee is not None else bstart
            )
            callee_end = (
                int(getattr(callee, "end_byte", callee_start)) if callee is not None else bend
            )
            callee_text = source_bytes[callee_start:callee_end].decode("utf-8", errors="replace")
            call_rows.append(
                {
                    "node_id": node_id,
                    "parent_id": None,
                    "callee_kind": callee_kind,
                    "callee_text": callee_text,
                    "callee_node_id": f"{payload_map.get('file_id') or file_path}:{callee_kind}:{callee_start}:{callee_end}",
                    "span": node_rows[-1]["span"],
                    "attrs": [],
                }
            )
        if kind in {"import_statement", "import_from_statement"}:
            snippet = source_bytes[bstart:bend].decode("utf-8", errors="replace").strip()
            import_rows.append(
                {
                    "node_id": node_id,
                    "parent_id": None,
                    "kind": "ImportFrom" if kind == "import_from_statement" else "Import",
                    "module": None,
                    "name": snippet,
                    "asname": None,
                    "alias_index": 0,
                    "level": None,
                    "span": node_rows[-1]["span"],
                    "attrs": [],
                }
            )
        if def_rows and kind in {"function_definition", "class_definition"}:
            capture_rows.append(
                {
                    "capture_id": f"{node_id}:capture:def",
                    "query_name": "defs",
                    "capture_name": "def.node",
                    "pattern_index": 0,
                    "node_id": node_id,
                    "node_kind": kind,
                    "span": node_rows[-1]["span"],
                    "attrs": [],
                }
            )
        children = tuple(getattr(node, "children", ()) or ())
        stack.extend(reversed(children))

    return {
        "repo": "",
        "path": file_path,
        "file_id": str(payload_map.get("file_id") or file_path),
        "file_sha256": (
            str(payload_map["file_sha256"]) if payload_map.get("file_sha256") is not None else None
        ),
        "nodes": node_rows,
        "edges": list[object](),
        "errors": list[object](),
        "missing": list[object](),
        "captures": capture_rows,
        "defs": def_rows,
        "calls": call_rows,
        "imports": import_rows,
        "docstrings": list[object](),
        "stats": {
            "node_count": len(node_rows),
            "named_count": len(node_rows),
            "error_count": 0,
            "missing_count": 0,
            "parse_ms": 0,
            "parse_timed_out": False,
            "incremental_used": False,
            "query_match_count": len(def_rows) + len(call_rows) + len(import_rows),
            "query_capture_count": len(capture_rows),
            "match_limit_exceeded": False,
        },
        "attrs": [("language_name", "python"), ("language_abi_version", "stub")],
    }


def install_codeanatomy_runtime(
    ctx: SessionContext,
    *,
    enable_async_udfs: bool = False,
    async_udf_timeout_ms: int | None = None,
    async_udf_batch_size: int | None = None,
) -> dict[str, object]:
    """Return a stub runtime-install payload with registry snapshot."""
    _ = (enable_async_udfs, async_udf_timeout_ms, async_udf_batch_size)
    snapshot = registry_snapshot(ctx)
    return {
        "contract_version": 3,
        "runtime_install_mode": "unified",
        "snapshot_msgpack": registry_snapshot_msgpack(ctx),
        "snapshot": snapshot,
        "udf_installed": True,
        "function_factory_installed": True,
        "expr_planners_installed": True,
        "cache_registrar_available": True,
    }


def replay_substrait_plan(ctx: SessionContext, payload_bytes: bytes) -> object:
    """Replay Substrait bytes through public DataFusion Python APIs in stub mode.

    Returns:
    -------
    object
        DataFusion DataFrame-like object produced by public Substrait APIs.

    Raises:
        RuntimeError: If the local DataFusion build does not expose substrait support.
    """
    try:
        from datafusion.substrait import Consumer, Serde
    except ImportError as exc:
        msg = "Stub replay_substrait_plan requires datafusion.substrait support."
        raise RuntimeError(msg) from exc
    plan = Serde.deserialize_bytes(payload_bytes)
    return Consumer.from_substrait_plan(ctx, plan)


def lineage_from_substrait(payload_bytes: bytes) -> dict[str, object]:
    """Return a minimal lineage payload for a Substrait plan in stub mode."""
    _ = payload_bytes
    return {
        "scans": [],
        "required_columns_by_dataset": {},
        "filters": [],
        "referenced_tables": [],
        "required_udfs": [],
        "required_rewrite_tags": [],
    }


def extract_lineage_json(plan: object) -> str:
    """Return a minimal lineage JSON payload for a LogicalPlan in stub mode."""
    _ = plan
    payload: dict[str, object] = {
        "scans": [],
        "required_columns_by_dataset": {},
        "filters": [],
        "referenced_tables": [],
        "required_udfs": [],
        "required_rewrite_tags": [],
    }
    return json.dumps(payload, ensure_ascii=True)


def build_extraction_session(config_payload: object) -> SessionContext:
    """Return a default SessionContext for extraction in stub mode."""
    _ = config_payload
    return SessionContext()


def register_dataset_provider(ctx: SessionContext, request_payload: object) -> dict[str, object]:
    """Return a minimal dataset-provider registration payload in stub mode."""
    table_name = "dataset"
    if isinstance(request_payload, dict):
        table_name = str(request_payload.get("table_name", table_name))
    _ = ctx
    return {
        "table_name": table_name,
        "registered": False,
        "snapshot": {},
        "scan_config": delta_scan_config_from_session(ctx),
    }


def capture_plan_bundle_runtime(
    ctx: SessionContext,
    payload: object,
    df: object,
) -> dict[str, object]:
    """Return a minimal runtime capture payload for plan bundle bridge tests."""
    _ = (ctx, payload, df)
    return {"captured": True, "logical_plan": "", "optimized_plan": "", "physical_plan": ""}


def build_plan_bundle_artifact_with_warnings(
    ctx: SessionContext,
    payload: object,
    df: object,
) -> dict[str, object]:
    """Return a minimal plan-bundle artifact payload for bridge tests."""
    _ = (ctx, payload, df)
    return {"artifact": {}, "warnings": []}


def delta_write_ipc_request(ctx: SessionContext, request_msgpack: bytes) -> dict[str, object]:
    """Return a stub Delta write result payload."""
    _ = (ctx, request_msgpack)
    mutation_report = {
        "operation": "write",
        "version": 0,
        "mutation_version": 0,
        "num_added_files": 0,
        "num_removed_files": 0,
        "num_copied_rows": 0,
        "num_deleted_rows": 0,
        "num_inserted_rows": 0,
        "num_updated_rows": 0,
    }
    return {
        **mutation_report,
        "final_version": 0,
        "mutation_report": mutation_report,
        "enabled_features": {},
        "constraint_status": "skipped",
        "constraint_outcome": {"status": "skipped", "requested": []},
        "feature_outcomes": {"requested": [], "enabled": {}},
        "metadata_outcome": {"required": {}, "present": True},
    }


def delta_delete_request(ctx: SessionContext, request_msgpack: bytes) -> dict[str, object]:
    """Return a stub Delta delete result payload."""
    _ = (ctx, request_msgpack)
    return {
        "version": 0,
        "num_added_files": 0,
        "num_removed_files": 0,
        "num_copied_rows": 0,
        "num_deleted_rows": 0,
    }


def delta_update_request(ctx: SessionContext, request_msgpack: bytes) -> dict[str, object]:
    """Return a stub Delta update result payload."""
    _ = (ctx, request_msgpack)
    return {
        "version": 0,
        "num_added_files": 0,
        "num_removed_files": 0,
        "num_copied_rows": 0,
        "num_updated_rows": 0,
    }


def delta_merge_request(ctx: SessionContext, request_msgpack: bytes) -> dict[str, object]:
    """Return a stub Delta merge result payload."""
    _ = (ctx, request_msgpack)
    return {
        "version": 0,
        "num_target_rows_inserted": 0,
        "num_target_rows_updated": 0,
        "num_target_rows_deleted": 0,
        "num_target_rows_copied": 0,
    }


def delta_cdf_table_provider(ctx: SessionContext, *args: object) -> dict[str, object]:
    """Return a stub CDF provider payload."""
    _ = (ctx, args)
    return {"provider": None, "table_name": None}


def register_cache_tables(ctx: SessionContext, payload: object) -> None:
    """Register cache tables in stub mode."""
    _ = (ctx, payload)


def install_tracing(ctx: SessionContext) -> None:
    """Install tracing hooks in stub mode."""
    _ = ctx


def arrow_stream_to_batches(obj: object) -> object:
    """Return a stub Arrow stream conversion result.

    Returns:
    -------
    object
        Stub conversion result.
    """
    return obj


def install_delta_table_factory(ctx: SessionContext, name: str | None = None) -> None:
    """Install a stub Delta table factory.

    Parameters
    ----------
    ctx
        DataFusion session context.
    name
        Optional factory name override.
    """
    _ = (ctx, name)


def registry_snapshot(ctx: SessionContext) -> dict[str, object]:
    """Return an empty registry snapshot payload.

    Parameters
    ----------
    ctx
        DataFusion session context.

    Returns:
    -------
    dict[str, object]
        Snapshot mapping with required keys.
    """
    _ = ctx
    return {
        "scalar": [],
        "aggregate": [],
        "window": [],
        "table": [],
        "pycapsule_udfs": [],
        "aliases": {},
        "parameter_names": {},
        "volatility": {},
        "rewrite_tags": {},
        "signature_inputs": {},
        "return_types": {},
        "simplify": {},
        "coerce_types": {},
        "short_circuits": {},
        "custom_udfs": [],
    }


def registry_snapshot_msgpack(ctx: SessionContext) -> bytes:
    """Return msgpack bytes for the registry snapshot payload."""
    return msgspec.msgpack.encode(registry_snapshot(ctx))


def udf_docs_snapshot(ctx: SessionContext) -> dict[str, object]:
    """Return an empty UDF docs snapshot payload.

    Parameters
    ----------
    ctx
        DataFusion session context.

    Returns:
    -------
    dict[str, object]
        Empty docs snapshot mapping.
    """
    _ = ctx
    return {}


def map_entries(expr: Expr) -> Expr:
    """Return a stub expression for map_entries.

    Parameters
    ----------
    expr
        Input expression.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(expr)


def map_keys(expr: Expr) -> Expr:
    """Return a stub expression for map_keys.

    Parameters
    ----------
    expr
        Input expression.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(expr)


def map_values(expr: Expr) -> Expr:
    """Return a stub expression for map_values.

    Parameters
    ----------
    expr
        Input expression.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(expr)


def map_extract(expr: Expr, key: str) -> Expr:
    """Return a stub expression for map_extract.

    Parameters
    ----------
    expr
        Input expression.
    key
        Map key to extract.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(expr, key)


def list_extract(expr: Expr, index: int) -> Expr:
    """Return a stub expression for list_extract.

    Parameters
    ----------
    expr
        Input expression.
    index
        List index to extract.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(expr, index)


def list_unique(expr: Expr) -> Expr:
    """Return a stub expression for list_unique.

    Parameters
    ----------
    expr
        Input expression.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(expr)


def first_value_agg(expr: Expr) -> Expr:
    """Return a stub expression for first_value_agg.

    Parameters
    ----------
    expr
        Input expression.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(expr)


def last_value_agg(expr: Expr) -> Expr:
    """Return a stub expression for last_value_agg.

    Parameters
    ----------
    expr
        Input expression.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(expr)


def count_distinct_agg(expr: Expr) -> Expr:
    """Return a stub expression for count_distinct_agg.

    Parameters
    ----------
    expr
        Input expression.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(expr)


def string_agg(value: Expr, delimiter: Expr) -> Expr:
    """Return a stub expression for string_agg.

    Parameters
    ----------
    value
        Input expression to aggregate.
    delimiter
        Delimiter expression.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(value, delimiter)


def row_number_window(expr: Expr) -> Expr:
    """Return a stub expression for row_number_window.

    Parameters
    ----------
    expr
        Input expression.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(expr)


def lag_window(expr: Expr) -> Expr:
    """Return a stub expression for lag_window.

    Parameters
    ----------
    expr
        Input expression.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(expr)


def lead_window(expr: Expr) -> Expr:
    """Return a stub expression for lead_window.

    Parameters
    ----------
    expr
        Input expression.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(expr)


def arrow_metadata(expr: Expr, key: str | None = None) -> Expr:
    """Return a stub expression for arrow_metadata.

    Parameters
    ----------
    expr
        Input expression.
    key
        Optional metadata key.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(expr, key)


def union_tag(expr: Expr) -> Expr:
    """Return a stub expression for union_tag.

    Parameters
    ----------
    expr
        Input expression.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(expr)


def union_extract(expr: Expr, tag: str) -> Expr:
    """Return a stub expression for union_extract.

    Parameters
    ----------
    expr
        Input expression.
    tag
        Union tag to extract.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(expr, tag)


def stable_hash64(value: Expr) -> Expr:
    """Return a stub expression for stable_hash64.

    Parameters
    ----------
    value
        Input expression.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(value)


def stable_hash128(value: Expr) -> Expr:
    """Return a stub expression for stable_hash128.

    Parameters
    ----------
    value
        Input expression.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(value)


def prefixed_hash64(prefix: str, value: Expr) -> Expr:
    """Return a stub expression for prefixed_hash64.

    Parameters
    ----------
    prefix
        Hash prefix.
    value
        Input expression.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(prefix, value)


def stable_id(prefix: str, value: Expr) -> Expr:
    """Return a stub expression for stable_id.

    Parameters
    ----------
    prefix
        Identifier prefix.
    value
        Input expression.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(prefix, value)


def semantic_tag(semantic_type: str, value: Expr) -> Expr:
    """Return a stub expression for semantic_tag.

    Parameters
    ----------
    semantic_type
        Semantic type tag.
    value
        Input expression.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(semantic_type, value)


def stable_id_parts(prefix: str, part1: Expr, *parts: Expr) -> Expr:
    """Return a stub expression for stable_id_parts.

    Parameters
    ----------
    prefix
        Identifier prefix.
    part1
        First expression part.
    *parts
        Additional expression parts.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(prefix, part1, *parts)


def prefixed_hash_parts64(prefix: str, part1: Expr, *parts: Expr) -> Expr:
    """Return a stub expression for prefixed_hash_parts64.

    Parameters
    ----------
    prefix
        Hash prefix.
    part1
        First expression part.
    *parts
        Additional expression parts.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(prefix, part1, *parts)


def stable_hash_any(
    value: Expr,
    *,
    canonical: bool | None = None,
    null_sentinel: str | None = None,
) -> Expr:
    """Return a stub expression for stable_hash_any.

    Parameters
    ----------
    value
        Input expression.
    canonical
        Whether to apply canonicalization.
    null_sentinel
        Null sentinel value.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(value, canonical, null_sentinel)


def span_make(
    bstart: Expr,
    bend: Expr,
    line_base: Expr | None = None,
    col_unit: Expr | None = None,
    end_exclusive: Expr | None = None,
) -> Expr:
    """Return a stub expression for span_make.

    Parameters
    ----------
    bstart
        Byte start expression.
    bend
        Byte end expression.
    line_base
        Optional line-base expression.
    col_unit
        Optional column unit expression.
    end_exclusive
        Optional end-exclusive flag expression.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(bstart, bend, line_base, col_unit, end_exclusive)


def span_len(span: Expr) -> Expr:
    """Return a stub expression for span_len.

    Parameters
    ----------
    span
        Span expression.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(span)


def span_overlaps(span_a: Expr, span_b: Expr) -> Expr:
    """Return a stub expression for span_overlaps.

    Parameters
    ----------
    span_a
        First span expression.
    span_b
        Second span expression.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(span_a, span_b)


def span_contains(span_a: Expr, span_b: Expr) -> Expr:
    """Return a stub expression for span_contains.

    Parameters
    ----------
    span_a
        First span expression.
    span_b
        Second span expression.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(span_a, span_b)


def interval_align_score(
    left_start: Expr,
    left_end: Expr,
    right_start: Expr,
    right_end: Expr,
) -> Expr:
    """Return a stub expression for interval_align_score.

    Parameters
    ----------
    left_start
        Left interval start expression.
    left_end
        Left interval end expression.
    right_start
        Right interval start expression.
    right_end
        Right interval end expression.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(left_start, left_end, right_start, right_end)


def span_id(
    prefix: str,
    path: Expr,
    bstart: Expr,
    bend: Expr,
    *,
    kind: Expr | None = None,
) -> Expr:
    """Return a stub expression for span_id.

    Parameters
    ----------
    prefix
        Identifier prefix.
    path
        Path expression.
    bstart
        Byte start expression.
    bend
        Byte end expression.
    kind
        Optional kind expression.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(prefix, path, bstart, bend, kind)


def utf8_normalize(
    value: Expr,
    *,
    form: str | None = None,
    casefold: bool | None = None,
    collapse_ws: bool | None = None,
) -> Expr:
    """Return a stub expression for utf8_normalize.

    Parameters
    ----------
    value
        Input expression.
    form
        Unicode normalization form.
    casefold
        Whether to casefold.
    collapse_ws
        Whether to collapse whitespace.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(value, form, casefold, collapse_ws)


def utf8_null_if_blank(value: Expr) -> Expr:
    """Return a stub expression for utf8_null_if_blank.

    Parameters
    ----------
    value
        Input expression.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(value)


def qname_normalize(
    symbol: Expr,
    *,
    module: Expr | None = None,
    lang: Expr | None = None,
) -> Expr:
    """Return a stub expression for qname_normalize.

    Parameters
    ----------
    symbol
        Symbol expression.
    module
        Optional module expression.
    lang
        Optional language expression.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(symbol, module, lang)


def map_get_default(map_expr: Expr, key: str, default_value: Expr) -> Expr:
    """Return a stub expression for map_get_default.

    Parameters
    ----------
    map_expr
        Map expression.
    key
        Map key to look up.
    default_value
        Default value expression.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(map_expr, key, default_value)


def map_normalize(
    map_expr: Expr,
    *,
    key_case: str | None = None,
    sort_keys: bool | None = None,
) -> Expr:
    """Return a stub expression for map_normalize.

    Parameters
    ----------
    map_expr
        Map expression.
    key_case
        Optional key-case mode.
    sort_keys
        Whether to sort keys.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(map_expr, key_case, sort_keys)


def list_compact(list_expr: Expr) -> Expr:
    """Return a stub expression for list_compact.

    Parameters
    ----------
    list_expr
        List expression.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(list_expr)


def list_unique_sorted(list_expr: Expr) -> Expr:
    """Return a stub expression for list_unique_sorted.

    Parameters
    ----------
    list_expr
        List expression.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(list_expr)


def struct_pick(struct_expr: Expr, *fields: str) -> Expr:
    """Return a stub expression for struct_pick.

    Parameters
    ----------
    struct_expr
        Struct expression.
    *fields
        Field names to select.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(struct_expr, *fields)


def cdf_change_rank(change_type: Expr) -> Expr:
    """Return a stub expression for cdf_change_rank.

    Parameters
    ----------
    change_type
        Change type expression.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(change_type)


def cdf_is_upsert(change_type: Expr) -> Expr:
    """Return a stub expression for cdf_is_upsert.

    Parameters
    ----------
    change_type
        Change type expression.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(change_type)


def cdf_is_delete(change_type: Expr) -> Expr:
    """Return a stub expression for cdf_is_delete.

    Parameters
    ----------
    change_type
        Change type expression.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(change_type)


def col_to_byte(line_text: Expr, col_index: Expr, col_unit: Expr) -> Expr:
    """Return a stub expression for col_to_byte.

    Parameters
    ----------
    line_text
        Line text expression.
    col_index
        Column index expression.
    col_unit
        Column unit expression.

    Returns:
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(line_text, col_index, col_unit)


def canonicalize_byte_span(
    start_line_start_byte: Expr,
    start_line_text: Expr,
    start_col: Expr,
    end_line_start_byte: Expr,
    end_line_text: Expr,
    end_col: Expr,
    col_unit: Expr,
) -> Expr:
    """Return a stub expression for canonicalize_byte_span."""
    return _stub_expr(
        start_line_start_byte,
        start_line_text,
        start_col,
        end_line_start_byte,
        end_line_text,
        end_col,
        col_unit,
    )


__all__ = [
    "arrow_metadata",
    "arrow_stream_to_batches",
    "build_plan_bundle_artifact_with_warnings",
    "canonicalize_byte_span",
    "capabilities_snapshot",
    "capture_plan_bundle_runtime",
    "cdf_change_rank",
    "cdf_is_delete",
    "cdf_is_upsert",
    "col_to_byte",
    "count_distinct_agg",
    "create_df_plugin_table_provider",
    "delta_scan_config_from_session",
    "derive_function_factory_policy",
    "first_value_agg",
    "install_delta_table_factory",
    "install_expr_planners",
    "install_function_factory",
    "interval_align_score",
    "lag_window",
    "last_value_agg",
    "lead_window",
    "list_compact",
    "list_extract",
    "list_unique",
    "list_unique_sorted",
    "load_df_plugin",
    "map_entries",
    "map_extract",
    "map_get_default",
    "map_keys",
    "map_normalize",
    "map_values",
    "prefixed_hash64",
    "prefixed_hash_parts64",
    "qname_normalize",
    "register_df_plugin",
    "register_df_plugin_table_functions",
    "register_df_plugin_table_providers",
    "register_df_plugin_udfs",
    "registry_snapshot",
    "registry_snapshot_msgpack",
    "row_number_window",
    "semantic_tag",
    "span_contains",
    "span_id",
    "span_len",
    "span_make",
    "span_overlaps",
    "stable_hash64",
    "stable_hash128",
    "stable_hash_any",
    "stable_id",
    "stable_id_parts",
    "string_agg",
    "struct_pick",
    "udf_docs_snapshot",
    "udf_expr",
    "union_extract",
    "union_tag",
    "utf8_normalize",
    "utf8_null_if_blank",
]
