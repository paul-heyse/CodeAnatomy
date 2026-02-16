"""Test enforcement of module dependency direction and private import boundaries."""

from __future__ import annotations

import subprocess
from pathlib import Path


def test_no_private_cross_module_imports() -> None:
    """Verify no private cross-module imports exist except for allowlist.

    Private imports (underscore-prefixed) should stay within their module.
    Cross-module imports should use public APIs.

    Raises:
        RuntimeError: If the underlying ripgrep search command fails.
        AssertionError: If non-allowlisted private cross-module imports are found.
    """
    # Allowlist for legitimate private cross-module imports
    # These are imports that use underscore for aliasing, not for privacy
    allowlist = {
        # Aliasing shared utilities to avoid name collisions
        "tools/cq/search/python/extractors.py:37:from tools.cq.search._shared.core import source_hash as _shared_source_hash",
        "tools/cq/search/python/extractors.py:38:from tools.cq.search._shared.core import truncate as _shared_truncate",
        "tools/cq/search/rust/enrichment.py:16:from tools.cq.search._shared.core import sg_node_text as _shared_sg_node_text",
        "tools/cq/search/rust/enrichment.py:17:from tools.cq.search._shared.core import source_hash as _shared_source_hash",
        "tools/cq/search/tree_sitter/rust_lane/runtime.py:25:from tools.cq.search._shared.core import truncate as _shared_truncate",
        # Internal type imports within same subsystem (TYPE_CHECKING blocks)
        "tools/cq/search/python/resolution_index.py:194:    from tools.cq.search.python.analysis_session import PythonAnalysisSession as _Session",
        # Runtime query execution delegation (same subsystem)
        "tools/cq/search/tree_sitter/core/query_pack_executor.py:44:    from tools.cq.search.tree_sitter.core.runtime import run_bounded_query_captures as _impl",
        "tools/cq/search/tree_sitter/core/query_pack_executor.py:65:    from tools.cq.search.tree_sitter.core.runtime import run_bounded_query_matches as _impl",
        # Legacy index module constants (deprecated subsystem)
        "tools/cq/index/arg_binder.py:11:from tools.cq.index.def_index import _SELF_CLS, FnDecl, ParamInfo",
        "tools/cq/index/call_resolver.py:10:from tools.cq.index.def_index import _SELF_CLS, DefIndex, FnDecl",
        # Calls package decomposition (S28) - intra-package imports
        "tools/cq/macros/calls/entry.py:50:from tools.cq.macros.calls.scanning import _group_candidates, _rg_find_candidates",
        "tools/cq/macros/calls/entry.py:51:from tools.cq.macros.calls.semantic import CallsSemanticRequest, _apply_calls_semantic",
        "tools/cq/macros/calls/__init__.py:17:from tools.cq.macros.calls.insight import _find_function_signature",
        "tools/cq/macros/calls/__init__.py:19:from tools.cq.macros.calls.scanning import _rg_find_candidates, group_candidates, rg_find_candidates",
        "tools/cq/macros/calls/__init__.py:20:from tools.cq.macros.calls.semantic import _calls_payload_reason",
        "tools/cq/macros/calls/analysis.py:21:from tools.cq.macros.calls.context_snippet import _extract_context_snippet",
        "tools/cq/macros/calls/analysis.py:22:from tools.cq.macros.calls.neighborhood import _compute_context_window",
        # Report decomposition (S31) - intra-module re-exports
        "tools/cq/core/report.py:22:from tools.cq.core.render_overview import render_code_overview as _render_code_overview",
        # Search pipeline decomposition (S26) - intra-subsystem imports
        "tools/cq/search/pipeline/partition_pipeline.py:50:    from tools.cq.search.pipeline.smart_search_types import _PythonSemanticPrefetchResult",
        "tools/cq/search/pipeline/assembly.py:580:    from tools.cq.search.pipeline.smart_search import _build_search_summary, build_sections",
        "tools/cq/search/pipeline/search_semantic.py:19:from tools.cq.search.pipeline.smart_search_types import EnrichedMatch, _SearchSemanticOutcome",
        # Python extractor decomposition (S29) - intra-subsystem imports
        "tools/cq/search/python/extractors.py:58:from tools.cq.search.python.extractors_analysis import find_ast_function as _find_ast_function",
        "tools/cq/search/python/extractors_structure.py:15:from tools.cq.search.python.extractors_classification import _unwrap_decorated",
        # Rust package lazy proxies to avoid circular import (S30)
        "tools/cq/search/rust/__init__.py:29:    from tools.cq.search.rust.enrichment import enrich_context_by_byte_range as _fn",
        "tools/cq/search/rust/__init__.py:40:    from tools.cq.search.rust.enrichment import extract_rust_context as _fn",
        "tools/cq/search/rust/__init__.py:51:    from tools.cq.search.rust.enrichment import runtime_available as _fn",
        # Rust lane decomposition (S30) - intra-subsystem imports
        "tools/cq/search/tree_sitter/rust_lane/role_classification.py:12:from tools.cq.search.tree_sitter.rust_lane.runtime_cache import _rust_field_ids",
        "tools/cq/search/tree_sitter/rust_lane/runtime.py:75:from tools.cq.search.tree_sitter.rust_lane.query_cache import _pack_sources",
        "tools/cq/search/tree_sitter/rust_lane/runtime.py:76:from tools.cq.search.tree_sitter.rust_lane.role_classification import _classify_item_role",
        "tools/cq/search/tree_sitter/rust_lane/enrichment_extractors.py:11:from tools.cq.search._shared.core import truncate as _shared_truncate",
        "tools/cq/search/tree_sitter/rust_lane/enrichment_extractors.py:14:from tools.cq.search.tree_sitter.rust_lane.runtime_cache import _rust_field_ids",
    }

    repo_root = Path(__file__).parent.parent.parent.parent.parent
    cq_dir = repo_root / "tools" / "cq"
    result = subprocess.run(
        [
            "rg",
            "-n",
            r"from\s+tools\.cq\..+\s+import.*\s_[A-Za-z0-9_]+",
            str(cq_dir),
        ],
        capture_output=True,
        text=True,
        cwd=str(repo_root),
        check=False,
    )

    if result.returncode == 1:
        # No matches found - ideal case
        return

    if result.returncode != 0:
        msg = f"ripgrep search failed: {result.stderr}"
        raise RuntimeError(msg)

    # Parse matches and filter out allowlist
    # Normalize paths to be relative to repo root for comparison
    matches = result.stdout.strip().split("\n")
    violations: list[str] = []
    for match in matches:
        if not match:
            continue
        # Extract relative path by removing absolute prefix if present
        normalized = match
        if str(repo_root) in match:
            normalized = match.replace(str(repo_root) + "/", "")
        if normalized not in allowlist:
            violations.append(normalized)

    if violations:
        msg = (
            "Found private cross-module imports (not in allowlist):\n"
            + "\n".join(f"  {v}" for v in violations)
            + "\n\nPrivate imports should be made public or kept within the same module."
        )
        raise AssertionError(msg)


def test_no_lane_to_pipeline_upward_imports() -> None:
    """Verify lane modules don't import from pipeline layer.

    Lanes (python_lane, rust_lane) are lower-level components that should not
    depend on higher-level pipeline orchestration.

    Raises:
        AssertionError: If any lane module imports from the pipeline layer.
    """
    cq_dir = Path(__file__).parent.parent.parent.parent.parent / "tools" / "cq"
    lanes = [
        cq_dir / "search" / "tree_sitter" / "python_lane",
        cq_dir / "search" / "tree_sitter" / "rust_lane",
    ]

    violations: list[str] = []
    for lane_dir in lanes:
        if not lane_dir.exists():
            continue

        result = subprocess.run(
            [
                "rg",
                "-l",
                r"from\s+tools\.cq\.search\.pipeline",
                str(lane_dir),
            ],
            capture_output=True,
            text=True,
            check=False,
        )

        if result.returncode == 0:
            # Found upward imports
            files = result.stdout.strip().split("\n")
            violations.extend(file for file in files if file)

    if violations:
        msg = (
            "Found upward imports from lane modules to pipeline:\n"
            + "\n".join(f"  {v}" for v in violations)
            + "\n\nLanes should not depend on higher-level pipeline orchestration."
        )
        raise AssertionError(msg)
