"""Test enforcement of module dependency direction and private import boundaries."""

from __future__ import annotations

import subprocess
from pathlib import Path


def test_no_private_cross_module_imports() -> None:
    """Verify no private cross-module imports exist except for allowlist.

    Private imports (underscore-prefixed) should stay within their module.
    Cross-module imports should use public APIs.
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
        "tools/cq/search/pipeline/partition_pipeline.py:535:        from tools.cq.search.pipeline.smart_search_types import _PythonSemanticPrefetchResult",
        # Runtime query execution delegation (same subsystem)
        "tools/cq/search/tree_sitter/core/query_pack_executor.py:44:    from tools.cq.search.tree_sitter.core.runtime import run_bounded_query_captures as _impl",
        "tools/cq/search/tree_sitter/core/query_pack_executor.py:65:    from tools.cq.search.tree_sitter.core.runtime import run_bounded_query_matches as _impl",
        # Legacy index module constants (deprecated subsystem)
        "tools/cq/index/arg_binder.py:11:from tools.cq.index.def_index import FnDecl, ParamInfo, _SELF_CLS",
        "tools/cq/index/call_resolver.py:10:from tools.cq.index.def_index import DefIndex, FnDecl, _SELF_CLS",
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
    violations = []
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
    """
    cq_dir = Path(__file__).parent.parent.parent.parent.parent / "tools" / "cq"
    lanes = [
        cq_dir / "search" / "tree_sitter" / "python_lane",
        cq_dir / "search" / "tree_sitter" / "rust_lane",
    ]

    violations = []
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
        )

        if result.returncode == 0:
            # Found upward imports
            files = result.stdout.strip().split("\n")
            for file in files:
                if file:
                    violations.append(file)

    if violations:
        msg = (
            "Found upward imports from lane modules to pipeline:\n"
            + "\n".join(f"  {v}" for v in violations)
            + "\n\nLanes should not depend on higher-level pipeline orchestration."
        )
        raise AssertionError(msg)
