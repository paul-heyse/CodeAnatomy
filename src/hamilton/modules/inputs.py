from __future__ import annotations

from typing import Any, Dict, List, Optional

from hamilton.function_modifiers import tag

from ...arrowdsl.runtime import ExecutionContext


@tag(layer="inputs", kind="runtime")
def ctx() -> ExecutionContext:
    """
    ExecutionContext for Arrow DSL compilation/execution.
    """
    return ExecutionContext()


@tag(layer="inputs", kind="scalar")
def include_globs() -> List[str]:
    """
    Default include globs for repo scanning.
    Override via execute(overrides={"include_globs": [...]})
    """
    return ["**/*.py"]


@tag(layer="inputs", kind="scalar")
def exclude_globs() -> List[str]:
    """
    Default exclude globs for repo scanning.
    Override via execute(overrides={"exclude_globs": [...]})
    """
    return [
        "**/.git/**",
        "**/.venv/**",
        "**/venv/**",
        "**/__pycache__/**",
        "**/node_modules/**",
        "**/.mypy_cache/**",
        "**/.pytest_cache/**",
        "**/.ruff_cache/**",
        "**/build/**",
        "**/dist/**",
    ]


@tag(layer="inputs", kind="scalar")
def max_files() -> int:
    return 200_000


@tag(layer="inputs", kind="scalar")
def output_dir() -> Optional[str]:
    """
    Default: no output directory.
    Override to materialize artifacts.
    """
    return None


@tag(layer="inputs", kind="scalar")
def work_dir() -> Optional[str]:
    """
    Default: None (pipeline will choose a local working directory).
    Override if you want all intermediate datasets written somewhere specific.
    """
    return None


@tag(layer="inputs", kind="scalar")
def scip_index_path() -> Optional[str]:
    """
    Default: no SCIP input. Override to attach SCIP data.
    """
    return None


@tag(layer="inputs", kind="scalar")
def relspec_mode() -> str:
    """
    "memory" (default) or "filesystem".

    Override via execute(overrides={"relspec_mode": "filesystem"}).
    """
    return "memory"


@tag(layer="inputs", kind="scalar")
def overwrite_intermediate_datasets() -> bool:
    """
    If True, deletes and rewrites intermediate dataset directories for filesystem resolver mode.
    """
    return True


@tag(layer="inputs", kind="scalar")
def hamilton_tags() -> Dict[str, Any]:
    """
    Optional metadata tags for introspection, trackers, etc.
    """
    return {}
