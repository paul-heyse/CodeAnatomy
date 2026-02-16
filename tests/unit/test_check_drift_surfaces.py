"""Tests for the AST-backed drift surface audit script."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from types import ModuleType


def _load_drift_module() -> tuple[ModuleType, Path]:
    script_path = Path(__file__).resolve().parents[2] / "scripts" / "check_drift_surfaces.py"
    spec = importlib.util.spec_from_file_location("check_drift_surfaces", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module, script_path


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _create_baseline_repo(repo_root: Path) -> None:
    _write(
        repo_root / "src/hamilton_pipeline/driver_factory.py",
        """
def build_view_graph_context() -> None:
    with compile_tracking(), resolver_identity_tracking():
        pass
""".strip()
        + "\n",
    )
    _write(
        repo_root / "src/datafusion_engine/plan/pipeline.py",
        """
def plan_with_delta_pins() -> None:
    with compile_tracking(), resolver_identity_tracking():
        pass
""".strip()
        + "\n",
    )
    _write(
        repo_root / "src/datafusion_engine/views/registration.py",
        """
def ensure_view_graph(dataset_resolver: object | None = None) -> None:
    record_resolver_if_tracking(dataset_resolver, label="view_registration")
""".strip()
        + "\n",
    )
    _write(
        repo_root / "src/datafusion_engine/dataset/resolution.py",
        """
def apply_scan_unit_overrides(dataset_resolver: object | None = None) -> None:
    record_resolver_if_tracking(dataset_resolver, label="scan_override")
""".strip()
        + "\n",
    )
    _write(
        repo_root / "src/datafusion_engine/delta/cdf.py",
        """
def register_cdf_inputs(dataset_resolver: object | None = None) -> None:
    record_resolver_if_tracking(dataset_resolver, label="cdf_registration")
""".strip()
        + "\n",
    )
    _write(
        repo_root / "src/datafusion_engine/session/runtime.py",
        """
def record_dataset_readiness(dataset_resolver: object | None = None) -> None:
    record_resolver_if_tracking(dataset_resolver, label="dataset_readiness")
""".strip()
        + "\n",
    )
    _write(
        repo_root / "src/semantics/pipeline.py",
        """
_CONSOLIDATED_BUILDER_HANDLERS = {
    "normalize": object(),
    "derive": object(),
    "relate": object(),
    "union": object(),
    "project": object(),
    "diagnostic": object(),
}
""".strip()
        + "\n",
    )
    _write(
        repo_root / "src/semantics/registry.py",
        """
from semantics.entity_registry import ENTITY_DECLARATIONS, generate_table_specs

SEMANTIC_TABLE_SPECS = generate_table_specs(ENTITY_DECLARATIONS)
""".strip()
        + "\n",
    )


def test_drift_audit_is_deterministic(tmp_path: Path) -> None:
    """Test drift audit is deterministic."""
    module, _ = _load_drift_module()
    _create_baseline_repo(tmp_path)

    report_one = module.run_audit(tmp_path)
    report_two = module.run_audit(tmp_path)

    assert report_one.to_dict() == report_two.to_dict()
    assert report_one.warnings == 0


def test_drift_audit_strict_mode_fails_on_injected_violation(tmp_path: Path) -> None:
    """Test drift audit strict mode fails on injected violation."""
    _, script_path = _load_drift_module()
    _create_baseline_repo(tmp_path)
    _write(
        tmp_path / "src/engine/violations.py",
        """
def emit(profile: object) -> None:
    profile.record_artifact("not_typed", {})
""".strip()
        + "\n",
    )

    result = subprocess.run(
        [sys.executable, str(script_path), "--strict", "--root", str(tmp_path)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert "FAIL:" in result.stdout


def test_drift_audit_ignores_comment_and_docstring_false_positives(tmp_path: Path) -> None:
    """Test drift audit ignores comment and docstring false positives."""
    module, script_path = _load_drift_module()
    _create_baseline_repo(tmp_path)
    _write(
        tmp_path / "src/engine/comment_only.py",
        '''
"""This file intentionally mentions profile.record_artifact("string", {}) in text only."""

# CompileContext(runtime_profile=profile) should not count from comments.
# canonical_output_name("x") should not count from comments.
'''.strip()
        + "\n",
    )

    report = module.run_audit(tmp_path)
    assert report.warnings == 0

    json_result = subprocess.run(
        [sys.executable, str(script_path), "--json", "--root", str(tmp_path)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert json_result.returncode == 0
    payload = json.loads(json_result.stdout)
    assert payload["summary"]["warnings"] == 0
