# ruff: noqa: D103
"""Tests for semantic pipeline build module."""

from __future__ import annotations

from importlib import import_module

pipeline_build = import_module("semantics.pipeline_build")


def test_pipeline_build_exports_cpg_entrypoint() -> None:
    assert hasattr(pipeline_build, "build_cpg")
