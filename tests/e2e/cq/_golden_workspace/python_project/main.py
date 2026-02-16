# ruff: noqa: INP001
"""Entrypoint module for fixture workspace."""

from __future__ import annotations

from importlib import import_module


def run() -> None:
    """Run."""
    api_module = import_module("app.api")
    build_pipeline = api_module.build_pipeline
    registry, router, service = build_pipeline()
    _ = registry, router, service


if __name__ == "__main__":
    run()
