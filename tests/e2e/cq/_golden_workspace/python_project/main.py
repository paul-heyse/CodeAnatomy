# ruff: noqa: INP001
"""Entrypoint module for fixture workspace."""

from __future__ import annotations

from app.api import build_pipeline


def run() -> None:
    registry, router, service = build_pipeline()
    _ = registry, router, service


if __name__ == "__main__":
    run()
