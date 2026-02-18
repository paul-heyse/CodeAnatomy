"""Calls macro command boundary.

This module intentionally exposes only the command entrypoint. Runtime
scan/analyze/assembly ownership lives in ``entry_runtime.py`` and
``entry_output.py``.
"""

from __future__ import annotations

from tools.cq.macros.calls.entry_command import cmd_calls

__all__ = ["cmd_calls"]
