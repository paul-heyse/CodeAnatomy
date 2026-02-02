"""CQ CLI entry point.

This module provides backward-compatible entry point that delegates to cli_app/.
"""

from __future__ import annotations

from tools.cq.cli_app import main

if __name__ == "__main__":
    raise SystemExit(main())
