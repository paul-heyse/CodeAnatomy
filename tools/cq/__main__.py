"""Allow running as python -m tools.cq."""

from __future__ import annotations

import sys

from tools.cq.cli import main

if __name__ == "__main__":
    sys.exit(main())
