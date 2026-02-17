"""Tests for CLI protocol contracts."""

from __future__ import annotations

import io
from dataclasses import dataclass
from typing import cast

from tools.cq.cli_app.protocols import ConsolePort


@dataclass
class _Console:
    file: io.StringIO
    printed: list[str]

    def print(self, *args: object, **kwargs: object) -> None:
        _ = kwargs
        self.printed.append(" ".join(str(arg) for arg in args))


def test_console_port_protocol_shape() -> None:
    """Validate the console protocol shape used by CLI render paths."""
    console: ConsolePort = cast("ConsolePort", _Console(file=io.StringIO(), printed=[]))

    console.print("hello")
    console.file.write("json")

    assert cast("io.StringIO", console.file).getvalue() == "json"
