"""Application service wrapper for calls macro execution."""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.schema import CqResult
from tools.cq.core.structs import CqStruct
from tools.cq.core.toolchain import Toolchain
from tools.cq.macros.calls import cmd_calls


class CallsServiceRequest(CqStruct, frozen=True):
    """Typed request contract for calls macro service execution."""

    root: Path
    function_name: str
    tc: Toolchain
    argv: list[str]


class CallsService:
    """Application-layer service for CQ calls macro."""

    @staticmethod
    def execute(request: CallsServiceRequest) -> CqResult:
        """Execute CQ calls macro.

        Returns:
            Calls macro result payload.
        """
        return cmd_calls(
            tc=request.tc,
            root=request.root,
            argv=request.argv,
            function_name=request.function_name,
        )


__all__ = ["CallsService", "CallsServiceRequest"]
