"""Tests for test_bootstrap_runtime_services."""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.bootstrap import (
    clear_runtime_services,
    resolve_runtime_services,
)
from tools.cq.core.contracts import CallsMacroRequestV1
from tools.cq.core.schema import RunMeta, mk_result
from tools.cq.core.services import (
    CallsServiceRequest,
    EntityFrontDoorRequest,
    SearchServiceRequest,
)
from tools.cq.core.toolchain import Toolchain


def test_runtime_services_are_workspace_scoped(tmp_path: Path) -> None:
    """Cache runtime services per workspace to enforce isolated service lifecycles."""
    clear_runtime_services()
    workspace_a = tmp_path / "workspace_a"
    workspace_b = tmp_path / "workspace_b"
    workspace_a.mkdir(parents=True, exist_ok=True)
    workspace_b.mkdir(parents=True, exist_ok=True)

    services_a1 = resolve_runtime_services(workspace_a)
    services_a2 = resolve_runtime_services(workspace_a)
    services_b = resolve_runtime_services(workspace_b)

    assert services_a1 is services_a2
    assert services_a1 is not services_b
    clear_runtime_services()


def test_service_request_contracts_are_constructible(tmp_path: Path) -> None:
    """Build runtime service request payloads for search and call workflows."""
    toolchain = Toolchain.detect()
    search = SearchServiceRequest(root=tmp_path, query="foo")
    calls = CallsServiceRequest(
        request=CallsMacroRequestV1(
            root=tmp_path,
            function_name="foo",
            tc=toolchain,
            argv=(),
        ),
    )
    run = RunMeta(
        macro="q",
        argv=[],
        root=str(tmp_path),
        started_ms=0.0,
        elapsed_ms=0.0,
        toolchain=toolchain.to_dict(),
    )
    entity = EntityFrontDoorRequest(result=mk_result(run))

    assert search.query == "foo"
    assert calls.request.function_name == "foo"
    assert entity.result is not None
