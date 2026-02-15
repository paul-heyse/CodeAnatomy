"""Tests for request factory."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.core.request_factory import (
    RequestContextV1,
    RequestFactory,
    SearchRequestOptionsV1,
)
from tools.cq.core.toolchain import Toolchain


@pytest.fixture
def mock_toolchain() -> Toolchain:
    """Create a mock toolchain for testing.

    Returns:
    -------
    Toolchain
        Mock toolchain with minimal capabilities.
    """
    return Toolchain(
        rg_available=True,
        rg_version="14.0.0",
        sgpy_available=True,
        sgpy_version="0.18.0",
        msgspec_available=True,
        msgspec_version="0.18.0",
        diskcache_available=True,
        diskcache_version="5.6.3",
        py_path="/usr/bin/python",
        py_version="3.13.12",
    )


@pytest.fixture
def request_context(mock_toolchain: Toolchain, tmp_path: Path) -> RequestContextV1:
    """Create a request context for testing.

    Parameters
    ----------
    mock_toolchain : Toolchain
        Mock toolchain fixture.
    tmp_path : Path
        Pytest tmp_path fixture.

    Returns:
    -------
    RequestContextV1
        Test request context.
    """
    return RequestContextV1(
        root=tmp_path,
        argv=["cq", "test"],
        tc=mock_toolchain,
    )


def test_request_context_construction(mock_toolchain: Toolchain, tmp_path: Path) -> None:
    """Test RequestContextV1 construction.

    Parameters
    ----------
    mock_toolchain : Toolchain
        Mock toolchain fixture.
    tmp_path : Path
        Pytest tmp_path fixture.
    """
    ctx = RequestContextV1(
        root=tmp_path,
        argv=["cq", "search", "test"],
        tc=mock_toolchain,
    )
    assert ctx.root == tmp_path
    assert ctx.argv == ["cq", "search", "test"]
    assert ctx.tc == mock_toolchain


def test_calls_request(request_context: RequestContextV1) -> None:
    """Test CallsServiceRequest factory method.

    Parameters
    ----------
    request_context : RequestContextV1
        Test request context.
    """
    from tools.cq.core.services import CallsServiceRequest

    request = RequestFactory.calls(request_context, function_name="test_function")
    assert isinstance(request, CallsServiceRequest)
    assert request.root == request_context.root
    assert request.function_name == "test_function"
    assert request.tc == request_context.tc
    assert request.argv == request_context.argv


def test_search_request(request_context: RequestContextV1) -> None:
    """Test SearchServiceRequest factory method.

    Parameters
    ----------
    request_context : RequestContextV1
        Test request context.
    """
    from tools.cq.core.services import SearchServiceRequest

    request = RequestFactory.search(
        request_context,
        query="test_query",
        options=SearchRequestOptionsV1(
            mode=None,
            lang_scope="python",
            include_strings=True,
        ),
    )
    assert isinstance(request, SearchServiceRequest)
    assert request.root == request_context.root
    assert request.query == "test_query"
    assert request.mode is None
    assert request.lang_scope == "python"
    assert request.include_strings is True
    assert request.tc == request_context.tc
    assert request.argv == request_context.argv


def test_impact_request(request_context: RequestContextV1) -> None:
    """Test ImpactRequest factory method.

    Parameters
    ----------
    request_context : RequestContextV1
        Test request context.
    """
    from tools.cq.macros.impact import ImpactRequest

    request = RequestFactory.impact(
        request_context,
        function_name="test_function",
        param_name="test_param",
        max_depth=10,
    )
    assert isinstance(request, ImpactRequest)
    assert request.tc == request_context.tc
    assert request.root == request_context.root
    assert request.argv == request_context.argv
    assert request.function_name == "test_function"
    assert request.param_name == "test_param"
    assert request.max_depth == 10


def test_sig_impact_request(request_context: RequestContextV1) -> None:
    """Test SigImpactRequest factory method.

    Parameters
    ----------
    request_context : RequestContextV1
        Test request context.
    """
    from tools.cq.macros.sig_impact import SigImpactRequest

    request = RequestFactory.sig_impact(
        request_context,
        symbol="test_function",
        to="test_function(a, b, c)",
    )
    assert isinstance(request, SigImpactRequest)
    assert request.tc == request_context.tc
    assert request.root == request_context.root
    assert request.argv == request_context.argv
    assert request.symbol == "test_function"
    assert request.to == "test_function(a, b, c)"


def test_imports_request(request_context: RequestContextV1) -> None:
    """Test ImportRequest factory method.

    Parameters
    ----------
    request_context : RequestContextV1
        Test request context.
    """
    from tools.cq.macros.imports import ImportRequest

    request = RequestFactory.imports_cmd(
        request_context,
        cycles=True,
        module="test_module",
    )
    assert isinstance(request, ImportRequest)
    assert request.tc == request_context.tc
    assert request.root == request_context.root
    assert request.argv == request_context.argv
    assert request.cycles is True
    assert request.module == "test_module"


def test_exceptions_request(request_context: RequestContextV1) -> None:
    """Test ExceptionsRequest factory method.

    Parameters
    ----------
    request_context : RequestContextV1
        Test request context.
    """
    from tools.cq.macros.exceptions import ExceptionsRequest

    request = RequestFactory.exceptions(
        request_context,
        function="test_function",
    )
    assert isinstance(request, ExceptionsRequest)
    assert request.tc == request_context.tc
    assert request.root == request_context.root
    assert request.argv == request_context.argv
    assert request.function == "test_function"


def test_side_effects_request(request_context: RequestContextV1) -> None:
    """Test SideEffectsRequest factory method.

    Parameters
    ----------
    request_context : RequestContextV1
        Test request context.
    """
    from tools.cq.macros.side_effects import SideEffectsRequest

    request = RequestFactory.side_effects(
        request_context,
        max_files=1000,
    )
    assert isinstance(request, SideEffectsRequest)
    assert request.tc == request_context.tc
    assert request.root == request_context.root
    assert request.argv == request_context.argv
    assert request.max_files == 1000


def test_scopes_request(request_context: RequestContextV1) -> None:
    """Test ScopeRequest factory method.

    Parameters
    ----------
    request_context : RequestContextV1
        Test request context.
    """
    from tools.cq.macros.scopes import ScopeRequest

    request = RequestFactory.scopes(
        request_context,
        target="test_target",
        max_files=1000,
    )
    assert isinstance(request, ScopeRequest)
    assert request.tc == request_context.tc
    assert request.root == request_context.root
    assert request.argv == request_context.argv
    assert request.target == "test_target"
    assert request.max_files == 1000


def test_bytecode_surface_request(request_context: RequestContextV1) -> None:
    """Test BytecodeSurfaceRequest factory method.

    Parameters
    ----------
    request_context : RequestContextV1
        Test request context.
    """
    from tools.cq.macros.bytecode import BytecodeSurfaceRequest

    request = RequestFactory.bytecode_surface(
        request_context,
        target="test_target",
        show="globals,attrs",
        max_files=1000,
    )
    assert isinstance(request, BytecodeSurfaceRequest)
    assert request.tc == request_context.tc
    assert request.root == request_context.root
    assert request.argv == request_context.argv
    assert request.target == "test_target"
    assert request.show == "globals,attrs"
    assert request.max_files == 1000
