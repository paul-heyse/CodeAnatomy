"""Unified request factory for CLI, run engine, and bundles.

This module provides a canonical request construction layer to replace inline
request object instantiation across CQ command surfaces.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.core.structs import CqStruct
from tools.cq.core.toolchain import Toolchain
from tools.cq.search.pipeline.enrichment_contracts import IncrementalEnrichmentModeV1

if TYPE_CHECKING:
    from tools.cq.core.services import CallsServiceRequest, SearchServiceRequest
    from tools.cq.core.types import QueryLanguageScope
    from tools.cq.macros.bytecode import BytecodeSurfaceRequest
    from tools.cq.macros.exceptions import ExceptionsRequest
    from tools.cq.macros.impact import ImpactRequest
    from tools.cq.macros.imports import ImportRequest
    from tools.cq.macros.scopes import ScopeRequest
    from tools.cq.macros.side_effects import SideEffectsRequest
    from tools.cq.macros.sig_impact import SigImpactRequest
    from tools.cq.search._shared.types import QueryMode, SearchLimits


class RequestContextV1(CqStruct, frozen=True):
    """Shared context for request construction.

    Parameters
    ----------
    root : Path
        Repository root path.
    argv : list[str]
        Command-line arguments for audit trail.
    tc : Toolchain
        Detected toolchain capabilities.
    """

    root: Path
    argv: list[str]
    tc: Toolchain


class SearchRequestOptionsV1(CqStruct, frozen=True):
    """Options for SearchServiceRequest construction."""

    mode: QueryMode | None = None
    lang_scope: QueryLanguageScope = "auto"
    include_globs: list[str] | None = None
    exclude_globs: list[str] | None = None
    include_strings: bool = False
    with_neighborhood: bool = False
    limits: SearchLimits | None = None
    run_id: str | None = None
    incremental_enrichment_enabled: bool = True
    incremental_enrichment_mode: IncrementalEnrichmentModeV1 = IncrementalEnrichmentModeV1.TS_SYM


class RequestFactory:
    """Canonical request builder for CLI, run engine, and bundles.

    All factory methods use lazy imports to avoid circular dependencies.
    Request objects are constructed with consistent parameter ordering and
    naming conventions.
    """

    @staticmethod
    def calls(
        ctx: RequestContextV1,
        function_name: str,
    ) -> CallsServiceRequest:
        """Build CallsServiceRequest.

        Parameters
        ----------
        ctx : RequestContextV1
            Request context.
        function_name : str
            Target function name.

        Returns:
        -------
        CallsServiceRequest
            Request object for calls service.
        """
        from tools.cq.core.services import CallsServiceRequest
        from tools.cq.macros.contracts import CallsRequest

        return CallsServiceRequest(
            request=CallsRequest(
                tc=ctx.tc,
                root=ctx.root,
                argv=ctx.argv,
                function_name=function_name,
            ),
        )

    @staticmethod
    def search(
        ctx: RequestContextV1,
        query: str,
        *,
        options: SearchRequestOptionsV1 | None = None,
    ) -> SearchServiceRequest:
        """Build SearchServiceRequest.

        Parameters
        ----------
        ctx : RequestContextV1
            Request context.
        query : str
            Search query string.
        options : SearchRequestOptionsV1 | None, optional
            Optional request options; defaults to canonical search defaults.

        Returns:
        -------
        SearchServiceRequest
            Request object for search service.
        """
        from tools.cq.core.services import SearchServiceRequest

        opts = options if options is not None else SearchRequestOptionsV1()
        return SearchServiceRequest(
            root=ctx.root,
            query=query,
            mode=opts.mode,
            lang_scope=opts.lang_scope,
            include_globs=opts.include_globs,
            exclude_globs=opts.exclude_globs,
            include_strings=opts.include_strings,
            with_neighborhood=opts.with_neighborhood,
            limits=opts.limits,
            tc=ctx.tc,
            argv=ctx.argv,
            run_id=opts.run_id,
            incremental_enrichment_enabled=opts.incremental_enrichment_enabled,
            incremental_enrichment_mode=opts.incremental_enrichment_mode,
        )

    @staticmethod
    def impact(
        ctx: RequestContextV1,
        function_name: str,
        param_name: str,
        *,
        max_depth: int = 5,
    ) -> ImpactRequest:
        """Build ImpactRequest.

        Parameters
        ----------
        ctx : RequestContextV1
            Request context.
        function_name : str
            Target function name.
        param_name : str
            Parameter to trace.
        max_depth : int, optional
            Maximum taint propagation depth.

        Returns:
        -------
        ImpactRequest
            Request object for impact macro.
        """
        from tools.cq.macros.impact import ImpactRequest

        return ImpactRequest(
            tc=ctx.tc,
            root=ctx.root,
            argv=ctx.argv,
            function_name=function_name,
            param_name=param_name,
            max_depth=max_depth,
        )

    @staticmethod
    def sig_impact(
        ctx: RequestContextV1,
        symbol: str,
        to: str,
    ) -> SigImpactRequest:
        """Build SigImpactRequest.

        Parameters
        ----------
        ctx : RequestContextV1
            Request context.
        symbol : str
            Target function symbol.
        to : str
            New signature string.

        Returns:
        -------
        SigImpactRequest
            Request object for signature impact macro.
        """
        from tools.cq.macros.sig_impact import SigImpactRequest

        return SigImpactRequest(
            tc=ctx.tc,
            root=ctx.root,
            argv=ctx.argv,
            symbol=symbol,
            to=to,
        )

    @staticmethod
    def imports_cmd(
        ctx: RequestContextV1,
        *,
        cycles: bool = False,
        module: str | None = None,
        include: list[str] | None = None,
        exclude: list[str] | None = None,
    ) -> ImportRequest:
        """Build ImportRequest.

        Parameters
        ----------
        ctx : RequestContextV1
            Request context.
        cycles : bool, optional
            Whether to detect import cycles.
        module : str | None, optional
            Filter to specific module.
        include : list[str] | None, optional
            Include patterns.
        exclude : list[str] | None, optional
            Exclude patterns.

        Returns:
        -------
        ImportRequest
            Request object for imports macro.
        """
        from tools.cq.macros.imports import ImportRequest

        return ImportRequest(
            tc=ctx.tc,
            root=ctx.root,
            argv=ctx.argv,
            cycles=cycles,
            module=module,
            include=include or [],
            exclude=exclude or [],
        )

    @staticmethod
    def exceptions(
        ctx: RequestContextV1,
        *,
        function: str | None = None,
        include: list[str] | None = None,
        exclude: list[str] | None = None,
    ) -> ExceptionsRequest:
        """Build ExceptionsRequest.

        Parameters
        ----------
        ctx : RequestContextV1
            Request context.
        function : str | None, optional
            Filter to specific function.
        include : list[str] | None, optional
            Include patterns.
        exclude : list[str] | None, optional
            Exclude patterns.

        Returns:
        -------
        ExceptionsRequest
            Request object for exceptions macro.
        """
        from tools.cq.macros.exceptions import ExceptionsRequest

        return ExceptionsRequest(
            tc=ctx.tc,
            root=ctx.root,
            argv=ctx.argv,
            function=function,
            include=include or [],
            exclude=exclude or [],
        )

    @staticmethod
    def side_effects(
        ctx: RequestContextV1,
        *,
        max_files: int = 500,
        include: list[str] | None = None,
        exclude: list[str] | None = None,
    ) -> SideEffectsRequest:
        """Build SideEffectsRequest.

        Parameters
        ----------
        ctx : RequestContextV1
            Request context.
        max_files : int, optional
            Maximum files to analyze.
        include : list[str] | None, optional
            Include patterns.
        exclude : list[str] | None, optional
            Exclude patterns.

        Returns:
        -------
        SideEffectsRequest
            Request object for side effects macro.
        """
        from tools.cq.macros.side_effects import SideEffectsRequest

        return SideEffectsRequest(
            tc=ctx.tc,
            root=ctx.root,
            argv=ctx.argv,
            max_files=max_files,
            include=include or [],
            exclude=exclude or [],
        )

    @staticmethod
    def scopes(
        ctx: RequestContextV1,
        target: str,
        *,
        max_files: int = 500,
    ) -> ScopeRequest:
        """Build ScopeRequest.

        Parameters
        ----------
        ctx : RequestContextV1
            Request context.
        target : str
            Target file path or symbol.
        max_files : int, optional
            Maximum files to analyze.

        Returns:
        -------
        ScopeRequest
            Request object for scopes macro.
        """
        from tools.cq.macros.scopes import ScopeRequest

        return ScopeRequest(
            tc=ctx.tc,
            root=ctx.root,
            argv=ctx.argv,
            target=target,
            max_files=max_files,
        )

    @staticmethod
    def bytecode_surface(
        ctx: RequestContextV1,
        target: str,
        *,
        show: str = "globals,attrs,constants",
        max_files: int = 500,
    ) -> BytecodeSurfaceRequest:
        """Build BytecodeSurfaceRequest.

        Parameters
        ----------
        ctx : RequestContextV1
            Request context.
        target : str
            Target file path or symbol.
        show : str, optional
            Comma-separated list of surfaces to show.
        max_files : int, optional
            Maximum files to analyze.

        Returns:
        -------
        BytecodeSurfaceRequest
            Request object for bytecode surface macro.
        """
        from tools.cq.macros.bytecode import BytecodeSurfaceRequest

        return BytecodeSurfaceRequest(
            tc=ctx.tc,
            root=ctx.root,
            argv=ctx.argv,
            target=target,
            show=show,
            max_files=max_files,
        )


__all__ = [
    "RequestContextV1",
    "RequestFactory",
    "SearchRequestOptionsV1",
]
