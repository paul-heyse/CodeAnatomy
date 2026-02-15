"""Unified request factory for CLI, run engine, and bundles.

This module provides a canonical request construction layer to replace inline
request object instantiation across CQ command surfaces.
"""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.structs import CqStruct
from tools.cq.core.toolchain import Toolchain


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
    ) -> object:
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

        return CallsServiceRequest(
            root=ctx.root,
            function_name=function_name,
            tc=ctx.tc,
            argv=ctx.argv,
        )

    @staticmethod
    def search(
        ctx: RequestContextV1,
        query: str,
        *,
        mode: object = None,
        lang_scope: object = "auto",
        include_globs: list[str] | None = None,
        exclude_globs: list[str] | None = None,
        include_strings: bool = False,
        with_neighborhood: bool = False,
        limits: object = None,
        run_id: str | None = None,
    ) -> object:
        """Build SearchServiceRequest.

        Parameters
        ----------
        ctx : RequestContextV1
            Request context.
        query : str
            Search query string.
        mode : QueryMode | None, optional
            Query mode override.
        lang_scope : QueryLanguageScope, optional
            Language scope filter.
        include_globs : list[str] | None, optional
            Include glob patterns.
        exclude_globs : list[str] | None, optional
            Exclude glob patterns.
        include_strings : bool, optional
            Whether to include matches in strings/comments.
        with_neighborhood : bool, optional
            Whether to include neighborhood data.
        limits : SearchLimits | None, optional
            Search result limits.
        run_id : str | None, optional
            Run identifier for cache tagging.

        Returns:
        -------
        SearchServiceRequest
            Request object for search service.
        """
        from tools.cq.core.services import SearchServiceRequest

        return SearchServiceRequest(
            root=ctx.root,
            query=query,
            mode=mode,
            lang_scope=lang_scope,
            include_globs=include_globs,
            exclude_globs=exclude_globs,
            include_strings=include_strings,
            with_neighborhood=with_neighborhood,
            limits=limits,
            tc=ctx.tc,
            argv=ctx.argv,
            run_id=run_id,
        )

    @staticmethod
    def impact(
        ctx: RequestContextV1,
        function_name: str,
        param_name: str,
        *,
        max_depth: int = 5,
    ) -> object:
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
    ) -> object:
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
    ) -> object:
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
    ) -> object:
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
    ) -> object:
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
    ) -> object:
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
    ) -> object:
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
]
