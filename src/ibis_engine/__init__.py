"""Ibis execution engine helpers."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ibis_engine.backend import build_backend as build_backend
    from ibis_engine.config import IbisBackendConfig as IbisBackendConfig
    from ibis_engine.config import ObjectStoreConfig as ObjectStoreConfig
    from ibis_engine.param_tables import (
        ListParamSpec as ListParamSpec,
    )
    from ibis_engine.param_tables import (
        ParamTableArtifact as ParamTableArtifact,
    )
    from ibis_engine.param_tables import (
        ParamTablePolicy as ParamTablePolicy,
    )
    from ibis_engine.param_tables import (
        ParamTableRegistry as ParamTableRegistry,
    )
    from ibis_engine.param_tables import (
        ParamTableScope as ParamTableScope,
    )
    from ibis_engine.param_tables import (
        ParamTableSpec as ParamTableSpec,
    )
    from ibis_engine.plan import IbisPlan as IbisPlan

__all__ = [
    "IbisBackendConfig",
    "IbisPlan",
    "ListParamSpec",
    "ObjectStoreConfig",
    "ParamTableArtifact",
    "ParamTablePolicy",
    "ParamTableRegistry",
    "ParamTableScope",
    "ParamTableSpec",
    "build_backend",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "build_backend": ("ibis_engine.backend", "build_backend"),
    "IbisBackendConfig": ("ibis_engine.config", "IbisBackendConfig"),
    "ObjectStoreConfig": ("ibis_engine.config", "ObjectStoreConfig"),
    "IbisPlan": ("ibis_engine.plan", "IbisPlan"),
    "ListParamSpec": ("ibis_engine.param_tables", "ListParamSpec"),
    "ParamTableArtifact": ("ibis_engine.param_tables", "ParamTableArtifact"),
    "ParamTablePolicy": ("ibis_engine.param_tables", "ParamTablePolicy"),
    "ParamTableRegistry": ("ibis_engine.param_tables", "ParamTableRegistry"),
    "ParamTableScope": ("ibis_engine.param_tables", "ParamTableScope"),
    "ParamTableSpec": ("ibis_engine.param_tables", "ParamTableSpec"),
}


def __getattr__(name: str) -> object:
    if name in _EXPORT_MAP:
        import importlib

        module_name, attr_name = _EXPORT_MAP[name]
        module = importlib.import_module(module_name)
        return getattr(module, attr_name)
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
