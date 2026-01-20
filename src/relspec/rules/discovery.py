"""Rule bundle discovery helpers."""

from __future__ import annotations

from collections.abc import Sequence
from importlib import import_module, metadata

from relspec.rules.bundles import RuleBundle, clear_bundles, iter_bundles, register_bundle

_DEFAULT_MODULES: tuple[str, ...] = (
    "relspec.rules.relationship_specs",
    "relspec.rules.cpg_relationship_specs",
    "relspec.normalize.rule_registry_specs",
    "relspec.extract.registry_bundles",
)
_DISCOVERY_STATE: dict[str, bool] = {"discovered": False}
_ENTRYPOINT_GROUP = "codeanatomy.relspec_bundles"


def discover_bundles(modules: Sequence[str] | None = None) -> tuple[RuleBundle, ...]:
    """Import modules to register rule bundles.

    Parameters
    ----------
    modules
        Optional module list to import. When omitted, default bundle modules
        and entry points are loaded.

    Returns
    -------
    tuple[RuleBundle, ...]
        Registered bundles.
    """
    if modules is None and _DISCOVERY_STATE["discovered"]:
        return iter_bundles()
    for module in modules or _DEFAULT_MODULES:
        import_module(module)
    if modules is None:
        load_entrypoint_bundles()
        _DISCOVERY_STATE["discovered"] = True
    return iter_bundles()


def load_entrypoint_bundles(*, group: str = _ENTRYPOINT_GROUP) -> None:
    """Load bundles registered via entry points.

    Parameters
    ----------
    group
        Entry point group name for relspec bundles.

    Raises
    ------
    TypeError
        Raised when an entry point returns a non-bundle value.
    """
    for entry in metadata.entry_points(group=group):
        factory = entry.load()
        bundle = factory()
        if not isinstance(bundle, RuleBundle):
            msg = f"Entry point {entry.name!r} did not return a RuleBundle."
            raise TypeError(msg)
        register_bundle(bundle)


def reset_discovery() -> None:
    """Reset discovery state and clear registered bundles."""
    _DISCOVERY_STATE["discovered"] = False
    clear_bundles()


__all__ = ["discover_bundles", "load_entrypoint_bundles", "reset_discovery"]
