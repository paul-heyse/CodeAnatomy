"""Validation helpers for Rust UDF extension capabilities."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence

from datafusion import SessionContext

from datafusion_engine.extensions.required_entrypoints import REQUIRED_RUNTIME_ENTRYPOINTS
from datafusion_engine.udf.constants import (
    ABI_VERSION_MISMATCH_MSG,
    REBUILD_WHEELS_HINT,
)
from datafusion_engine.udf.extension_core import (
    expected_plugin_abi,
    extension_module_with_capabilities,
    invoke_runtime_entrypoint,
)
from utils.validation import validate_required_items

_REQUIRED_SNAPSHOT_KEYS: tuple[str, ...] = (
    "scalar",
    "aggregate",
    "window",
    "table",
    "aliases",
    "parameter_names",
    "volatility",
    "simplify",
    "coerce_types",
    "short_circuits",
    "signature_inputs",
    "return_types",
)


def _mutable_mapping(payload: Mapping[str, object], key: str) -> dict[str, object]:
    mapping = payload.get(key)
    if isinstance(mapping, Mapping):
        return {str(name): value for name, value in mapping.items()}
    return {}


def _require_sequence(snapshot: Mapping[str, object], *, name: str) -> Sequence[object]:
    value = snapshot.get(name)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return value
    msg = f"Rust UDF snapshot field {name!r} must be a sequence."
    raise TypeError(msg)


def _require_mapping(snapshot: Mapping[str, object], *, name: str) -> Mapping[str, object]:
    value = snapshot.get(name)
    if isinstance(value, Mapping):
        return value
    msg = f"Rust UDF snapshot field {name!r} must be a mapping."
    raise TypeError(msg)


def _require_bool_mapping(snapshot: Mapping[str, object], *, name: str) -> Mapping[str, bool]:
    value = _require_mapping(snapshot, name=name)
    for key, flag in value.items():
        if not isinstance(flag, bool):
            msg = f"Rust UDF snapshot {name!r} entry {key!r} must be bool."
            raise TypeError(msg)
    return {str(key): bool(flag) for key, flag in value.items()}


def _coerce_config_default_value(field_value: object) -> bool | int | str:
    if isinstance(field_value, bool):
        return field_value
    if isinstance(field_value, int) and not isinstance(field_value, bool):
        return field_value
    if isinstance(field_value, str):
        return field_value
    if isinstance(field_value, Mapping) and len(field_value) == 1:
        tag, value = next(iter(field_value.items()))
        tag_text = str(tag).strip().lower()
        if tag_text in {"bool", "boolean"} and isinstance(value, bool):
            return value
        if (
            tag_text in {"int", "int32", "int64", "uint", "uint32", "uint64"}
            and isinstance(value, int)
            and not isinstance(value, bool)
        ):
            return value
        if tag_text in {"str", "string"} and isinstance(value, str):
            return value
    msg = "Rust UDF snapshot config_defaults entries must be bool, int, or str values."
    raise TypeError(msg)


def _require_config_defaults(snapshot: Mapping[str, object]) -> Mapping[str, Mapping[str, object]]:
    raw_defaults = snapshot.get("config_defaults")
    if raw_defaults is None:
        return {}
    if not isinstance(raw_defaults, Mapping):
        msg = "Rust UDF snapshot field 'config_defaults' must be a mapping."
        raise TypeError(msg)
    normalized: dict[str, dict[str, object]] = {}
    for section_name, section_payload in raw_defaults.items():
        if not isinstance(section_payload, Mapping):
            msg = "Rust UDF snapshot config_defaults sections must contain mapping payloads."
            raise TypeError(msg)
        section_dict: dict[str, object] = {}
        for field_name, field_value in section_payload.items():
            section_dict[str(field_name)] = _coerce_config_default_value(field_value)
        normalized[str(section_name)] = section_dict
    return normalized


def _snapshot_names(snapshot: Mapping[str, object]) -> frozenset[str]:
    names: set[str] = set()
    for key in ("scalar", "aggregate", "window", "table"):
        values = snapshot.get(key, [])
        if isinstance(values, Sequence) and not isinstance(values, (str, bytes, bytearray)):
            names.update(str(value) for value in values if value is not None)
    return frozenset(names)


def _alias_to_canonical(snapshot: Mapping[str, object]) -> dict[str, str]:
    aliases = _mutable_mapping(snapshot, "aliases")
    names = _snapshot_names(snapshot)
    canonical: dict[str, str] = {}
    for alias, target in aliases.items():
        alias_name = str(alias)
        target_name: str | None = None
        if isinstance(target, str):
            target_name = target
        elif isinstance(target, Sequence) and not isinstance(target, (str, bytes, bytearray)):
            values = [str(item) for item in target if item is not None]
            if values:
                target_name = values[0]
        if target_name is None:
            continue
        if target_name in names:
            canonical[alias_name] = target_name
    return canonical


def _iter_snapshot_values(values: object) -> set[str]:
    if isinstance(values, Iterable) and not isinstance(values, (str, bytes)):
        return {str(value) for value in values if value is not None}
    return set()


def _snapshot_alias_names(snapshot: Mapping[str, object]) -> set[str]:
    raw = snapshot.get("aliases")
    if not isinstance(raw, Mapping):
        return set()
    names: set[str] = set()
    for alias, target in raw.items():
        if alias is not None:
            names.add(str(alias))
        if target is None:
            continue
        if isinstance(target, str):
            names.add(target)
        elif isinstance(target, Sequence) and not isinstance(target, (str, bytes, bytearray)):
            names.update({str(value) for value in target if value is not None})
    return names


def _validate_required_snapshot_keys(snapshot: Mapping[str, object]) -> None:
    validate_required_items(
        _REQUIRED_SNAPSHOT_KEYS,
        snapshot,
        item_label="Rust UDF snapshot keys",
        error_type=ValueError,
    )


def _require_snapshot_metadata(
    snapshot: Mapping[str, object],
) -> tuple[
    Mapping[str, object],
    Mapping[str, object],
    Mapping[str, object],
    Mapping[str, object],
    frozenset[str],
]:
    _require_sequence(snapshot, name="scalar")
    _require_sequence(snapshot, name="aggregate")
    _require_sequence(snapshot, name="window")
    _require_sequence(snapshot, name="table")
    _require_mapping(snapshot, name="aliases")
    param_names = _require_mapping(snapshot, name="parameter_names")
    volatility = _require_mapping(snapshot, name="volatility")
    _require_bool_mapping(snapshot, name="simplify")
    _require_bool_mapping(snapshot, name="coerce_types")
    _require_bool_mapping(snapshot, name="short_circuits")
    _require_config_defaults(snapshot)
    signature_inputs = _require_mapping(snapshot, name="signature_inputs")
    return_types = _require_mapping(snapshot, name="return_types")
    names = _snapshot_names(snapshot)
    return param_names, volatility, signature_inputs, return_types, names


def _validate_udf_entries(
    snapshot: Mapping[str, object],
    *,
    list_name: str,
    param_names: Mapping[str, object],
    volatility: Mapping[str, object],
) -> None:
    for name in _require_sequence(snapshot, name=list_name):
        if not isinstance(name, str):
            msg = f"Rust UDF snapshot {list_name} entries must be strings."
            raise TypeError(msg)
        if name not in param_names:
            msg = f"Rust UDF snapshot missing parameter names for {name!r}."
            raise ValueError(msg)
        if name not in volatility:
            msg = f"Rust UDF snapshot missing volatility for {name!r}."
            raise ValueError(msg)


def _validate_signature_metadata(
    *,
    names: frozenset[str],
    signature_inputs: Mapping[str, object],
    return_types: Mapping[str, object],
) -> None:
    if names and not signature_inputs:
        msg = "Rust UDF snapshot missing signature_inputs entries."
        raise ValueError(msg)
    if names and not return_types:
        msg = "Rust UDF snapshot missing return_types entries."
        raise ValueError(msg)


def validate_extension_capabilities(
    *,
    strict: bool = True,
    ctx: SessionContext | None = None,
) -> Mapping[str, object]:
    """Validate extension capabilities for the runtime profile.

    Returns:
        Mapping[str, object]: Capability report payload.

    Raises:
        RuntimeError: If strict validation is enabled and compatibility or probe checks fail.
    """
    report = extension_capabilities_report()
    if strict and not report.get("compatible", False):
        msg = report.get("error") or "Extension ABI compatibility check failed."
        raise RuntimeError(msg)
    module = extension_module_with_capabilities()
    missing = [
        name for name in REQUIRED_RUNTIME_ENTRYPOINTS if not callable(getattr(module, name, None))
    ]
    if strict and missing:
        missing_csv = ", ".join(sorted(missing))
        msg = (
            "Required runtime entrypoints are missing from extension module: "
            f"{missing_csv}. {REBUILD_WHEELS_HINT}"
        )
        raise RuntimeError(msg)
    probe = getattr(module, "session_context_contract_probe", None) if module is not None else None
    if strict and module is not None and callable(probe):
        probe_ctx = ctx if ctx is not None else SessionContext()
        try:
            invoke_runtime_entrypoint(module, "session_context_contract_probe", ctx=probe_ctx)
        except (RuntimeError, TypeError, ValueError) as exc:
            expected = expected_plugin_abi()
            msg = (
                "SessionContext ABI mismatch detected by extension contract probe. "
                f"expected_plugin_abi={expected}. "
                f"{REBUILD_WHEELS_HINT}"
            )
            raise RuntimeError(msg) from exc
    return report


def extension_capabilities_report() -> Mapping[str, object]:
    """Return extension capability diagnostics report."""
    expected = expected_plugin_abi()
    try:
        snapshot = extension_capabilities_snapshot()
    except (ImportError, TypeError, RuntimeError, ValueError) as exc:
        return {
            "available": False,
            "compatible": False,
            "expected_plugin_abi": expected,
            "error": str(exc),
            "snapshot": None,
        }
    plugin_abi = snapshot.get("plugin_abi") if isinstance(snapshot, Mapping) else None
    major = None
    minor = None
    if isinstance(plugin_abi, Mapping):
        major = plugin_abi.get("major")
        minor = plugin_abi.get("minor")
    compatible = major == expected["major"] and minor == expected["minor"]
    error = None
    if not compatible:
        error = ABI_VERSION_MISMATCH_MSG.format(
            expected=expected,
            actual={"major": major, "minor": minor},
        )
    return {
        "available": True,
        "compatible": compatible,
        "expected_plugin_abi": expected,
        "observed_plugin_abi": {"major": major, "minor": minor},
        "snapshot": snapshot,
        "error": error,
    }


def extension_capabilities_snapshot() -> Mapping[str, object]:
    """Return the Rust extension capabilities snapshot when available.

    Raises:
        TypeError: If the extension capability snapshot is not a mapping payload.
    """
    from datafusion_engine.udf.extension_core import (
        EXTENSION_MODULE_LABEL,
    )

    module = extension_module_with_capabilities()
    snapshot = module.capabilities_snapshot()
    if isinstance(snapshot, Mapping):
        return dict(snapshot)
    msg = f"{EXTENSION_MODULE_LABEL}.capabilities_snapshot returned a non-mapping payload."
    raise TypeError(msg)


def snapshot_parameter_names(snapshot: Mapping[str, object]) -> dict[str, tuple[str, ...]]:
    """Return a mapping of UDF parameter names from a registry snapshot."""
    raw = snapshot.get("parameter_names")
    if not isinstance(raw, Mapping):
        return {}
    resolved: dict[str, tuple[str, ...]] = {}
    for name, params in raw.items():
        if name is None:
            continue
        if params is None or isinstance(params, str):
            continue
        if isinstance(params, Sequence) and not isinstance(params, (str, bytes, bytearray)):
            resolved[str(name)] = tuple(str(param) for param in params if param is not None)
    return resolved


def snapshot_return_types(snapshot: Mapping[str, object]) -> dict[str, tuple[str, ...]]:
    """Return a mapping of UDF return types from a registry snapshot."""
    raw = snapshot.get("return_types")
    if not isinstance(raw, Mapping):
        return {}
    resolved: dict[str, tuple[str, ...]] = {}
    for name, entries in raw.items():
        if name is None:
            continue
        if not isinstance(entries, Sequence) or isinstance(entries, (str, bytes, bytearray)):
            continue
        resolved[str(name)] = tuple(str(item) for item in entries if item is not None)
    return resolved


def snapshot_alias_mapping(snapshot: Mapping[str, object]) -> dict[str, str]:
    """Return alias-to-canonical mapping from a registry snapshot."""
    return _alias_to_canonical(snapshot)


def validate_rust_udf_snapshot(snapshot: Mapping[str, object]) -> None:
    """Validate structural requirements for a Rust UDF snapshot.

    Raises:
        TypeError: If snapshot field types are invalid.
        ValueError: If snapshot contents violate required metadata rules.
    """
    try:
        _validate_required_snapshot_keys(snapshot)
        param_names, volatility, signature_inputs, return_types, names = _require_snapshot_metadata(
            snapshot,
        )
        for list_name in ("scalar", "aggregate", "window"):
            _validate_udf_entries(
                snapshot,
                list_name=list_name,
                param_names=param_names,
                volatility=volatility,
            )
        _validate_signature_metadata(
            names=names,
            signature_inputs=signature_inputs,
            return_types=return_types,
        )
    except TypeError as exc:
        msg = f"Invalid Rust UDF snapshot types: {exc}"
        raise TypeError(msg) from exc
    except ValueError as exc:
        msg = f"Invalid Rust UDF snapshot values: {exc}"
        raise ValueError(msg) from exc


def validate_required_udfs(
    snapshot: Mapping[str, object],
    *,
    required: Sequence[str],
) -> None:
    """Validate required UDF names and metadata against a registry snapshot.

    Raises:
        ValueError: If required UDFs or their metadata are missing.
    """
    if not required:
        return
    names = _snapshot_names(snapshot)
    validate_required_items(
        required,
        names,
        item_label="Rust UDFs",
        error_type=ValueError,
    )
    aliases = _alias_to_canonical(snapshot)
    signature_inputs = _require_mapping(snapshot, name="signature_inputs")
    return_types = _require_mapping(snapshot, name="return_types")
    missing_signatures: list[str] = []
    missing_returns: list[str] = []
    for name in required:
        canonical = aliases.get(name, name)
        if canonical not in signature_inputs and name not in signature_inputs:
            missing_signatures.append(name)
        if canonical not in return_types and name not in return_types:
            missing_returns.append(name)
    if missing_signatures:
        msg = f"Missing Rust UDF signature metadata for: {sorted(missing_signatures)}."
        raise ValueError(msg)
    if missing_returns:
        msg = f"Missing Rust UDF return metadata for: {sorted(missing_returns)}."
        raise ValueError(msg)


def udf_names_from_snapshot(snapshot: Mapping[str, object]) -> frozenset[str]:
    """Return all UDF names derived from a registry snapshot."""
    validate_rust_udf_snapshot(snapshot)
    return _snapshot_names(snapshot)


def validate_runtime_capabilities(
    *,
    strict: bool = True,
    ctx: SessionContext | None = None,
) -> Mapping[str, object]:
    """Compatibility alias for runtime capability validation.

    Returns:
    -------
    Mapping[str, object]
        Capability report payload.
    """
    return validate_extension_capabilities(strict=strict, ctx=ctx)


def capability_report() -> Mapping[str, object]:
    """Compatibility alias for extension capability reports.

    Returns:
    -------
    Mapping[str, object]
        Capability report payload.
    """
    return extension_capabilities_report()


__all__ = [
    "capability_report",
    "extension_capabilities_report",
    "extension_capabilities_snapshot",
    "snapshot_alias_mapping",
    "snapshot_parameter_names",
    "snapshot_return_types",
    "udf_names_from_snapshot",
    "validate_extension_capabilities",
    "validate_required_udfs",
    "validate_runtime_capabilities",
    "validate_rust_udf_snapshot",
]
