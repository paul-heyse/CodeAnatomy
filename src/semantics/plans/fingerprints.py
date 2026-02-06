"""Plan fingerprint computation for semantic views.

Provides portable plan fingerprints using Substrait when available, with
fallback to optimized logical plan text. Fingerprints enable caching and
diff-based debugging of semantic view execution plans.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from core.fingerprinting import CompositeFingerprint
from utils.hashing import hash_sha256_hex

if TYPE_CHECKING:
    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame


@dataclass(frozen=True)
class PlanFingerprint:
    """Fingerprint for a semantic view's execution plan.

    Attributes:
    ----------
    view_name
        Name of the semantic view.
    logical_plan_hash
        Hash of the optimized logical plan text.
    substrait_hash
        Hash of Substrait plan bytes (if available).
    schema_hash
        Hash of output schema for contract validation.
    """

    view_name: str
    logical_plan_hash: str
    substrait_hash: str | None = None
    schema_hash: str | None = None

    def matches(self, other: PlanFingerprint) -> bool:
        """Check if fingerprints match (for cache validation).

        Two fingerprints match when their logical plan hashes and schema
        hashes are identical. Substrait hashes are informational only.

        Parameters
        ----------
        other
            Another fingerprint to compare against.

        Returns:
        -------
        bool
            True if fingerprints represent equivalent plans.
        """
        return (
            self.logical_plan_hash == other.logical_plan_hash
            and self.schema_hash == other.schema_hash
        )

    def composite_fingerprint(self) -> CompositeFingerprint:
        """Return a composite fingerprint for cache keys and diagnostics.

        Returns:
        -------
        CompositeFingerprint
            Composite fingerprint for this plan.
        """
        components = {
            "view_name": self.view_name,
            "logical_plan_hash": self.logical_plan_hash,
        }
        if self.schema_hash:
            components["schema_hash"] = self.schema_hash
        if self.substrait_hash:
            components["substrait_hash"] = self.substrait_hash
        return CompositeFingerprint.from_components(1, **components)

    def cache_key(self, *, prefix: str = "plan_fingerprint") -> str:
        """Return a deterministic cache key derived from the composite fingerprint.

        Returns:
        -------
        str
            Deterministic cache key.
        """
        return self.composite_fingerprint().as_cache_key(prefix=prefix)


def _hash_bytes(data: bytes, *, length: int = 32) -> str:
    """Compute SHA256 hash of bytes, truncated for readability.

    Parameters
    ----------
    data
        Raw bytes to hash.
    length
        Number of hex characters to return.

    Returns:
    -------
    str
        Truncated hex digest.
    """
    return hash_sha256_hex(data, length=length)


def _hash_string(s: str, *, length: int = 32) -> str:
    """Compute SHA256 hash of a string.

    Parameters
    ----------
    s
        String to hash.
    length
        Number of hex characters to return.

    Returns:
    -------
    str
        Truncated hex digest.
    """
    return _hash_bytes(s.encode("utf-8"), length=length)


def _extract_field_type(schema: object, idx: int) -> str:
    """Extract field type string from a schema at a given index.

    Parameters
    ----------
    schema
        Schema object with field access.
    idx
        Field index.

    Returns:
    -------
    str
        Field type as string, or "unknown" if unavailable.
    """
    field_method = getattr(schema, "field", None)
    if not callable(field_method):
        return "unknown"
    try:
        field = field_method(idx)
        type_attr = getattr(field, "type", None)
        if callable(type_attr):
            return str(type_attr())
        return str(type_attr) if type_attr else "unknown"
    except (RuntimeError, TypeError, ValueError, IndexError):
        return "unknown"


def _schema_from_names(schema: object, names: list[object] | tuple[object, ...]) -> str:
    """Build schema string from field names.

    Parameters
    ----------
    schema
        Schema object with field access.
    names
        Field names.

    Returns:
    -------
    str
        Canonical schema representation.
    """
    parts: list[str] = []
    for idx, name in enumerate(names):
        field_type = _extract_field_type(schema, idx)
        parts.append(f"{name}:{field_type}")
    return ",".join(sorted(parts))


def _schema_to_string(schema: object) -> str:
    """Convert a schema object to a canonical string representation.

    Parameters
    ----------
    schema
        Schema object from DataFusion or PyArrow.

    Returns:
    -------
    str
        Canonical string representation.
    """
    # Try to_arrow() method for DataFusion schema
    to_arrow_method = getattr(schema, "to_arrow", None)
    if callable(to_arrow_method):
        try:
            arrow_schema = to_arrow_method()
            return str(arrow_schema)
        except (RuntimeError, TypeError, ValueError):
            pass

    # Try names property (DataFusion Schema has this as property, not method)
    names_attr = getattr(schema, "names", None)
    if names_attr is not None:
        names = names_attr() if callable(names_attr) else names_attr
        if isinstance(names, (list, tuple)):
            return _schema_from_names(schema, names)

    # Fallback to string representation
    return str(schema)


def _compute_schema_hash(df: DataFrame) -> str:
    """Compute hash from DataFrame schema.

    Uses schema field names and types for fingerprinting.

    Parameters
    ----------
    df
        DataFrame to extract schema from.

    Returns:
    -------
    str
        Schema hash string.
    """
    schema = df.schema()
    schema_repr = _schema_to_string(schema)
    return _hash_string(schema_repr)


def _compute_logical_plan_hash(df: DataFrame) -> str:
    """Compute hash from optimized logical plan.

    Prefers the optimized logical plan for determinism. Falls back
    to unoptimized plan if optimization is unavailable.

    Parameters
    ----------
    df
        DataFrame to extract plan from.

    Returns:
    -------
    str
        Plan hash string.
    """
    # Try optimized logical plan first (preferred)
    optimized_method = getattr(df, "optimized_logical_plan", None)
    if callable(optimized_method):
        try:
            plan = optimized_method()
            plan_str = _plan_to_string(plan)
            if plan_str:
                return _hash_string(plan_str)
        except (RuntimeError, TypeError, ValueError):
            pass

    # Fall back to unoptimized logical plan
    logical_method = getattr(df, "logical_plan", None)
    if callable(logical_method):
        try:
            plan = logical_method()
            plan_str = _plan_to_string(plan)
            if plan_str:
                return _hash_string(plan_str)
        except (RuntimeError, TypeError, ValueError):
            pass

    # Ultimate fallback
    return _hash_string("unknown_plan")


def _plan_to_string(plan: object) -> str | None:
    """Convert a plan object to a string representation.

    Parameters
    ----------
    plan
        Logical plan object from DataFusion.

    Returns:
    -------
    str | None
        String representation, or None if unavailable.
    """
    if plan is None:
        return None

    # Try display_indent_schema for most complete representation
    display_method = getattr(plan, "display_indent_schema", None)
    if callable(display_method):
        try:
            return str(display_method())
        except (RuntimeError, TypeError, ValueError):
            pass

    # Fall back to basic string conversion
    try:
        return str(plan)
    except (RuntimeError, TypeError, ValueError):
        return None


def _encode_substrait_bytes(plan_obj: object) -> bytes | None:
    encode_method = getattr(plan_obj, "encode", None)
    if not callable(encode_method):
        return None
    encoded = encode_method()
    if isinstance(encoded, (bytes, bytearray)):
        return bytes(encoded)
    return None


def _internal_substrait_bytes(
    ctx: SessionContext,
    optimized_plan: object,
) -> bytes | None:
    try:
        import datafusion as datafusion_module
    except ImportError:
        return None

    datafusion_internal = getattr(datafusion_module, "_internal", None)
    if datafusion_internal is None:
        return None

    internal_substrait = getattr(datafusion_internal, "substrait", None)
    internal_producer = getattr(internal_substrait, "Producer", None)
    to_substrait = getattr(internal_producer, "to_substrait_plan", None)
    if not callable(to_substrait):
        return None

    raw_plan = getattr(optimized_plan, "_raw_plan", optimized_plan)
    try:
        substrait_plan = to_substrait(raw_plan, ctx.ctx)
    except (RuntimeError, TypeError, ValueError, AttributeError):
        return None

    return _encode_substrait_bytes(substrait_plan)


def _public_substrait_bytes(
    ctx: SessionContext,
    optimized_plan: object,
) -> bytes | None:
    try:
        from datafusion.substrait import Producer as SubstraitProducer
    except ImportError:
        return None

    to_substrait = getattr(SubstraitProducer, "to_substrait_plan", None)
    if not callable(to_substrait):
        return None

    try:
        substrait_plan = to_substrait(optimized_plan, ctx)
    except (RuntimeError, TypeError, ValueError, AttributeError):
        return None

    return _encode_substrait_bytes(substrait_plan)


def _encode_substrait_plan(
    ctx: SessionContext,
    optimized_plan: object,
) -> bytes | None:
    """Encode an optimized logical plan to Substrait bytes.

    Parameters
    ----------
    ctx
        DataFusion session context for Substrait producer.
    optimized_plan
        Optimized logical plan to encode.

    Returns:
    -------
    bytes | None
        Encoded Substrait bytes, or None if encoding fails.
    """
    internal_bytes = _internal_substrait_bytes(ctx, optimized_plan)
    if internal_bytes is not None:
        return internal_bytes
    return _public_substrait_bytes(ctx, optimized_plan)


def _compute_substrait_hash(
    ctx: SessionContext,
    df: DataFrame,
) -> str | None:
    """Compute hash from Substrait plan if available.

    Substrait provides a portable plan representation suitable for
    cross-version comparisons and external tooling integration.

    Parameters
    ----------
    ctx
        DataFusion session context for Substrait producer.
    df
        DataFrame to serialize to Substrait.

    Returns:
    -------
    str | None
        Substrait hash, or None if unavailable.
    """
    optimized_method = getattr(df, "optimized_logical_plan", None)
    if not callable(optimized_method):
        return None

    try:
        optimized_plan = optimized_method()
        if optimized_plan is None:
            return None

        encoded = _encode_substrait_plan(ctx, optimized_plan)
        if encoded is not None:
            return _hash_bytes(encoded)
    except (RuntimeError, TypeError, ValueError, AttributeError):
        pass

    return None


def compute_plan_fingerprint(
    df: DataFrame,
    *,
    view_name: str,
    ctx: SessionContext | None = None,
) -> PlanFingerprint:
    """Compute fingerprint for a DataFrame's execution plan.

    Computes hashes from the optimized logical plan and schema. When
    a SessionContext is provided and Substrait is available, also
    computes a portable Substrait hash.

    Parameters
    ----------
    df
        DataFrame to fingerprint.
    view_name
        Name of the semantic view.
    ctx
        Optional SessionContext for Substrait serialization.

    Returns:
    -------
    PlanFingerprint
        Fingerprint with plan and schema hashes.
    """
    logical_plan_hash = _compute_logical_plan_hash(df)
    schema_hash = _compute_schema_hash(df)

    substrait_hash: str | None = None
    if ctx is not None:
        substrait_hash = _compute_substrait_hash(ctx, df)

    return PlanFingerprint(
        view_name=view_name,
        logical_plan_hash=logical_plan_hash,
        substrait_hash=substrait_hash,
        schema_hash=schema_hash,
    )


def fingerprints_match(a: PlanFingerprint, b: PlanFingerprint) -> bool:
    """Check if two fingerprints represent equivalent plans.

    Parameters
    ----------
    a
        First fingerprint.
    b
        Second fingerprint.

    Returns:
    -------
    bool
        True if fingerprints match.
    """
    return a.matches(b)


__all__ = [
    "PlanFingerprint",
    "compute_plan_fingerprint",
    "fingerprints_match",
]
