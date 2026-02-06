"""Encoding policy dataclasses for schema metadata."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from core.config_base import FingerprintableConfig, config_fingerprint
from datafusion_engine.arrow.interop import DataTypeLike


@dataclass(frozen=True)
class EncodingSpec:
    """Column-level dictionary encoding specification."""

    column: str
    index_type: DataTypeLike | None = None
    ordered: bool | None = None


@dataclass(frozen=True)
class EncodingPolicy(FingerprintableConfig):
    """Dictionary encoding policy for schema alignment."""

    dictionary_cols: frozenset[str] = field(default_factory=frozenset)
    specs: tuple[EncodingSpec, ...] = ()
    dictionary_index_type: DataTypeLike | None = None
    dictionary_ordered: bool = False
    dictionary_index_types: dict[str, DataTypeLike] = field(default_factory=dict)
    dictionary_ordered_flags: dict[str, bool] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Normalize derived encoding fields from specs."""
        if not self.specs:
            return
        if not self.dictionary_cols:
            cols = frozenset(spec.column for spec in self.specs)
            object.__setattr__(self, "dictionary_cols", cols)
        if not self.dictionary_index_types:
            index_types = {
                spec.column: spec.index_type for spec in self.specs if spec.index_type is not None
            }
            object.__setattr__(self, "dictionary_index_types", index_types)
        if not self.dictionary_ordered_flags:
            ordered_flags = {
                spec.column: spec.ordered for spec in self.specs if spec.ordered is not None
            }
            object.__setattr__(self, "dictionary_ordered_flags", ordered_flags)

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for the encoding policy.

        Returns:
        -------
        Mapping[str, object]
            Payload describing encoding policy settings.
        """
        specs_payload = [
            {
                "column": spec.column,
                "index_type": str(spec.index_type) if spec.index_type is not None else None,
                "ordered": spec.ordered,
            }
            for spec in sorted(self.specs, key=lambda spec: spec.column)
        ]
        return {
            "dictionary_cols": tuple(sorted(self.dictionary_cols)),
            "specs": specs_payload,
            "dictionary_index_type": (
                str(self.dictionary_index_type) if self.dictionary_index_type is not None else None
            ),
            "dictionary_ordered": self.dictionary_ordered,
            "dictionary_index_types": {
                key: str(value) for key, value in sorted(self.dictionary_index_types.items())
            },
            "dictionary_ordered_flags": dict(sorted(self.dictionary_ordered_flags.items())),
        }

    def fingerprint(self) -> str:
        """Return fingerprint for the encoding policy.

        Returns:
        -------
        str
            Deterministic fingerprint for the policy.
        """
        return config_fingerprint(self.fingerprint_payload())


__all__ = ["EncodingPolicy", "EncodingSpec"]
