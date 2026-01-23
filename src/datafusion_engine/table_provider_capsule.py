"""PyCapsule wrapper for DataFusion table providers."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TableProviderCapsule:
    """Expose a PyCapsule as a DataFusion table provider."""

    capsule: object

    def datafusion_table_provider(self) -> object:
        """Return the wrapped provider capsule.

        Returns
        -------
        object
            PyCapsule provider used by DataFusion.
        """
        return self.capsule


_TABLE_PROVIDER_ATTR = "__datafusion_table_provider__"
setattr(
    TableProviderCapsule,
    _TABLE_PROVIDER_ATTR,
    TableProviderCapsule.datafusion_table_provider,
)


__all__ = ["TableProviderCapsule"]
