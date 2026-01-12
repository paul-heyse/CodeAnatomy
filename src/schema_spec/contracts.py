"""Contract specification models."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, ValidationInfo, field_validator

from arrowdsl.contracts import Contract, DedupeSpec, SortKey
from schema_spec.core import TableSchemaSpec


class SortKeySpec(BaseModel):
    """Sort key specification."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    column: str
    order: Literal["ascending", "descending"] = "ascending"

    def to_sort_key(self) -> SortKey:
        """Convert to a SortKey instance.

        Returns
        -------
        SortKey
            SortKey with the configured column and order.
        """
        return SortKey(column=self.column, order=self.order)


class DedupeSpecSpec(BaseModel):
    """Dedupe specification."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    keys: tuple[str, ...]
    tie_breakers: tuple[SortKeySpec, ...] = ()
    strategy: Literal[
        "KEEP_FIRST_AFTER_SORT",
        "KEEP_BEST_BY_SCORE",
        "COLLAPSE_LIST",
        "KEEP_ARBITRARY",
    ] = "KEEP_FIRST_AFTER_SORT"

    def to_dedupe_spec(self) -> DedupeSpec:
        """Convert to a DedupeSpec instance.

        Returns
        -------
        DedupeSpec
            DedupeSpec with translated tie-breakers.
        """
        return DedupeSpec(
            keys=self.keys,
            tie_breakers=tuple(tb.to_sort_key() for tb in self.tie_breakers),
            strategy=self.strategy,
        )


class ContractSpec(BaseModel):
    """Output contract specification."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    table_schema: TableSchemaSpec

    dedupe: DedupeSpecSpec | None = None
    canonical_sort: tuple[SortKeySpec, ...] = ()
    version: int | None = None

    virtual_fields: tuple[str, ...] = ()
    virtual_field_docs: dict[str, str] | None = None

    @field_validator("virtual_field_docs")
    @classmethod
    def _docs_match_virtual_fields(
        cls,
        value: dict[str, str] | None,
        info: ValidationInfo,
    ) -> dict[str, str] | None:
        if value is None:
            return None
        virtual_fields: tuple[str, ...] = info.data.get("virtual_fields", ())
        missing = [key for key in value if key not in virtual_fields]
        if missing:
            msg = f"virtual_field_docs keys missing in virtual_fields: {missing}"
            raise ValueError(msg)
        return value

    def to_contract(self) -> Contract:
        """Convert to a runtime Contract.

        Returns
        -------
        Contract
            Contract constructed from the spec.
        """
        version = self.version if self.version is not None else self.table_schema.version
        return Contract(
            name=self.name,
            schema=self.table_schema.to_arrow_schema(),
            schema_spec=self.table_schema,
            key_fields=self.table_schema.key_fields,
            required_non_null=self.table_schema.required_non_null,
            dedupe=self.dedupe.to_dedupe_spec() if self.dedupe is not None else None,
            canonical_sort=tuple(sk.to_sort_key() for sk in self.canonical_sort),
            version=version,
            virtual_fields=self.virtual_fields,
            virtual_field_docs=self.virtual_field_docs,
        )
