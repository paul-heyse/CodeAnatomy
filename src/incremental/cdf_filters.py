"""Change data feed filtering policies."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion.expr import Expr


class CdfChangeType(Enum):
    """Delta CDF change operation types.

    Delta CDF tracks three types of changes:
    - INSERT: New rows added to the table
    - UPDATE_POSTIMAGE: Updated rows (post-image shows final state)
    - DELETE: Rows removed from the table

    Note: UPDATE_PREIMAGE is not included as we typically only need
    the final state of updated rows for incremental processing.
    """

    INSERT = "insert"
    UPDATE_POSTIMAGE = "update_postimage"
    DELETE = "delete"

    @classmethod
    def from_cdf_column(cls, value: str) -> CdfChangeType | None:
        """Convert CDF _change_type column value to enum.

        Parameters
        ----------
        value : str
            Value from the _change_type column in CDF output.

        Returns
        -------
        CdfChangeType | None
            Corresponding change type enum value, or None if unrecognized.
        """
        normalized = value.lower().strip()
        if normalized == "insert":
            return cls.INSERT
        if normalized in {"update_postimage", "update_post"}:
            return cls.UPDATE_POSTIMAGE
        if normalized == "delete":
            return cls.DELETE
        return None

    def to_cdf_column_value(self) -> str:
        """Convert enum to CDF _change_type column value.

        Returns
        -------
        str
            Value to match in the _change_type column.
        """
        return self.value


@dataclass(frozen=True)
class CdfFilterPolicy:
    """Policy for filtering Delta CDF changes by operation type.

    This policy determines which types of changes should be included
    when reading from a Delta table's change data feed.

    Attributes
    ----------
    include_insert : bool
        Whether to include INSERT operations.
    include_update_postimage : bool
        Whether to include UPDATE_POSTIMAGE operations.
    include_delete : bool
        Whether to include DELETE operations.
    """

    include_insert: bool = True
    include_update_postimage: bool = True
    include_delete: bool = True

    def matches(self, change_type: CdfChangeType) -> bool:
        """Check if a change type should be included per this policy.

        Parameters
        ----------
        change_type : CdfChangeType
            Change type to check.

        Returns
        -------
        bool
            True if the change type should be included.
        """
        if change_type == CdfChangeType.INSERT:
            return self.include_insert
        if change_type == CdfChangeType.UPDATE_POSTIMAGE:
            return self.include_update_postimage
        if change_type == CdfChangeType.DELETE:
            return self.include_delete
        return False

    def to_sql_predicate(self) -> str | None:
        """Generate SQL predicate for filtering CDF _change_type column.

        Returns
        -------
        str | None
            SQL WHERE clause predicate, or None if all types are included.
        """
        if self.include_insert and self.include_update_postimage and self.include_delete:
            return None

        included_types: list[str] = []
        if self.include_insert:
            included_types.append(f"'{CdfChangeType.INSERT.to_cdf_column_value()}'")
        if self.include_update_postimage:
            included_types.append(f"'{CdfChangeType.UPDATE_POSTIMAGE.to_cdf_column_value()}'")
        if self.include_delete:
            included_types.append(f"'{CdfChangeType.DELETE.to_cdf_column_value()}'")

        if not included_types:
            return "FALSE"

        if len(included_types) == 1:
            return f"_change_type = {included_types[0]}"

        return f"_change_type IN ({', '.join(included_types)})"

    def to_datafusion_predicate(self) -> Expr | None:
        """Return a DataFusion predicate for filtering CDF change types.

        Returns
        -------
        datafusion.expr.Expr | None
            Predicate to apply, or None when all change types are included.
        """
        if self.include_insert and self.include_update_postimage and self.include_delete:
            return None
        from datafusion import col, lit
        from datafusion import functions as f

        included_types: list[str] = []
        if self.include_insert:
            included_types.append(CdfChangeType.INSERT.to_cdf_column_value())
        if self.include_update_postimage:
            included_types.append(CdfChangeType.UPDATE_POSTIMAGE.to_cdf_column_value())
        if self.include_delete:
            included_types.append(CdfChangeType.DELETE.to_cdf_column_value())
        if not included_types:
            return lit(value=False)
        change_type_col = col("_change_type")
        if len(included_types) == 1:
            return change_type_col == lit(included_types[0])
        return f.in_list(change_type_col, [lit(value) for value in included_types])

    @classmethod
    def include_all(cls) -> CdfFilterPolicy:
        """Create a policy that includes all change types.

        Returns
        -------
        CdfFilterPolicy
            Policy including all change types.
        """
        return cls(include_insert=True, include_update_postimage=True, include_delete=True)

    @classmethod
    def inserts_and_updates_only(cls) -> CdfFilterPolicy:
        """Create a policy that excludes deletes.

        Returns
        -------
        CdfFilterPolicy
            Policy including only inserts and updates.
        """
        return cls(include_insert=True, include_update_postimage=True, include_delete=False)

    @classmethod
    def inserts_only(cls) -> CdfFilterPolicy:
        """Create a policy that includes only inserts.

        Returns
        -------
        CdfFilterPolicy
            Policy including only inserts.
        """
        return cls(include_insert=True, include_update_postimage=False, include_delete=False)


__all__ = ["CdfChangeType", "CdfFilterPolicy"]
