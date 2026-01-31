"""Extraction row builder for consistent row construction.

This module provides standardized row builders for extraction tables, ensuring
consistent file identity, span, and metadata handling across all extractors.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    from datafusion_engine.arrow.interop import SchemaLike
    from extract.coordination.context import FileContext


@dataclass(frozen=True)
class SpanTemplateSpec:
    """Specification for span template parameters.

    Consolidates the many span parameters into a single spec object
    to reduce parameter count in methods.
    """

    start_line0: int | None = None
    start_col: int | None = None
    end_line0: int | None = None
    end_col: int | None = None
    end_exclusive: bool | None = True
    col_unit: str | None = None
    byte_start: int | None = None
    byte_len: int | None = None


def _pos_dict(line0: int | None, col: int | None) -> dict[str, int | None] | None:
    if line0 is None and col is None:
        return None
    return {"line0": line0, "col": col}


def _byte_span_dict(byte_start: int | None, byte_len: int | None) -> dict[str, int | None] | None:
    if byte_start is None and byte_len is None:
        return None
    return {"byte_start": byte_start, "byte_len": byte_len}


def _span_dict_from_spec(spec: SpanTemplateSpec) -> dict[str, object] | None:
    start = _pos_dict(spec.start_line0, spec.start_col)
    end = _pos_dict(spec.end_line0, spec.end_col)
    byte_span = _byte_span_dict(spec.byte_start, spec.byte_len)
    if (
        start is None
        and end is None
        and byte_span is None
        and spec.col_unit is None
        and spec.end_exclusive is None
    ):
        return None
    return {
        "start": start,
        "end": end,
        "end_exclusive": spec.end_exclusive,
        "col_unit": spec.col_unit,
        "byte_span": byte_span,
    }


def _attrs_map(values: Mapping[str, object] | None) -> list[tuple[str, str]]:
    if not values:
        return []
    return [(str(key), str(val)) for key, val in values.items() if val is not None]


def make_span_dict(bstart: int, bend: int) -> dict[str, int]:
    """Return byte span columns.

    Parameters
    ----------
    bstart
        Start byte offset (inclusive).
    bend
        End byte offset (exclusive).

    Returns
    -------
    dict[str, int]
        Row fragment with bstart and bend columns.
    """
    return {"bstart": bstart, "bend": bend}


def make_span_spec_dict(spec: SpanTemplateSpec) -> dict[str, object] | None:
    """Return a structured span dict for nested span fields.

    Parameters
    ----------
    spec
        Span template specification with position and byte information.

    Returns
    -------
    dict[str, object] | None
        Structured span dict or None when all fields are empty.
    """
    return _span_dict_from_spec(spec)


def make_attrs_list(values: Mapping[str, object] | None = None) -> list[tuple[str, str]]:
    """Return map entries for nested Arrow map fields.

    Parameters
    ----------
    values
        Key-value pairs to include in the attrs map.

    Returns
    -------
    list[tuple[str, str]]
        List of key/value map entries.
    """
    return _attrs_map(values)


@dataclass(frozen=True)
class ExtractionRowBuilder:
    """Build extraction rows with consistent file identity and span handling.

    This builder provides a standardized interface for constructing extraction
    rows with proper file identity columns, byte spans, and metadata attributes.

    Parameters
    ----------
    file_id
        Unique file identifier.
    path
        Repository-relative file path.
    file_sha256
        Optional SHA256 hash of file contents.
    repo_id
        Optional repository identifier.

    Examples
    --------
    >>> builder = ExtractionRowBuilder(
    ...     file_id="abc123",
    ...     path="src/main.py",
    ...     file_sha256="sha256...",
    ... )
    >>> row = builder.build_row(
    ...     kind="FunctionDef",
    ...     name="my_func",
    ...     bstart=100,
    ...     bend=250,
    ... )
    """

    file_id: str
    path: str
    file_sha256: str | None = None
    repo_id: str | None = None

    @classmethod
    def from_file_context(
        cls,
        file_ctx: FileContext,
        *,
        repo_id: str | None = None,
    ) -> ExtractionRowBuilder:
        """Create a row builder from a FileContext.

        Parameters
        ----------
        file_ctx
            Source file context with identity information.
        repo_id
            Optional repository identifier override.

        Returns
        -------
        ExtractionRowBuilder
            Row builder initialized from the file context.
        """
        return cls(
            file_id=file_ctx.file_id,
            path=file_ctx.path,
            file_sha256=file_ctx.file_sha256,
            repo_id=repo_id,
        )

    def add_identity(self) -> dict[str, str | None]:
        """Return the standard file identity columns.

        Returns
        -------
        dict[str, str | None]
            Row fragment with file_id, path, and file_sha256.
        """
        return {
            "file_id": self.file_id,
            "path": self.path,
            "file_sha256": self.file_sha256,
        }

    def add_repo_identity(self) -> dict[str, str | None]:
        """Return file identity columns with repo_id.

        Returns
        -------
        dict[str, str | None]
            Row fragment with repo, file_id, path, and file_sha256.
        """
        return {
            "repo": self.repo_id,
            **self.add_identity(),
        }

    @staticmethod
    def add_span(bstart: int, bend: int) -> dict[str, int]:
        """Return byte span columns.

        Parameters
        ----------
        bstart
            Start byte offset (inclusive).
        bend
            End byte offset (exclusive).

        Returns
        -------
        dict[str, int]
            Row fragment with bstart and bend columns.
        """
        return make_span_dict(bstart, bend)

    @staticmethod
    def add_span_spec(spec: SpanTemplateSpec) -> dict[str, object] | None:
        """Return a structured span dict for nested span fields.

        Parameters
        ----------
        spec
            Span template specification with position and byte information.

        Returns
        -------
        dict[str, object] | None
            Structured span dict or None when all fields are empty.
        """
        return make_span_spec_dict(spec)

    @staticmethod
    def add_attrs(values: Mapping[str, object] | None = None) -> list[tuple[str, str]]:
        """Return map entries for nested Arrow map fields.

        Parameters
        ----------
        values
            Key-value pairs to include in the attrs map.

        Returns
        -------
        list[tuple[str, str]]
            List of key/value map entries.
        """
        return make_attrs_list(values)

    def build_row(
        self,
        *,
        include_repo: bool = False,
        **fields: object,
    ) -> dict[str, object]:
        """Build a complete extraction row with identity and custom fields.

        Parameters
        ----------
        include_repo
            Whether to include the repo column in identity.
        fields
            Custom fields to include in the row.

        Returns
        -------
        dict[str, object]
            Complete row with identity columns and custom fields.
        """
        if include_repo:
            row: dict[str, object] = dict(self.add_repo_identity())
        else:
            row = dict(self.add_identity())
        row.update(fields)
        return row

    def build_row_with_span(
        self,
        bstart: int,
        bend: int,
        *,
        include_repo: bool = False,
        **fields: object,
    ) -> dict[str, object]:
        """Build a row with identity, byte span, and custom fields.

        Parameters
        ----------
        bstart
            Start byte offset (inclusive).
        bend
            End byte offset (exclusive).
        include_repo
            Whether to include the repo column in identity.
        fields
            Custom fields to include in the row.

        Returns
        -------
        dict[str, object]
            Complete row with identity, span, and custom fields.
        """
        row = self.build_row(include_repo=include_repo, **fields)
        row.update(self.add_span(bstart, bend))
        return row


def _resolve_schema(schema: SchemaLike) -> pa.Schema:
    """Resolve a schema-like object to a pyarrow Schema.

    Parameters
    ----------
    schema
        Arrow schema or compatible schema-like object.

    Returns
    -------
    pa.Schema
        Resolved pyarrow Schema.

    Raises
    ------
    TypeError
        Raised when schema is not a pa.Schema or lacks to_pyarrow() method.
    """
    if isinstance(schema, pa.Schema):
        return schema
    to_pyarrow = getattr(schema, "to_pyarrow", None)
    if callable(to_pyarrow):
        resolved = to_pyarrow()
        if isinstance(resolved, pa.Schema):
            return resolved
        msg = "Schema to_pyarrow() must return pa.Schema."
        raise TypeError(msg)
    msg = "Schema must be pa.Schema or have to_pyarrow() method."
    raise TypeError(msg)


@dataclass
class ExtractionBatchBuilder:
    """Build Arrow record batches from extraction rows.

    This builder accumulates rows and produces schema-aligned Arrow batches
    for efficient downstream processing.

    Parameters
    ----------
    schema
        Arrow schema defining the batch structure.

    Examples
    --------
    >>> schema = pa.schema([("file_id", pa.utf8()), ("path", pa.utf8())])
    >>> batch_builder = ExtractionBatchBuilder(schema)
    >>> batch_builder.add_row({"file_id": "abc", "path": "x.py"})
    >>> batch = batch_builder.build()
    """

    _schema: pa.Schema
    _rows: list[dict[str, object]] = field(default_factory=list)

    def __init__(self, schema: SchemaLike) -> None:
        """Initialize the batch builder with a schema.

        Parameters
        ----------
        schema
            Arrow schema or compatible schema-like object.
        """
        self._schema = _resolve_schema(schema)
        self._rows = []

    @classmethod
    def for_dataset(cls, dataset_name: str) -> ExtractionBatchBuilder:
        """Create a batch builder for a registered dataset.

        Parameters
        ----------
        dataset_name
            Name of the registered extraction dataset (e.g., "ast_files_v1").

        Returns
        -------
        ExtractionBatchBuilder
            Batch builder initialized with the dataset schema.
        """
        # Lazy import to avoid circular dependency
        from datafusion_engine.extract.registry import dataset_schema

        schema = dataset_schema(dataset_name)
        return cls(schema)

    @property
    def schema(self) -> pa.Schema:
        """Return the batch schema.

        Returns
        -------
        pa.Schema
            Arrow schema for the batch.
        """
        return self._schema

    @property
    def num_rows(self) -> int:
        """Return the number of accumulated rows.

        Returns
        -------
        int
            Row count.
        """
        return len(self._rows)

    def add_row(self, row: Mapping[str, object]) -> None:
        """Add a single row to the batch.

        Parameters
        ----------
        row
            Row mapping with column values.
        """
        self._rows.append(dict(row))

    def add_rows(self, rows: Iterable[Mapping[str, object]]) -> None:
        """Add multiple rows to the batch.

        Parameters
        ----------
        rows
            Iterable of row mappings.
        """
        self._rows.extend(dict(row) for row in rows)

    def extend(self, rows: Iterable[Mapping[str, object]]) -> None:
        """Extend the batch with rows (alias for add_rows).

        Parameters
        ----------
        rows
            Iterable of row mappings.
        """
        self.add_rows(rows)

    def clear(self) -> None:
        """Remove all accumulated rows."""
        self._rows.clear()

    def build(self) -> pa.RecordBatch:
        """Build a RecordBatch from accumulated rows.

        Returns
        -------
        pa.RecordBatch
            Arrow RecordBatch aligned to the schema.
        """
        return pa.RecordBatch.from_pylist(self._rows, schema=self._schema)

    def build_table(self) -> pa.Table:
        """Build a Table from accumulated rows.

        Returns
        -------
        pa.Table
            Arrow Table aligned to the schema.
        """
        return pa.Table.from_pylist(self._rows, schema=self._schema)

    def build_and_clear(self) -> pa.RecordBatch:
        """Build a RecordBatch and clear accumulated rows.

        Returns
        -------
        pa.RecordBatch
            Arrow RecordBatch aligned to the schema.
        """
        batch = self.build()
        self.clear()
        return batch


@dataclass(frozen=True)
class SchemaTemplateOptions:
    """Options for extraction schema template generation.

    Consolidates schema template parameters into a single options object.
    """

    include_file_id: bool = True
    include_path: bool = True
    include_sha256: bool = True
    include_repo: bool = False
    include_bstart: bool = False
    include_bend: bool = False


def extraction_schema_template(
    options: SchemaTemplateOptions | None = None,
) -> list[tuple[str, pa.DataType]]:
    """Return common extraction schema field templates.

    This function returns a list of (name, type) tuples representing
    common extraction schema patterns that can be extended with
    extractor-specific fields.

    Parameters
    ----------
    options
        Schema template options. If None, uses default options.

    Returns
    -------
    list[tuple[str, pa.DataType]]
        Field definitions for Arrow schema construction.

    Examples
    --------
    >>> opts = SchemaTemplateOptions(include_bstart=True, include_bend=True)
    >>> fields = extraction_schema_template(opts)
    >>> fields.extend(
    ...     [
    ...         ("kind", pa.utf8()),
    ...         ("name", pa.utf8()),
    ...     ]
    ... )
    >>> schema = pa.schema(fields)
    """
    opts = options or SchemaTemplateOptions()
    fields: list[tuple[str, pa.DataType]] = []
    if opts.include_repo:
        fields.append(("repo", pa.utf8()))
    if opts.include_file_id:
        fields.append(("file_id", pa.utf8()))
    if opts.include_path:
        fields.append(("path", pa.utf8()))
    if opts.include_sha256:
        fields.append(("file_sha256", pa.utf8()))
    if opts.include_bstart:
        fields.append(("bstart", pa.int64()))
    if opts.include_bend:
        fields.append(("bend", pa.int64()))
    return fields


def build_extraction_rows(
    file_ctx: FileContext,
    row_data: Iterable[Mapping[str, object]],
    *,
    repo_id: str | None = None,
    include_repo: bool = False,
) -> list[dict[str, object]]:
    """Build extraction rows for a file context.

    Convenience function that creates an ExtractionRowBuilder and builds
    rows with file identity merged into each row.

    Parameters
    ----------
    file_ctx
        Source file context.
    row_data
        Iterable of row data mappings to merge with identity.
    repo_id
        Optional repository identifier.
    include_repo
        Whether to include the repo column.

    Returns
    -------
    list[dict[str, object]]
        Rows with file identity merged.
    """
    builder = ExtractionRowBuilder.from_file_context(file_ctx, repo_id=repo_id)
    return [builder.build_row(include_repo=include_repo, **dict(data)) for data in row_data]


__all__ = [
    "ExtractionBatchBuilder",
    "ExtractionRowBuilder",
    "SchemaTemplateOptions",
    "SpanTemplateSpec",
    "build_extraction_rows",
    "extraction_schema_template",
    "make_attrs_list",
    "make_span_dict",
    "make_span_spec_dict",
]
