"""Extraction schema derivation from DataFusion plan analysis.

Provide a fluent builder interface for constructing extraction schemas from
composable field bundles. The builder supports automatic derivation of schemas
from DataFusion plans, reducing manual specification and ensuring consistency
across extractors.

This module is the capstone integration point for:
- File identity fields (schema_spec.file_identity)
- Span fields (schema_spec.specs, datafusion_engine.arrow.semantic)
- Evidence metadata (schema_spec.evidence_metadata)
- Schema inference via DataFusion lineage (datafusion_engine.lineage.datafusion)
- Nested type builders (datafusion_engine.arrow.nested)

Example
-------
Build an extraction schema with standard field bundles::

    from extract.schema_derivation import ExtractionSchemaBuilder

    schema = (
        ExtractionSchemaBuilder("my_extractor", version=1)
        .with_file_identity(include_sha256=True)
        .with_span(prefix="")
        .with_evidence_metadata()
        .with_fields(FieldSpec(name="custom_col", dtype=pa.string()))
        .build()
    )

Derive a schema from a DataFusion source table::

    from extract.schema_derivation import derive_extraction_schema

    schema = derive_extraction_schema(
        extractor_name="cst_refs",
        source_table="py_cst_refs_v1",
        ctx=session_context,
    )
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Self, cast

import pyarrow as pa

from datafusion_engine.arrow import interop
from datafusion_engine.arrow.semantic import SPAN_STORAGE
from schema_spec.arrow_types import arrow_type_from_pyarrow
from schema_spec.evidence_metadata import evidence_metadata_bundle
from schema_spec.field_spec import FieldSpec
from schema_spec.file_identity import file_identity_field_specs
from schema_spec.specs import FieldBundle, TableSchemaSpec, span_bundle

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.arrow.interop import DataTypeLike, SchemaLike
    from datafusion_engine.lineage.datafusion import LineageReport


# ---------------------------------------------------------------------------
# Field bundle factories for common extraction patterns
# ---------------------------------------------------------------------------


def _byte_span_bundle(prefix: str = "") -> FieldBundle:
    """Return a bundle for byte offset span columns.

    Parameters
    ----------
    prefix
        Optional prefix for field names.

    Returns
    -------
    FieldBundle
        Bundle containing bstart/bend byte offset fields.
    """
    normalized = f"{prefix}_" if prefix and not prefix.endswith("_") else prefix
    return FieldBundle(
        name=f"{normalized}byte_span" if normalized else "byte_span",
        fields=(
            FieldSpec(
                name=f"{normalized}bstart", dtype=arrow_type_from_pyarrow(interop.int64())
            ),
            FieldSpec(name=f"{normalized}bend", dtype=arrow_type_from_pyarrow(interop.int64())),
        ),
    )


def _structured_span_bundle(prefix: str = "") -> FieldBundle:
    """Return a bundle for structured span columns.

    Parameters
    ----------
    prefix
        Optional prefix for field names.

    Returns
    -------
    FieldBundle
        Bundle containing the nested span struct field.
    """
    normalized = f"{prefix}_" if prefix and not prefix.endswith("_") else prefix
    return FieldBundle(
        name=f"{normalized}span" if normalized else "span",
        fields=(
            FieldSpec(name=f"{normalized}span", dtype=arrow_type_from_pyarrow(SPAN_STORAGE)),
        ),
    )


def _line_col_span_bundle(prefix: str = "") -> FieldBundle:
    """Return a bundle for line/column span columns.

    Parameters
    ----------
    prefix
        Optional prefix for field names.

    Returns
    -------
    FieldBundle
        Bundle containing line and column start/end fields.
    """
    normalized = f"{prefix}_" if prefix and not prefix.endswith("_") else prefix
    return FieldBundle(
        name=f"{normalized}line_col_span" if normalized else "line_col_span",
        fields=(
            FieldSpec(
                name=f"{normalized}start_line", dtype=arrow_type_from_pyarrow(interop.int32())
            ),
            FieldSpec(
                name=f"{normalized}start_col", dtype=arrow_type_from_pyarrow(interop.int32())
            ),
            FieldSpec(
                name=f"{normalized}end_line", dtype=arrow_type_from_pyarrow(interop.int32())
            ),
            FieldSpec(
                name=f"{normalized}end_col", dtype=arrow_type_from_pyarrow(interop.int32())
            ),
        ),
    )


# ---------------------------------------------------------------------------
# Extraction schema builder
# ---------------------------------------------------------------------------


@dataclass
class ExtractionSchemaBuilder:
    """Fluent builder for extraction table schemas.

    Construct extraction schemas by composing standard field bundles for
    file identity, spans, and evidence metadata with custom fields.

    Parameters
    ----------
    name
        Dataset name for the extraction schema.
    version
        Schema version number.

    Attributes
    ----------
    _name : str
        Dataset name.
    _version : int
        Schema version.
    _bundles : list[FieldBundle]
        Accumulated field bundles.
    _fields : list[FieldSpec]
        Accumulated custom field specs.
    _required_non_null : list[str]
        Required non-null column names.
    _key_fields : list[str]
        Key column names for deduplication.
    """

    _name: str
    _version: int = 1
    _bundles: list[FieldBundle] = field(default_factory=list)
    _fields: list[FieldSpec] = field(default_factory=list)
    _required_non_null: list[str] = field(default_factory=list)
    _key_fields: list[str] = field(default_factory=list)

    def __init__(self, name: str, version: int = 1) -> None:
        """Initialize the extraction schema builder.

        Parameters
        ----------
        name
            Dataset name for the extraction schema.
        version
            Schema version number.
        """
        self._name = name
        self._version = version
        self._bundles = []
        self._fields = []
        self._required_non_null = []
        self._key_fields = []

    def with_file_identity(
        self,
        *,
        include_sha256: bool = True,
        include_repo: bool = False,
    ) -> Self:
        """Add file identity fields to the schema.

        Parameters
        ----------
        include_sha256
            Include the file_sha256 field. Default True.
        include_repo
            Include the repo field. Default False.

        Returns
        -------
        Self
            Builder instance for chaining.
        """
        specs = file_identity_field_specs(
            include_sha256=include_sha256,
            include_repo=include_repo,
        )
        bundle = FieldBundle(name="file_identity", fields=specs)
        self._bundles.append(bundle)
        # file_id and path are typically required
        self._required_non_null.extend(["file_id", "path"])
        return self

    def with_span(self, prefix: str = "", *, style: str = "byte") -> Self:
        """Add span fields to the schema.

        Parameters
        ----------
        prefix
            Optional prefix for span field names.
        style
            Span style: "byte" for bstart/bend, "structured" for nested span
            struct, "line_col" for line/column pairs. Default "byte".

        Returns
        -------
        Self
            Builder instance for chaining.

        Raises
        ------
        ValueError
            Raised when an unsupported span style is provided.
        """
        if style == "byte":
            self._bundles.append(_byte_span_bundle(prefix))
        elif style == "structured":
            self._bundles.append(_structured_span_bundle(prefix))
        elif style == "line_col":
            self._bundles.append(_line_col_span_bundle(prefix))
        else:
            msg = f"Unsupported span style: {style!r}. Use 'byte', 'structured', or 'line_col'."
            raise ValueError(msg)
        return self

    def with_evidence_metadata(self) -> Self:
        """Add evidence metadata fields to the schema.

        Returns
        -------
        Self
            Builder instance for chaining.
        """
        self._bundles.append(evidence_metadata_bundle())
        return self

    def with_bundle(self, bundle: FieldBundle) -> Self:
        """Add a custom field bundle to the schema.

        Parameters
        ----------
        bundle
            Field bundle to add.

        Returns
        -------
        Self
            Builder instance for chaining.
        """
        self._bundles.append(bundle)
        return self

    def with_bundles(self, *bundles: FieldBundle) -> Self:
        """Add multiple field bundles to the schema.

        Parameters
        ----------
        *bundles
            Field bundles to add.

        Returns
        -------
        Self
            Builder instance for chaining.
        """
        self._bundles.extend(bundles)
        return self

    def with_fields(self, *fields: FieldSpec) -> Self:
        """Add custom field specs to the schema.

        Parameters
        ----------
        *fields
            Field specifications to add.

        Returns
        -------
        Self
            Builder instance for chaining.
        """
        self._fields.extend(fields)
        return self

    def with_field(
        self,
        name: str,
        dtype: DataTypeLike,
        *,
        nullable: bool = True,
        metadata: dict[str, str] | None = None,
    ) -> Self:
        """Add a single field to the schema.

        Parameters
        ----------
        name
            Field name.
        dtype
            Arrow data type.
        nullable
            Whether the field is nullable.
        metadata
            Optional field metadata.

        Returns
        -------
        Self
            Builder instance for chaining.
        """
        spec = FieldSpec(
            name=name,
            dtype=arrow_type_from_pyarrow(interop.ensure_arrow_dtype(dtype)),
            nullable=nullable,
            metadata=metadata or {},
        )
        self._fields.append(spec)
        return self

    def with_required_non_null(self, *columns: str) -> Self:
        """Mark columns as required non-null.

        Parameters
        ----------
        *columns
            Column names that must not be null.

        Returns
        -------
        Self
            Builder instance for chaining.
        """
        self._required_non_null.extend(columns)
        return self

    def with_key_fields(self, *columns: str) -> Self:
        """Mark columns as key fields for deduplication.

        Parameters
        ----------
        *columns
            Column names forming the deduplication key.

        Returns
        -------
        Self
            Builder instance for chaining.
        """
        self._key_fields.extend(columns)
        return self

    def build(self) -> TableSchemaSpec:
        """Build the final TableSchemaSpec.

        Returns
        -------
        TableSchemaSpec
            Constructed table schema specification.
        """
        # Merge bundle fields with explicit fields, deduplicating by name
        merged_fields: list[FieldSpec] = []
        seen_names: set[str] = set()

        for bundle in self._bundles:
            for bundle_field in bundle.fields:
                if bundle_field.name not in seen_names:
                    merged_fields.append(bundle_field)
                    seen_names.add(bundle_field.name)

        for custom_field in self._fields:
            if custom_field.name not in seen_names:
                merged_fields.append(custom_field)
                seen_names.add(custom_field.name)

        # Deduplicate constraint lists
        required = tuple(dict.fromkeys(self._required_non_null))
        keys = tuple(dict.fromkeys(self._key_fields))

        return TableSchemaSpec(
            name=self._name,
            version=self._version,
            fields=merged_fields,
            required_non_null=required,
            key_fields=keys,
        )


# ---------------------------------------------------------------------------
# Schema derivation from DataFusion plans
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DerivedSchemaInfo:
    """Information derived from DataFusion plan analysis.

    Attributes
    ----------
    source_tables : tuple[str, ...]
        Table names referenced in the plan.
    required_columns : Mapping[str, tuple[str, ...]]
        Per-table required columns.
    inferred_fields : tuple[FieldSpec, ...]
        Field specs inferred from the plan output schema.
    lineage : LineageReport | None
        Full lineage report when available.
    """

    source_tables: tuple[str, ...]
    required_columns: Mapping[str, tuple[str, ...]]
    inferred_fields: tuple[FieldSpec, ...]
    lineage: LineageReport | None = None


@dataclass(frozen=True)
class DerivationOptions:
    """Options for schema derivation from DataFusion plans.

    Attributes
    ----------
    version : int
        Schema version number.
    include_file_identity : bool
        Include file identity fields.
    include_span : bool
        Include span fields.
    include_evidence_metadata : bool
        Include evidence metadata fields.
    span_style : str
        Span style to use.
    """

    version: int = 1
    include_file_identity: bool = True
    include_span: bool = True
    include_evidence_metadata: bool = False
    span_style: str = "byte"


def _field_spec_from_arrow_field(arrow_field: pa.Field) -> FieldSpec:
    """Convert an Arrow field to a FieldSpec.

    Parameters
    ----------
    arrow_field
        PyArrow field definition.

    Returns
    -------
    FieldSpec
        Equivalent field specification.
    """
    metadata: dict[str, str] = {}
    if arrow_field.metadata:
        for key, value in arrow_field.metadata.items():
            decoded_key = key.decode("utf-8") if isinstance(key, bytes) else str(key)
            decoded_value = value.decode("utf-8") if isinstance(value, bytes) else str(value)
            metadata[decoded_key] = decoded_value

    return FieldSpec(
        name=arrow_field.name,
        dtype=arrow_type_from_pyarrow(arrow_field.type),
        nullable=arrow_field.nullable,
        metadata=metadata,
    )


def _derive_schema_info_from_plan(
    plan: object,
    *,
    udf_snapshot: Mapping[str, object] | None = None,
) -> DerivedSchemaInfo:
    """Derive schema information from a DataFusion logical plan.

    Parameters
    ----------
    plan
        DataFusion logical plan object.
    udf_snapshot
        Optional UDF snapshot for lineage analysis.

    Returns
    -------
    DerivedSchemaInfo
        Schema information derived from plan analysis.
    """
    from datafusion_engine.lineage.datafusion import extract_lineage

    lineage = extract_lineage(plan, udf_snapshot=udf_snapshot)

    # Get output schema from the plan
    schema_method = getattr(plan, "schema", None)
    inferred_fields: tuple[FieldSpec, ...] = ()
    if callable(schema_method):
        try:
            output_schema = schema_method()
            # Convert Arrow schema to FieldSpec list
            if isinstance(output_schema, pa.Schema):
                typed_schema = cast("pa.Schema", output_schema)
                inferred_fields = tuple(
                    _field_spec_from_arrow_field(field) for field in typed_schema
                )
        except (RuntimeError, TypeError, ValueError):
            pass

    return DerivedSchemaInfo(
        source_tables=lineage.referenced_tables,
        required_columns=dict(lineage.required_columns_by_dataset),
        inferred_fields=inferred_fields,
        lineage=lineage,
    )


def derive_extraction_schema(
    extractor_name: str,
    source_table: str,
    ctx: SessionContext,
    *,
    options: DerivationOptions | None = None,
    udf_snapshot: Mapping[str, object] | None = None,
) -> TableSchemaSpec:
    """Derive an extraction schema from a DataFusion source table.

    Analyze the DataFusion plan for the source table and construct an
    extraction schema that includes standard field bundles plus any
    additional columns inferred from the plan.

    Parameters
    ----------
    extractor_name
        Name for the derived extraction schema.
    source_table
        Source table name registered in DataFusion.
    ctx
        DataFusion session context.
    options
        Derivation options controlling which bundles to include.
    udf_snapshot
        Optional UDF snapshot for lineage analysis.

    Returns
    -------
    TableSchemaSpec
        Derived extraction schema specification.

    Raises
    ------
    ValueError
        Raised when the source table cannot be resolved or analyzed.
    """
    if options is None:
        options = DerivationOptions()

    # Get the table from the context
    try:
        table_df = ctx.table(source_table)
    except Exception as exc:
        msg = f"Cannot resolve source table {source_table!r} from context."
        raise ValueError(msg) from exc

    # Get the logical plan
    plan = table_df.logical_plan()

    # Derive schema information from the plan
    schema_info = _derive_schema_info_from_plan(plan, udf_snapshot=udf_snapshot)

    # Build the extraction schema using the builder
    builder = ExtractionSchemaBuilder(extractor_name, version=options.version)

    if options.include_file_identity:
        builder = builder.with_file_identity()

    if options.include_span:
        builder = builder.with_span(style=options.span_style)

    if options.include_evidence_metadata:
        builder = builder.with_evidence_metadata()

    # Add inferred fields that aren't already covered by bundles
    standard_field_names = _standard_extraction_field_names(
        include_file_identity=options.include_file_identity,
        include_span=options.include_span,
        include_evidence_metadata=options.include_evidence_metadata,
    )

    for field_spec in schema_info.inferred_fields:
        if field_spec.name not in standard_field_names:
            builder = builder.with_fields(field_spec)

    return builder.build()


def _standard_extraction_field_names(
    *,
    include_file_identity: bool,
    include_span: bool,
    include_evidence_metadata: bool,
) -> set[str]:
    """Return the set of standard extraction field names.

    Parameters
    ----------
    include_file_identity
        Include file identity field names.
    include_span
        Include span field names.
    include_evidence_metadata
        Include evidence metadata field names.

    Returns
    -------
    set[str]
        Set of standard field names.
    """
    names: set[str] = set()

    if include_file_identity:
        for spec in file_identity_field_specs(include_sha256=True, include_repo=True):
            names.add(spec.name)

    if include_span:
        # All span styles
        for bundle in [_byte_span_bundle(), _structured_span_bundle(), _line_col_span_bundle()]:
            for field_spec in bundle.fields:
                names.add(field_spec.name)
        # Also include the standard span bundle
        for field_spec in span_bundle().fields:
            names.add(field_spec.name)

    if include_evidence_metadata:
        for field_spec in evidence_metadata_bundle().fields:
            names.add(field_spec.name)

    return names


# ---------------------------------------------------------------------------
# Extraction schema templates for common extractor patterns
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ExtractionSchemaTemplate:
    """Template for common extraction schema patterns.

    Attributes
    ----------
    name : str
        Template name (e.g., "ast", "cst", "bytecode").
    include_file_identity : bool
        Include file identity fields.
    include_span : bool
        Include span fields.
    span_style : str
        Span style to use.
    include_evidence_metadata : bool
        Include evidence metadata fields.
    additional_fields : tuple[FieldSpec, ...]
        Additional fields specific to this template.
    builder_hook : Callable[[ExtractionSchemaBuilder], ExtractionSchemaBuilder] | None
        Optional hook to customize the builder.
    """

    name: str
    include_file_identity: bool = True
    include_span: bool = True
    span_style: str = "byte"
    include_evidence_metadata: bool = False
    additional_fields: tuple[FieldSpec, ...] = ()
    builder_hook: Callable[[ExtractionSchemaBuilder], ExtractionSchemaBuilder] | None = None


def _ast_template_hook(builder: ExtractionSchemaBuilder) -> ExtractionSchemaBuilder:
    """Customize builder for AST extraction schemas.

    Returns
    -------
    ExtractionSchemaBuilder
        Builder with AST-specific fields added.
    """
    return builder.with_field("node_type", interop.string()).with_field("node_id", interop.string())


def _cst_template_hook(builder: ExtractionSchemaBuilder) -> ExtractionSchemaBuilder:
    """Customize builder for CST extraction schemas.

    Returns
    -------
    ExtractionSchemaBuilder
        Builder with CST-specific fields added.
    """
    return (
        builder.with_field("node_type", interop.string())
        .with_field("node_id", interop.string())
        .with_field("qualified_name", interop.string(), nullable=True)
    )


def _bytecode_template_hook(builder: ExtractionSchemaBuilder) -> ExtractionSchemaBuilder:
    """Customize builder for bytecode extraction schemas.

    Returns
    -------
    ExtractionSchemaBuilder
        Builder with bytecode-specific fields added.
    """
    return (
        builder.with_field("code_unit_id", interop.string())
        .with_field("block_id", interop.string())
        .with_field("instr_id", interop.string())
        .with_field("opname", interop.string())
        .with_field("offset", interop.int32())
    )


def _scip_template_hook(builder: ExtractionSchemaBuilder) -> ExtractionSchemaBuilder:
    """Customize builder for SCIP extraction schemas.

    Returns
    -------
    ExtractionSchemaBuilder
        Builder with SCIP-specific fields added.
    """
    return (
        builder.with_span(style="line_col")
        .with_field("symbol", interop.string())
        .with_field("document_id", interop.string())
    )


def _tree_sitter_template_hook(builder: ExtractionSchemaBuilder) -> ExtractionSchemaBuilder:
    """Customize builder for tree-sitter extraction schemas.

    Returns
    -------
    ExtractionSchemaBuilder
        Builder with tree-sitter-specific fields added.
    """
    return (
        builder.with_field("node_id", interop.string())
        .with_field("node_type", interop.string())
        .with_field("parent_id", interop.string(), nullable=True)
    )


def _repo_scan_template_hook(builder: ExtractionSchemaBuilder) -> ExtractionSchemaBuilder:
    """Customize builder for repo scan extraction schemas.

    Returns
    -------
    ExtractionSchemaBuilder
        Builder with repo-scan-specific fields added.
    """
    # Repo scan has file identity but no span
    return builder.with_field("abs_path", interop.string()).with_field(
        "size_bytes", interop.int64()
    )


EXTRACTION_SCHEMA_TEMPLATES: Mapping[str, ExtractionSchemaTemplate] = {
    "ast": ExtractionSchemaTemplate(
        name="ast",
        include_file_identity=True,
        include_span=True,
        span_style="byte",
        builder_hook=_ast_template_hook,
    ),
    "cst": ExtractionSchemaTemplate(
        name="cst",
        include_file_identity=True,
        include_span=True,
        span_style="byte",
        builder_hook=_cst_template_hook,
    ),
    "bytecode": ExtractionSchemaTemplate(
        name="bytecode",
        include_file_identity=True,
        include_span=False,
        builder_hook=_bytecode_template_hook,
    ),
    "symtable": ExtractionSchemaTemplate(
        name="symtable",
        include_file_identity=True,
        include_span=False,
    ),
    "scip": ExtractionSchemaTemplate(
        name="scip",
        include_file_identity=True,
        include_span=False,  # SCIP uses line_col, added via hook
        builder_hook=_scip_template_hook,
    ),
    "tree_sitter": ExtractionSchemaTemplate(
        name="tree_sitter",
        include_file_identity=True,
        include_span=True,
        span_style="byte",
        builder_hook=_tree_sitter_template_hook,
    ),
    "repo_scan": ExtractionSchemaTemplate(
        name="repo_scan",
        include_file_identity=True,
        include_span=False,
        builder_hook=_repo_scan_template_hook,
    ),
}


def build_schema_from_template(
    template_name: str,
    dataset_name: str,
    *,
    version: int = 1,
    extra_fields: Sequence[FieldSpec] | None = None,
) -> TableSchemaSpec:
    """Build an extraction schema from a template.

    Parameters
    ----------
    template_name
        Template name (e.g., "ast", "cst", "bytecode").
    dataset_name
        Name for the resulting dataset schema.
    version
        Schema version number.
    extra_fields
        Additional fields to append to the schema.

    Returns
    -------
    TableSchemaSpec
        Table schema specification built from the template.

    Raises
    ------
    KeyError
        Raised when the template name is unknown.
    """
    template = EXTRACTION_SCHEMA_TEMPLATES.get(template_name)
    if template is None:
        available = sorted(EXTRACTION_SCHEMA_TEMPLATES.keys())
        msg = f"Unknown extraction template: {template_name!r}. Available: {available}"
        raise KeyError(msg)

    builder = ExtractionSchemaBuilder(dataset_name, version=version)

    if template.include_file_identity:
        builder = builder.with_file_identity()

    if template.include_span:
        builder = builder.with_span(style=template.span_style)

    if template.include_evidence_metadata:
        builder = builder.with_evidence_metadata()

    if template.additional_fields:
        builder = builder.with_fields(*template.additional_fields)

    if template.builder_hook is not None:
        builder = template.builder_hook(builder)

    if extra_fields:
        builder = builder.with_fields(*extra_fields)

    return builder.build()


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------


def extraction_schema_to_arrow(spec: TableSchemaSpec) -> SchemaLike:
    """Convert a TableSchemaSpec to an Arrow schema.

    Parameters
    ----------
    spec
        Table schema specification.

    Returns
    -------
    SchemaLike
        Arrow schema with metadata applied.
    """
    return spec.to_arrow_schema()


def validate_extraction_schema(
    spec: TableSchemaSpec,
    *,
    require_file_identity: bool = True,
    require_span: bool = False,
) -> tuple[bool, list[str]]:
    """Validate an extraction schema against requirements.

    Parameters
    ----------
    spec
        Table schema specification to validate.
    require_file_identity
        Require file identity fields. Default True.
    require_span
        Require span fields. Default False.

    Returns
    -------
    tuple[bool, list[str]]
        Tuple of (is_valid, list of validation errors).
    """
    errors: list[str] = []
    field_names = {field.name for field in spec.fields}

    if require_file_identity:
        required_identity = {"file_id", "path"}
        missing_identity = required_identity - field_names
        if missing_identity:
            errors.append(f"Missing file identity fields: {sorted(missing_identity)}")

    if require_span:
        # Check for any span-style fields
        span_patterns = {"bstart", "bend", "span", "start_line", "start_col"}
        has_span = bool(span_patterns & field_names)
        if not has_span:
            errors.append("Missing span fields (expected bstart/bend, span, or start_line/col)")

    return (len(errors) == 0, errors)


__all__ = [
    "EXTRACTION_SCHEMA_TEMPLATES",
    "DerivationOptions",
    "DerivedSchemaInfo",
    "ExtractionSchemaBuilder",
    "ExtractionSchemaTemplate",
    "build_schema_from_template",
    "derive_extraction_schema",
    "extraction_schema_to_arrow",
    "validate_extraction_schema",
]
