# Extraction-Semantics Alignment Plan

> **Objective:** Align `src/extract` architecture with `src/semantics` patterns and better integrate with the DataFusion/DeltaLake compute infrastructure used throughout the codebase.

---

## Executive Summary

The `src/extract` module is a mature, well-structured extraction framework with strong PyArrow/DataFusion integration. However, it predates the declarative, spec-driven architecture deployed in `src/semantics`. This plan identifies 8 scopes of work to:

1. Adopt declarative spec registries for extractor definitions
2. Introduce an ExtractionCompiler pattern mirroring SemanticCompiler
3. Add semantic typing to extraction schemas
4. Consolidate catalog and metadata management
5. Align naming conventions with canonical semantic patterns
6. Enable CDF-aware incremental extraction
7. Strengthen validation and quality layers
8. Deprecate legacy coordination patterns

---

## Scope 1: Declarative Extractor Spec Registry

### Goal
Replace imperative extractor instantiation with a declarative spec registry pattern matching `semantics.spec_registry`.

### Key Architectural Elements

```python
# src/extract/spec_registry.py

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal, Protocol, runtime_checkable

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext
    from extract.coordination.context import FileContext


EvidenceTier = Literal[1, 2, 3, 4]
SourceType = Literal["ast", "cst", "scip", "bytecode", "symtable", "treesitter", "line_index"]


@dataclass(frozen=True)
class ExtractorSpec:
    """Declarative specification for an evidence extractor.

    Attributes
    ----------
    name
        Canonical extractor name (e.g., "cst_refs", "scip_occurrences").
    source_type
        Evidence source category.
    evidence_tier
        Confidence classification (1=highest syntactic, 4=derived).
    required_inputs
        Upstream dependencies (e.g., ("repo_files_v1",)).
    output_name
        Canonical output table name with _v1 suffix.
    options_class
        Optional dataclass for extractor-specific options.
    parallel
        Whether file-level parallelization is supported.
    cacheable
        Whether results can be cached by content hash.
    """

    name: str
    source_type: SourceType
    evidence_tier: EvidenceTier
    required_inputs: tuple[str, ...] = ()
    output_name: str | None = None
    options_class: type | None = None
    parallel: bool = True
    cacheable: bool = True

    def canonical_output(self) -> str:
        """Return canonical output name with _v1 suffix."""
        if self.output_name is not None:
            return self.output_name
        return f"{self.name}_v1"


@runtime_checkable
class ExtractionViewBuilder(Protocol):
    """Protocol for extraction view builders.

    Mirrors semantics.builders.protocol.SemanticViewBuilder pattern.
    """

    @property
    def name(self) -> str:
        """Canonical extractor name."""
        ...

    @property
    def evidence_tier(self) -> EvidenceTier:
        """Evidence confidence tier."""
        ...

    @property
    def upstream_deps(self) -> tuple[str, ...]:
        """Required upstream inputs."""
        ...

    def build(
        self,
        ctx: SessionContext,
        file_contexts: Sequence[FileContext] | None = None,
    ) -> DataFrame:
        """Execute extraction and return DataFrame."""
        ...


# Declarative registry of all extractors
EXTRACTOR_SPECS: tuple[ExtractorSpec, ...] = (
    ExtractorSpec(
        name="ast_files",
        source_type="ast",
        evidence_tier=1,
        required_inputs=("repo_files_v1",),
        output_name="ast_files_v1",
        parallel=True,
        cacheable=True,
    ),
    ExtractorSpec(
        name="cst_refs",
        source_type="cst",
        evidence_tier=1,
        required_inputs=("repo_files_v1",),
        output_name="cst_refs_v1",
        parallel=True,
        cacheable=True,
    ),
    ExtractorSpec(
        name="cst_defs",
        source_type="cst",
        evidence_tier=1,
        required_inputs=("repo_files_v1",),
        output_name="cst_defs_v1",
        parallel=True,
        cacheable=True,
    ),
    ExtractorSpec(
        name="scip_occurrences",
        source_type="scip",
        evidence_tier=2,
        required_inputs=("repo_files_v1",),
        output_name="scip_occurrences_v1",
        parallel=False,
        cacheable=True,
    ),
    ExtractorSpec(
        name="bytecode_files",
        source_type="bytecode",
        evidence_tier=2,
        required_inputs=("repo_files_v1",),
        output_name="bytecode_files_v1",
        parallel=True,
        cacheable=True,
    ),
    ExtractorSpec(
        name="symtable_files",
        source_type="symtable",
        evidence_tier=2,
        required_inputs=("repo_files_v1",),
        output_name="symtable_files_v1",
        parallel=True,
        cacheable=True,
    ),
    ExtractorSpec(
        name="tree_sitter_files",
        source_type="treesitter",
        evidence_tier=1,
        required_inputs=("repo_files_v1",),
        output_name="tree_sitter_files_v1",
        parallel=True,
        cacheable=True,
    ),
    ExtractorSpec(
        name="file_line_index",
        source_type="line_index",
        evidence_tier=1,
        required_inputs=("repo_files_v1",),
        output_name="file_line_index_v1",
        parallel=True,
        cacheable=True,
    ),
)


def extractor_spec(name: str) -> ExtractorSpec:
    """Look up extractor spec by name.

    Raises KeyError if not found.
    """
    for spec in EXTRACTOR_SPECS:
        if spec.name == name or spec.canonical_output() == name:
            return spec
    msg = f"Unknown extractor: {name!r}"
    raise KeyError(msg)


def extractors_by_tier(tier: EvidenceTier) -> tuple[ExtractorSpec, ...]:
    """Return all extractors at a given evidence tier."""
    return tuple(spec for spec in EXTRACTOR_SPECS if spec.evidence_tier == tier)


def extractors_by_source(source_type: SourceType) -> tuple[ExtractorSpec, ...]:
    """Return all extractors for a source type."""
    return tuple(spec for spec in EXTRACTOR_SPECS if spec.source_type == source_type)
```

### Target Files
- **Create:** `src/extract/spec_registry.py`
- **Create:** `src/extract/builders/__init__.py`
- **Create:** `src/extract/builders/protocol.py`
- **Create:** `src/extract/builders/factory.py`

### Files to Deprecate After Completion
- `src/extract/coordination/spec_helpers.py` (consolidate into spec_registry)

### Implementation Checklist
- [ ] Create `ExtractorSpec` dataclass with all metadata fields
- [ ] Define `ExtractionViewBuilder` protocol
- [ ] Build `EXTRACTOR_SPECS` registry with all 8+ extractors
- [ ] Add lookup functions: `extractor_spec()`, `extractors_by_tier()`, `extractors_by_source()`
- [ ] Create builder factory that generates builders from specs
- [ ] Add tests for spec registry lookups and validation
- [ ] Update `src/extract/__init__.py` to export new registry

---

## Scope 2: ExtractionCompiler Pattern

### Goal
Introduce an `ExtractionCompiler` class mirroring `SemanticCompiler` to manage extraction context, builder registry, and lazy table analysis.

### Key Architectural Elements

```python
# src/extract/compiler.py

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from obs.otel.scopes import SCOPE_EXTRACT
from obs.otel.tracing import stage_span

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from extract.builders.protocol import ExtractionViewBuilder
    from extract.coordination.context import FileContext
    from extract.spec_registry import ExtractorSpec


@dataclass
class ExtractedTableInfo:
    """Analyzed extraction table with metadata.

    Mirrors semantics.compiler.TableInfo pattern.
    """

    name: str
    df: DataFrame
    spec: ExtractorSpec
    schema: ExtractionSchema
    row_count: int | None = None
    plan_fingerprint: str | None = None

    @classmethod
    def analyze(
        cls,
        name: str,
        df: DataFrame,
        spec: ExtractorSpec,
    ) -> ExtractedTableInfo:
        """Analyze a DataFrame and wrap with extraction metadata."""
        from extract.schema import ExtractionSchema

        return cls(
            name=name,
            df=df,
            spec=spec,
            schema=ExtractionSchema.from_df(df, table_name=name),
            row_count=None,
            plan_fingerprint=None,
        )


@dataclass
class ExtractionCompiler:
    """Compiler for extraction operations.

    Maintains a registry of extraction builders and their outputs,
    providing lazy table analysis and caching of results.

    Mirrors semantics.compiler.SemanticCompiler pattern.
    """

    ctx: SessionContext
    runtime_profile: DataFusionRuntimeProfile | None = None
    config: ExtractionConfig | None = None
    _builders: dict[str, ExtractionViewBuilder] = field(default_factory=dict)
    _tables: dict[str, ExtractedTableInfo] = field(default_factory=dict)

    def register_builder(
        self,
        spec: ExtractorSpec,
        *,
        file_contexts: Sequence[FileContext] | None = None,
    ) -> ExtractionViewBuilder:
        """Register an extraction builder from spec.

        Parameters
        ----------
        spec
            Extractor specification.
        file_contexts
            Optional file contexts for extraction.

        Returns
        -------
        ExtractionViewBuilder
            Registered builder instance.
        """
        from extract.builders.factory import builder_from_spec

        builder = builder_from_spec(spec, file_contexts=file_contexts)
        self._builders[spec.name] = builder
        return builder

    def get(self, name: str) -> ExtractedTableInfo:
        """Get analyzed table by name, executing extraction if needed.

        Tables are lazily analyzed on first access and cached.
        """
        if name in self._tables:
            return self._tables[name]

        if not self.ctx.table_exist(name):
            # Check if we have a builder for this table
            spec = extractor_spec(name)
            if spec.name not in self._builders:
                msg = f"No builder registered for {name!r}"
                raise ValueError(msg)
            builder = self._builders[spec.name]
            with stage_span(
                f"extract.compile.{name}",
                stage="extract",
                scope_name=SCOPE_EXTRACT,
            ):
                df = builder.build(self.ctx)
                self.ctx.register_dataframe(spec.canonical_output(), df)

        df = self.ctx.table(name)
        spec = extractor_spec(name)
        info = ExtractedTableInfo.analyze(name, df, spec)
        self._tables[name] = info
        return info

    def extract(
        self,
        name: str,
        *,
        file_contexts: Sequence[FileContext] | None = None,
    ) -> DataFrame:
        """Execute extraction and return DataFrame.

        Convenience method that registers builder, executes, and returns.
        """
        spec = extractor_spec(name)
        self.register_builder(spec, file_contexts=file_contexts)
        info = self.get(spec.canonical_output())
        return info.df

    def all_extracted(self) -> dict[str, ExtractedTableInfo]:
        """Return all extracted tables."""
        return dict(self._tables)

    def dependency_order(self) -> list[str]:
        """Return extraction order respecting dependencies."""
        from extract.spec_registry import EXTRACTOR_SPECS

        # Topological sort based on required_inputs
        ordered: list[str] = []
        visited: set[str] = set()

        def visit(name: str) -> None:
            if name in visited:
                return
            visited.add(name)
            spec = extractor_spec(name)
            for dep in spec.required_inputs:
                if dep in {s.canonical_output() for s in EXTRACTOR_SPECS}:
                    visit(dep)
            ordered.append(name)

        for spec in EXTRACTOR_SPECS:
            visit(spec.canonical_output())
        return ordered
```

### Target Files
- **Create:** `src/extract/compiler.py`
- **Create:** `src/extract/config.py` (ExtractionConfig dataclass)
- **Modify:** `src/extract/session.py` (integrate compiler)

### Files to Deprecate After Completion
- `src/extract/coordination/materialization.py` (inline into compiler)
- `src/extract/helpers.py` (consolidate re-exports)

### Implementation Checklist
- [ ] Create `ExtractedTableInfo` dataclass with spec and schema metadata
- [ ] Implement `ExtractionCompiler` with builder registry and lazy table analysis
- [ ] Add `register_builder()`, `get()`, `extract()` methods
- [ ] Implement `dependency_order()` for topological sorting
- [ ] Add OpenTelemetry instrumentation with SCOPE_EXTRACT
- [ ] Create `ExtractionConfig` for runtime behavior control
- [ ] Integrate compiler into `ExtractSession`
- [ ] Add tests for compiler operations and dependency ordering

---

## Scope 3: Extraction Schema Semantic Typing

### Goal
Enhance `ExtractionSchemaBuilder` with semantic column typing matching `SemanticSchema` patterns for automatic column discovery and validation.

### Key Architectural Elements

```python
# src/extract/schema.py

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    from datafusion import DataFrame
    from datafusion.expr import Expr


class ExtractionColumnType(Enum):
    """Semantic types for extraction columns.

    Mirrors semantics.schema.SemanticSchema column type detection.
    """

    FILE_ID = auto()
    PATH = auto()
    FILE_SHA256 = auto()
    REPO_ID = auto()
    BSTART = auto()
    BEND = auto()
    LINE_START = auto()
    LINE_END = auto()
    COL_START = auto()
    COL_END = auto()
    TEXT = auto()
    NAME = auto()
    SYMBOL = auto()
    METADATA = auto()
    EVIDENCE = auto()
    UNKNOWN = auto()


@dataclass
class ExtractionColumn:
    """Column with semantic type annotation."""

    name: str
    dtype: pa.DataType
    column_type: ExtractionColumnType
    nullable: bool = True


@dataclass(frozen=True)
class ExtractionSchema:
    """Extraction table schema with semantic type metadata.

    Mirrors semantics.schema.SemanticSchema pattern for
    column discovery by semantic type.
    """

    table_name: str
    columns: tuple[ExtractionColumn, ...]

    @classmethod
    def from_df(cls, df: DataFrame, *, table_name: str) -> ExtractionSchema:
        """Analyze DataFrame and infer semantic column types."""
        arrow_schema = df.schema()
        columns: list[ExtractionColumn] = []
        for field in arrow_schema:
            col_type = _infer_column_type(field.name, field.type)
            columns.append(
                ExtractionColumn(
                    name=field.name,
                    dtype=field.type,
                    column_type=col_type,
                    nullable=field.nullable,
                )
            )
        return cls(table_name=table_name, columns=tuple(columns))

    def has_file_identity(self) -> bool:
        """Check if schema has file identity columns."""
        required = {ExtractionColumnType.FILE_ID, ExtractionColumnType.PATH}
        present = {c.column_type for c in self.columns}
        return required.issubset(present)

    def has_byte_span(self) -> bool:
        """Check if schema has byte span columns."""
        required = {ExtractionColumnType.BSTART, ExtractionColumnType.BEND}
        present = {c.column_type for c in self.columns}
        return required.issubset(present)

    def require_column(self, col_type: ExtractionColumnType) -> ExtractionColumn:
        """Get column by type, raising if missing."""
        for col in self.columns:
            if col.column_type == col_type:
                return col
        msg = f"Schema {self.table_name!r} missing required column type: {col_type.name}"
        raise ValueError(msg)

    def file_id_expr(self) -> Expr:
        """Build file_id expression."""
        from datafusion import col

        return col(self.require_column(ExtractionColumnType.FILE_ID).name)

    def path_expr(self) -> Expr:
        """Build path expression."""
        from datafusion import col

        return col(self.require_column(ExtractionColumnType.PATH).name)

    def span_start_expr(self) -> Expr:
        """Build span start expression."""
        from datafusion import col

        return col(self.require_column(ExtractionColumnType.BSTART).name)

    def span_end_expr(self) -> Expr:
        """Build span end expression."""
        from datafusion import col

        return col(self.require_column(ExtractionColumnType.BEND).name)


def _infer_column_type(name: str, dtype: pa.DataType) -> ExtractionColumnType:
    """Infer semantic column type from name and dtype."""
    name_lower = name.lower()

    # Identity columns
    if name_lower == "file_id":
        return ExtractionColumnType.FILE_ID
    if name_lower == "path":
        return ExtractionColumnType.PATH
    if name_lower in {"file_sha256", "sha256"}:
        return ExtractionColumnType.FILE_SHA256
    if name_lower in {"repo_id", "repo"}:
        return ExtractionColumnType.REPO_ID

    # Span columns
    if name_lower in {"bstart", "byte_start", "start_byte"}:
        return ExtractionColumnType.BSTART
    if name_lower in {"bend", "byte_end", "end_byte"}:
        return ExtractionColumnType.BEND
    if name_lower in {"start_line", "line_start", "line0"}:
        return ExtractionColumnType.LINE_START
    if name_lower in {"end_line", "line_end"}:
        return ExtractionColumnType.LINE_END
    if name_lower in {"start_col", "col_start", "col"}:
        return ExtractionColumnType.COL_START
    if name_lower in {"end_col", "col_end"}:
        return ExtractionColumnType.COL_END

    # Content columns
    if "text" in name_lower or "content" in name_lower:
        return ExtractionColumnType.TEXT
    if name_lower in {"name", "identifier"}:
        return ExtractionColumnType.NAME
    if "symbol" in name_lower:
        return ExtractionColumnType.SYMBOL

    # Metadata
    if name_lower.endswith("_metadata") or name_lower == "attrs":
        return ExtractionColumnType.METADATA

    return ExtractionColumnType.UNKNOWN
```

### Target Files
- **Create:** `src/extract/schema.py`
- **Modify:** `src/extract/schema_derivation.py` (integrate with ExtractionSchema)
- **Modify:** `src/extract/row_builder.py` (use ExtractionColumnType)

### Files to Deprecate After Completion
- None (enhancement only)

### Implementation Checklist
- [ ] Create `ExtractionColumnType` enum with all semantic types
- [ ] Create `ExtractionColumn` dataclass with type annotation
- [ ] Implement `ExtractionSchema` with column discovery methods
- [ ] Add `from_df()` classmethod for DataFrame analysis
- [ ] Implement validation predicates: `has_file_identity()`, `has_byte_span()`
- [ ] Add expression generators: `file_id_expr()`, `path_expr()`, `span_*_expr()`
- [ ] Integrate with `ExtractionSchemaBuilder` in schema_derivation.py
- [ ] Add tests for column type inference and schema validation

---

## Scope 4: Extraction Catalog with Metadata

### Goal
Build an `ExtractionCatalog` class matching `SemanticCatalog` for registering extractors with fingerprints, schemas, statistics, and dependency tracking.

### Key Architectural Elements

```python
# src/extract/catalog/__init__.py

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from extract.builders.protocol import ExtractionViewBuilder
    from extract.schema import ExtractionSchema
    from extract.spec_registry import ExtractorSpec


@dataclass
class ExtractionCatalogEntry:
    """Entry in the extraction catalog.

    Mirrors semantics.catalog.catalog.CatalogEntry pattern.
    """

    name: str
    spec: ExtractorSpec
    builder: ExtractionViewBuilder | None = None
    schema: ExtractionSchema | None = None
    plan_fingerprint: str | None = None
    plan_identity_hash: str | None = None
    row_count: int | None = None
    file_count: int | None = None
    last_extracted_at: str | None = None
    cache_hit: bool = False


@dataclass
class ExtractionCatalog:
    """Catalog of registered extractors with metadata.

    Mirrors semantics.catalog.catalog.SemanticCatalog pattern.
    """

    entries: dict[str, ExtractionCatalogEntry] = field(default_factory=dict)

    def register(
        self,
        spec: ExtractorSpec,
        *,
        builder: ExtractionViewBuilder | None = None,
        overwrite: bool = False,
    ) -> ExtractionCatalogEntry:
        """Register an extractor in the catalog.

        Parameters
        ----------
        spec
            Extractor specification.
        builder
            Optional builder instance.
        overwrite
            Whether to overwrite existing entry.

        Returns
        -------
        ExtractionCatalogEntry
            Registered entry.

        Raises
        ------
        ValueError
            If entry exists and overwrite is False.
        """
        name = spec.canonical_output()
        if name in self.entries and not overwrite:
            msg = f"Extractor {name!r} already registered. Use overwrite=True to replace."
            raise ValueError(msg)
        entry = ExtractionCatalogEntry(name=name, spec=spec, builder=builder)
        self.entries[name] = entry
        return entry

    def get(self, name: str) -> ExtractionCatalogEntry | None:
        """Look up entry by name."""
        return self.entries.get(name)

    def require(self, name: str) -> ExtractionCatalogEntry:
        """Look up entry by name, raising if missing."""
        entry = self.get(name)
        if entry is None:
            msg = f"Extractor {name!r} not registered in catalog."
            raise KeyError(msg)
        return entry

    def update_fingerprint(
        self,
        name: str,
        *,
        plan_fingerprint: str,
        plan_identity_hash: str | None = None,
    ) -> None:
        """Update plan fingerprint for incremental detection."""
        entry = self.require(name)
        entry.plan_fingerprint = plan_fingerprint
        entry.plan_identity_hash = plan_identity_hash

    def update_schema(self, name: str, schema: ExtractionSchema) -> None:
        """Update schema after extraction."""
        entry = self.require(name)
        entry.schema = schema

    def update_stats(
        self,
        name: str,
        *,
        row_count: int | None = None,
        file_count: int | None = None,
    ) -> None:
        """Update statistics after extraction."""
        entry = self.require(name)
        if row_count is not None:
            entry.row_count = row_count
        if file_count is not None:
            entry.file_count = file_count

    def entries_by_tier(self, tier: int) -> list[ExtractionCatalogEntry]:
        """Return entries at a specific evidence tier."""
        return [e for e in self.entries.values() if e.spec.evidence_tier == tier]

    def topological_order(self) -> list[str]:
        """Return extraction order respecting dependencies."""
        ordered: list[str] = []
        visited: set[str] = set()

        def visit(name: str) -> None:
            if name in visited:
                return
            entry = self.get(name)
            if entry is None:
                return
            visited.add(name)
            for dep in entry.spec.required_inputs:
                visit(dep)
            ordered.append(name)

        for name in self.entries:
            visit(name)
        return ordered

    def dependency_graph(self) -> dict[str, list[str]]:
        """Return adjacency list of dependencies."""
        graph: dict[str, list[str]] = {}
        for name, entry in self.entries.items():
            graph[name] = list(entry.spec.required_inputs)
        return graph


# Module-level singleton for global access
EXTRACTION_CATALOG: ExtractionCatalog = ExtractionCatalog()
```

### Target Files
- **Create:** `src/extract/catalog/__init__.py`
- **Create:** `src/extract/catalog/entry.py`
- **Create:** `src/extract/catalog/catalog.py`

### Files to Deprecate After Completion
- `src/extract/coordination/evidence_plan.py` (consolidate evidence planning into catalog)

### Implementation Checklist
- [ ] Create `ExtractionCatalogEntry` with all metadata fields
- [ ] Implement `ExtractionCatalog` with register/get/require methods
- [ ] Add fingerprint and schema update methods
- [ ] Implement `topological_order()` for dependency-driven execution
- [ ] Add `dependency_graph()` for lineage visualization
- [ ] Create module-level `EXTRACTION_CATALOG` singleton
- [ ] Add tests for catalog operations and dependency ordering

---

## Scope 5: Canonical Naming Policy

### Goal
Centralize extraction output naming with versioned suffixes matching `semantics.naming` patterns.

### Key Architectural Elements

```python
# src/extract/naming.py

from __future__ import annotations

from typing import Literal

# Canonical extraction output names with _v1 versioning
EXTRACTION_OUTPUT_NAMES: dict[str, str] = {
    # Repository scanning
    "repo_files": "repo_files_v1",
    "repo_scope_manifest": "repo_scope_manifest_v1",
    # AST extraction
    "ast_files": "ast_files_v1",
    "ast_nodes": "ast_nodes_v1",
    # CST extraction
    "libcst_files": "libcst_files_v1",
    "cst_refs": "cst_refs_v1",
    "cst_defs": "cst_defs_v1",
    "cst_imports": "cst_imports_v1",
    "cst_callsites": "cst_callsites_v1",
    "cst_call_args": "cst_call_args_v1",
    "cst_docstrings": "cst_docstrings_v1",
    "cst_decorators": "cst_decorators_v1",
    "cst_type_exprs": "cst_type_exprs_v1",
    # SCIP extraction
    "scip_index": "scip_index_v1",
    "scip_occurrences": "scip_occurrences_v1",
    "scip_documents": "scip_documents_v1",
    "scip_symbols": "scip_symbols_v1",
    # Symtable extraction
    "symtable_files": "symtable_files_v1",
    "symtable_scopes": "symtable_scopes_v1",
    "symtable_symbols": "symtable_symbols_v1",
    # Bytecode extraction
    "bytecode_files": "bytecode_files_v1",
    "bytecode_instructions": "bytecode_instructions_v1",
    "bytecode_cfg": "bytecode_cfg_v1",
    # Tree-sitter extraction
    "tree_sitter_files": "tree_sitter_files_v1",
    "tree_sitter_nodes": "tree_sitter_nodes_v1",
    "tree_sitter_captures": "tree_sitter_captures_v1",
    # Line index
    "file_line_index": "file_line_index_v1",
    # Unified imports
    "python_imports": "python_imports_v1",
    # External interfaces
    "python_external": "python_external_v1",
}

# All canonical extraction view names
EXTRACTION_VIEW_NAMES: tuple[str, ...] = tuple(EXTRACTION_OUTPUT_NAMES.values())


def canonical_output_name(name: str) -> str:
    """Return canonical output name with _v1 suffix.

    Parameters
    ----------
    name
        Internal extractor name or already-canonical name.

    Returns
    -------
    str
        Canonical name with _v1 suffix.

    Examples
    --------
    >>> canonical_output_name("cst_refs")
    'cst_refs_v1'
    >>> canonical_output_name("cst_refs_v1")
    'cst_refs_v1'
    """
    if name in EXTRACTION_OUTPUT_NAMES:
        return EXTRACTION_OUTPUT_NAMES[name]
    if name.endswith("_v1"):
        return name
    return f"{name}_v1"


def is_extraction_output(name: str) -> bool:
    """Check if name is a canonical extraction output."""
    return name in EXTRACTION_VIEW_NAMES or canonical_output_name(name) in EXTRACTION_VIEW_NAMES


def internal_name(canonical: str) -> str:
    """Convert canonical name to internal name.

    Parameters
    ----------
    canonical
        Canonical output name with _v1 suffix.

    Returns
    -------
    str
        Internal name without version suffix.
    """
    if canonical.endswith("_v1"):
        base = canonical[:-3]
        if base in EXTRACTION_OUTPUT_NAMES:
            return base
    for internal, canonical_name in EXTRACTION_OUTPUT_NAMES.items():
        if canonical_name == canonical:
            return internal
    return canonical
```

### Target Files
- **Create:** `src/extract/naming.py`
- **Modify:** `src/extract/__init__.py` (export naming functions)
- **Modify:** `src/extract/spec_registry.py` (use canonical_output_name)
- **Modify:** All extractors to use canonical naming

### Files to Deprecate After Completion
- None (new module)

### Implementation Checklist
- [ ] Create `EXTRACTION_OUTPUT_NAMES` mapping all internal → canonical names
- [ ] Define `EXTRACTION_VIEW_NAMES` tuple for validation
- [ ] Implement `canonical_output_name()` function
- [ ] Implement `is_extraction_output()` predicate
- [ ] Implement `internal_name()` reverse lookup
- [ ] Update all extractors to use canonical naming
- [ ] Update `ExtractorSpec` to use canonical_output_name()
- [ ] Add tests for naming consistency

---

## Scope 6: CDF-Aware Incremental Extraction

### Goal
Enable Delta Lake Change Data Feed (CDF) integration for incremental extraction, matching the pattern in `semantics.pipeline._resolve_cdf_inputs()`.

### Key Architectural Elements

```python
# src/extract/incremental/__init__.py

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from extract.catalog import ExtractionCatalog


@dataclass(frozen=True)
class IncrementalExtractionConfig:
    """Configuration for incremental extraction.

    Mirrors semantics.runtime.SemanticRuntimeConfig CDF options.
    """

    cdf_enabled: bool = False
    cdf_cursor_store: str | None = None
    fingerprint_check: bool = True
    skip_unchanged: bool = True


@dataclass(frozen=True)
class IncrementalExtractionState:
    """State for tracking incremental extraction."""

    last_fingerprints: dict[str, str]
    last_row_counts: dict[str, int]
    cdf_cursors: dict[str, str]


def resolve_extraction_input_mapping(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
    use_cdf: bool | None,
    cdf_inputs: Mapping[str, str] | None,
    catalog: ExtractionCatalog,
) -> tuple[dict[str, str], bool]:
    """Resolve extraction input mapping with CDF support.

    Mirrors semantics.pipeline._resolve_semantic_input_mapping pattern.

    Parameters
    ----------
    ctx
        DataFusion session context.
    runtime_profile
        Runtime profile for CDF registration.
    use_cdf
        Whether to enable CDF inputs.
    cdf_inputs
        Optional explicit CDF input overrides.
    catalog
        Extraction catalog for dependency resolution.

    Returns
    -------
    tuple[dict[str, str], bool]
        Mapping of canonical names to registered table names, and CDF enabled flag.
    """
    resolved_inputs: dict[str, str] = {}
    for name in catalog.entries:
        resolved_inputs[name] = name

    if use_cdf is False:
        return resolved_inputs, False

    if runtime_profile is None:
        if use_cdf:
            msg = "CDF input registration requires a runtime profile."
            raise ValueError(msg)
        return dict(cdf_inputs) if cdf_inputs else resolved_inputs, False

    # Check if CDF is available for any inputs
    from datafusion_engine.dataset.registry import resolve_datafusion_provider

    cdf_candidates: list[str] = []
    for name in resolved_inputs:
        location = runtime_profile.dataset_location(name)
        if location is None:
            continue
        if location.delta_cdf_options is not None:
            cdf_candidates.append(name)
        elif resolve_datafusion_provider(location) == "delta_cdf":
            cdf_candidates.append(name)

    if not cdf_candidates:
        return dict(cdf_inputs) if cdf_inputs else resolved_inputs, False

    # Register CDF inputs
    from datafusion_engine.session.runtime import register_cdf_inputs_for_profile

    cdf_mapping = register_cdf_inputs_for_profile(
        runtime_profile,
        ctx,
        table_names=cdf_candidates,
    )
    if cdf_inputs:
        cdf_mapping = {**cdf_mapping, **cdf_inputs}

    for canonical, source in list(resolved_inputs.items()):
        override = cdf_mapping.get(source) or cdf_mapping.get(canonical)
        if override is not None:
            resolved_inputs[canonical] = override

    return resolved_inputs, True


def check_extraction_fingerprints(
    catalog: ExtractionCatalog,
    previous_state: IncrementalExtractionState | None,
) -> dict[str, bool]:
    """Check which extractions need re-running based on fingerprints.

    Returns
    -------
    dict[str, bool]
        Mapping of extractor names to whether they need re-running.
    """
    needs_rerun: dict[str, bool] = {}
    if previous_state is None:
        for name in catalog.entries:
            needs_rerun[name] = True
        return needs_rerun

    for name, entry in catalog.entries.items():
        if entry.plan_fingerprint is None:
            needs_rerun[name] = True
            continue
        previous = previous_state.last_fingerprints.get(name)
        needs_rerun[name] = previous != entry.plan_fingerprint

    return needs_rerun
```

### Target Files
- **Create:** `src/extract/incremental/__init__.py`
- **Create:** `src/extract/incremental/cdf.py`
- **Create:** `src/extract/incremental/fingerprint.py`
- **Modify:** `src/extract/compiler.py` (integrate incremental support)

### Files to Deprecate After Completion
- `src/extract/infrastructure/worklists.py` (consolidate into incremental module)

### Implementation Checklist
- [ ] Create `IncrementalExtractionConfig` for CDF settings
- [ ] Create `IncrementalExtractionState` for tracking state
- [ ] Implement `resolve_extraction_input_mapping()` with CDF support
- [ ] Implement `check_extraction_fingerprints()` for change detection
- [ ] Add cursor management for Delta Lake CDF
- [ ] Integrate into ExtractionCompiler
- [ ] Add tests for incremental extraction scenarios

---

## Scope 7: Validation and Quality Layers

### Goal
Add validation layers for extraction completeness and quality, matching `semantics.validation` patterns.

### Key Architectural Elements

```python
# src/extract/validation/__init__.py

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion import SessionContext

    from extract.catalog import ExtractionCatalog
    from extract.schema import ExtractionSchema


@dataclass(frozen=True)
class ExtractionValidationResult:
    """Result of extraction validation."""

    valid: bool
    missing_tables: tuple[str, ...]
    schema_errors: tuple[str, ...]
    warnings: tuple[str, ...]


def require_extraction_inputs(
    ctx: SessionContext,
    *,
    required: Sequence[str],
    input_mapping: dict[str, str] | None = None,
) -> dict[str, str]:
    """Validate required extraction inputs are registered.

    Mirrors semantics.validation.require_semantic_inputs pattern.

    Parameters
    ----------
    ctx
        DataFusion session context.
    required
        Required extraction table names.
    input_mapping
        Optional mapping of canonical to actual table names.

    Returns
    -------
    dict[str, str]
        Validated mapping of canonical to actual names.

    Raises
    ------
    ValueError
        If required inputs are missing.
    """
    mapping = dict(input_mapping or {})
    missing: list[str] = []

    for name in required:
        actual = mapping.get(name, name)
        if not ctx.table_exist(actual):
            missing.append(name)
        else:
            mapping[name] = actual

    if missing:
        msg = f"Required extraction inputs missing: {sorted(missing)}"
        raise ValueError(msg)

    return mapping


def validate_extraction_schema(
    schema: ExtractionSchema,
    *,
    require_file_identity: bool = True,
    require_byte_span: bool = False,
) -> ExtractionValidationResult:
    """Validate extraction schema meets requirements.

    Parameters
    ----------
    schema
        Extraction schema to validate.
    require_file_identity
        Whether file_id and path are required.
    require_byte_span
        Whether bstart and bend are required.

    Returns
    -------
    ExtractionValidationResult
        Validation result with any errors.
    """
    errors: list[str] = []
    warnings: list[str] = []

    if require_file_identity and not schema.has_file_identity():
        errors.append(f"Schema {schema.table_name!r} missing file identity columns")

    if require_byte_span and not schema.has_byte_span():
        errors.append(f"Schema {schema.table_name!r} missing byte span columns")

    return ExtractionValidationResult(
        valid=len(errors) == 0,
        missing_tables=(),
        schema_errors=tuple(errors),
        warnings=tuple(warnings),
    )


def validate_extraction_completeness(
    catalog: ExtractionCatalog,
    *,
    required_outputs: Sequence[str] | None = None,
) -> ExtractionValidationResult:
    """Validate extraction catalog completeness.

    Parameters
    ----------
    catalog
        Extraction catalog to validate.
    required_outputs
        Optional list of required output names.

    Returns
    -------
    ExtractionValidationResult
        Validation result.
    """
    from extract.naming import EXTRACTION_VIEW_NAMES

    required = set(required_outputs or EXTRACTION_VIEW_NAMES)
    registered = set(catalog.entries.keys())
    missing = required - registered

    errors: list[str] = []
    warnings: list[str] = []

    if missing:
        errors.append(f"Missing required extractions: {sorted(missing)}")

    # Check for schema errors in registered entries
    for name, entry in catalog.entries.items():
        if entry.schema is None:
            warnings.append(f"Extraction {name!r} has no schema metadata")
        elif not entry.schema.has_file_identity():
            errors.append(f"Extraction {name!r} missing file identity columns")

    return ExtractionValidationResult(
        valid=len(errors) == 0,
        missing_tables=tuple(missing),
        schema_errors=tuple(errors),
        warnings=tuple(warnings),
    )
```

### Target Files
- **Create:** `src/extract/validation/__init__.py`
- **Create:** `src/extract/validation/inputs.py`
- **Create:** `src/extract/validation/schema.py`
- **Modify:** `src/extract/compiler.py` (add validation hooks)

### Files to Deprecate After Completion
- None (new module)

### Implementation Checklist
- [ ] Create `ExtractionValidationResult` dataclass
- [ ] Implement `require_extraction_inputs()` for input validation
- [ ] Implement `validate_extraction_schema()` for schema validation
- [ ] Implement `validate_extraction_completeness()` for catalog validation
- [ ] Add validation hooks to ExtractionCompiler
- [ ] Add graceful degradation support for optional extractions
- [ ] Add tests for all validation scenarios

---

## Scope 8: Consolidate Coordination Layer

### Goal
Deprecate legacy coordination modules by consolidating into the new compiler, catalog, and spec registry patterns.

### Key Architectural Elements

```python
# src/extract/pipeline.py

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from obs.otel.scopes import SCOPE_EXTRACT
from obs.otel.tracing import stage_span

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from extract.catalog import ExtractionCatalog
    from extract.compiler import ExtractionCompiler
    from extract.coordination.context import FileContext


@dataclass(frozen=True)
class ExtractionBuildOptions:
    """Options for extraction pipeline execution.

    Mirrors semantics.pipeline.CpgBuildOptions pattern.
    """

    extractors: Sequence[str] | None = None
    parallel: bool = True
    cache_enabled: bool = True
    validate_schemas: bool = True
    materialize_outputs: bool = True
    incremental: bool = False


@dataclass(frozen=True)
class ExtractionBuildResult:
    """Result of extraction pipeline execution."""

    catalog: ExtractionCatalog
    extracted: tuple[str, ...]
    skipped: tuple[str, ...]
    errors: tuple[str, ...]


def build_extraction(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    file_contexts: Sequence[FileContext],
    options: ExtractionBuildOptions | None = None,
) -> ExtractionBuildResult:
    """Build extraction outputs from file contexts.

    Mirrors semantics.pipeline.build_cpg pattern.

    Parameters
    ----------
    ctx
        DataFusion session context.
    runtime_profile
        Runtime profile for execution.
    file_contexts
        File contexts to extract from.
    options
        Optional build settings.

    Returns
    -------
    ExtractionBuildResult
        Result with catalog and statistics.
    """
    resolved = options or ExtractionBuildOptions()

    with stage_span(
        "extract.build_extraction",
        stage="extract",
        scope_name=SCOPE_EXTRACT,
        attributes={
            "codeanatomy.file_count": len(file_contexts),
            "codeanatomy.parallel": resolved.parallel,
            "codeanatomy.incremental": resolved.incremental,
        },
    ):
        from extract.catalog import ExtractionCatalog
        from extract.compiler import ExtractionCompiler
        from extract.spec_registry import EXTRACTOR_SPECS, extractor_spec
        from extract.validation import require_extraction_inputs

        compiler = ExtractionCompiler(ctx, runtime_profile=runtime_profile)
        catalog = ExtractionCatalog()

        # Determine which extractors to run
        if resolved.extractors is not None:
            specs = [extractor_spec(name) for name in resolved.extractors]
        else:
            specs = list(EXTRACTOR_SPECS)

        # Register and execute extractors in dependency order
        extracted: list[str] = []
        skipped: list[str] = []
        errors: list[str] = []

        for spec in specs:
            try:
                catalog.register(spec)
                compiler.register_builder(spec, file_contexts=file_contexts)
                info = compiler.get(spec.canonical_output())
                catalog.update_schema(spec.canonical_output(), info.schema)
                extracted.append(spec.canonical_output())
            except Exception as e:
                errors.append(f"{spec.name}: {e}")

        if resolved.validate_schemas:
            from extract.validation import validate_extraction_completeness

            validation = validate_extraction_completeness(catalog)
            if not validation.valid:
                for error in validation.schema_errors:
                    errors.append(error)

        return ExtractionBuildResult(
            catalog=catalog,
            extracted=tuple(extracted),
            skipped=tuple(skipped),
            errors=tuple(errors),
        )
```

### Target Files
- **Create:** `src/extract/pipeline.py`
- **Modify:** `src/extract/__init__.py` (export new pipeline)

### Files to Deprecate After Completion
- `src/extract/coordination/materialization.py` → inline into pipeline.py
- `src/extract/coordination/evidence_plan.py` → consolidate into catalog
- `src/extract/coordination/spec_helpers.py` → consolidate into spec_registry
- `src/extract/coordination/schema_ops.py` → consolidate into schema.py
- `src/extract/helpers.py` → remove re-exports

### Implementation Checklist
- [ ] Create `ExtractionBuildOptions` matching CpgBuildOptions pattern
- [ ] Create `ExtractionBuildResult` for pipeline output
- [ ] Implement `build_extraction()` as main entry point
- [ ] Add OpenTelemetry instrumentation
- [ ] Integrate compiler, catalog, validation in pipeline
- [ ] Add parallel execution support
- [ ] Add incremental extraction support
- [ ] Migrate all callers to new pipeline
- [ ] Delete deprecated coordination modules
- [ ] Add comprehensive tests for pipeline

---

## Final File Summary

### Created (16 files)
- `src/extract/spec_registry.py`
- `src/extract/builders/__init__.py`
- `src/extract/builders/protocol.py`
- `src/extract/builders/factory.py`
- `src/extract/compiler.py`
- `src/extract/config.py`
- `src/extract/schema.py`
- `src/extract/catalog/__init__.py`
- `src/extract/catalog/entry.py`
- `src/extract/catalog/catalog.py`
- `src/extract/naming.py`
- `src/extract/incremental/__init__.py`
- `src/extract/incremental/cdf.py`
- `src/extract/incremental/fingerprint.py`
- `src/extract/validation/__init__.py`
- `src/extract/pipeline.py`

### Modified (8+ files)
- `src/extract/__init__.py`
- `src/extract/session.py`
- `src/extract/schema_derivation.py`
- `src/extract/row_builder.py`
- `src/extract/extractors/ast_extract.py`
- `src/extract/extractors/cst_extract.py`
- `src/extract/extractors/scip/extract.py`
- All other extractors for canonical naming

### Deprecated/Deleted (6 files)
- `src/extract/coordination/materialization.py`
- `src/extract/coordination/evidence_plan.py`
- `src/extract/coordination/spec_helpers.py`
- `src/extract/coordination/schema_ops.py`
- `src/extract/infrastructure/worklists.py`
- `src/extract/helpers.py`

---

## Verification Commands

```bash
# Full quality gates
uv run ruff check --fix
uv run pyrefly check
uv run pyright --warnings --pythonversion=3.13

# Unit tests
uv run pytest tests/unit/extract/ -v
uv run pytest tests/unit/datafusion_engine/extract/ -v

# Integration tests
uv run pytest tests/integration/test_extraction_pipeline.py -v
uv run pytest tests/integration/ -v -k "extract"

# Full test suite (excluding e2e)
uv run pytest tests/ -m "not e2e" -v

# Verify no SQL strings in extraction (DataFusion-native)
rg "ctx\.sql|SELECT|FROM" src/extract/
```

---

## Implementation Order

1. **Scope 5** (Naming) - Foundation for canonical references
2. **Scope 1** (Spec Registry) - Foundation for declarative patterns
3. **Scope 3** (Schema) - Required for catalog metadata
4. **Scope 4** (Catalog) - Depends on schema + naming
5. **Scope 2** (Compiler) - Depends on spec registry + catalog
6. **Scope 7** (Validation) - Depends on schema + catalog
7. **Scope 6** (Incremental) - Depends on catalog + compiler
8. **Scope 8** (Pipeline) - Final consolidation

---

## Risk Mitigation

1. **Backward Compatibility**: Keep deprecated modules until all callers migrate
2. **Incremental Rollout**: Enable new patterns via feature flags
3. **Schema Stability**: Maintain existing output schemas during migration
4. **Test Coverage**: Add comprehensive tests before deprecating old code
