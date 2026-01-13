"""Extract Python bytecode artifacts into Arrow tables using shared helpers."""

from __future__ import annotations

import dis
import types as pytypes
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Literal, Required, TypedDict, Unpack, cast, overload

import pyarrow as pa

from arrowdsl.compute.expr_specs import MaskedHashExprSpec
from arrowdsl.core.context import ExecutionContext, OrderingLevel, execution_context_factory
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.query import ProjectionSpec, QuerySpec
from arrowdsl.plan.rows import plan_from_rows
from arrowdsl.plan.runner import materialize_plan, run_plan_bundle
from arrowdsl.schema.schema import SchemaMetadataSpec, empty_table
from extract.common import file_identity_row, iter_contexts, text_from_file_ctx
from extract.file_context import FileContext
from extract.hash_specs import (
    BC_BLOCK_ID_SPEC,
    BC_CODE_UNIT_ID_SPEC,
    BC_COND_INSTR_ID_SPEC,
    BC_DST_BLOCK_ID_SPEC,
    BC_EDGE_ID_SPEC,
    BC_EXC_ENTRY_ID_SPEC,
    BC_INSTR_ID_SPEC,
    BC_PARENT_CODE_UNIT_ID_SPEC,
    BC_SRC_BLOCK_ID_SPEC,
)
from extract.spec_helpers import (
    DatasetRegistration,
    infer_ordering_keys,
    merge_metadata_specs,
    options_metadata_spec,
    ordering_metadata_spec,
    register_dataset,
)
from extract.tables import align_plan, project_columns
from schema_spec.specs import ArrowFieldSpec, file_identity_bundle

type RowValue = str | int | bool | None
type Row = dict[str, RowValue]

SCHEMA_VERSION = 1


@dataclass(frozen=True)
class BytecodeExtractOptions:
    """
    Bytecode extraction options.

    - adaptive=False is the canonical stream for stable semantics across runs
      (per your recipe). :contentReference[oaicite:17]{index=17}
    - include_cfg_derivations implements the blocks/CFG edges derivation recipe.
      :contentReference[oaicite:18]{index=18}
    """

    optimize: int = 0
    dont_inherit: bool = True
    adaptive: bool = False
    include_cfg_derivations: bool = True
    terminator_opnames: Sequence[str] = (
        "RETURN_VALUE",
        "RETURN_CONST",
        "RAISE_VARARGS",
        "RERAISE",
    )


@dataclass(frozen=True)
class BytecodeExtractResult:
    """Extracted bytecode tables for code units, instructions, and edges."""

    py_bc_code_units: TableLike
    py_bc_instructions: TableLike
    py_bc_exception_table: TableLike
    py_bc_blocks: TableLike
    py_bc_cfg_edges: TableLike
    py_bc_errors: TableLike


@dataclass(frozen=True)
class BytecodeFileContext:
    """Per-file context for bytecode extraction."""

    file_ctx: FileContext
    options: BytecodeExtractOptions

    @property
    def file_id(self) -> str:
        """Return the file id for this extraction context.

        Returns
        -------
        str
            File id from the file context.
        """
        return self.file_ctx.file_id

    @property
    def path(self) -> str:
        """Return the file path for this extraction context.

        Returns
        -------
        str
            File path from the file context.
        """
        return self.file_ctx.path

    @property
    def file_sha256(self) -> str | None:
        """Return the file sha256 for this extraction context.

        Returns
        -------
        str | None
            File hash from the file context.
        """
        return self.file_ctx.file_sha256

    def identity_row(self) -> Row:
        """Return the identity row for this context.

        Returns
        -------
        dict[str, object]
            File identity columns for row construction.
        """
        return cast("Row", file_identity_row(self.file_ctx))


@dataclass(frozen=True)
class InstructionData:
    """Instruction data for a compiled code object."""

    instructions: list[dis.Instruction]
    index_by_offset: dict[int, int]


@dataclass(frozen=True)
class CodeUnitKey:
    """Key fields identifying a code unit."""

    qualpath: str
    co_name: str
    firstlineno: int


@dataclass(frozen=True)
class CodeUnitContext:
    """Per-code-unit extraction context."""

    code_unit_key: CodeUnitKey
    file_ctx: BytecodeFileContext
    instruction_data: InstructionData
    exc_entries: Sequence[object]
    code_len: int


@dataclass(frozen=True)
class CfgEdgeSpec:
    """CFG edge descriptor."""

    src_block_start: int
    src_block_end: int
    dst_block_start: int
    dst_block_end: int
    kind: str
    edge_key: str
    cond_instr_index: int | None
    cond_instr_offset: int | None
    exc_index: int | None


@dataclass(frozen=True)
class _CfgEdgeContext:
    """Context data for CFG edge derivation."""

    unit_ctx: CodeUnitContext
    instructions: Sequence[dis.Instruction]
    index_by_offset: Mapping[int, int]
    terminators: Sequence[str]
    off_to_block: Mapping[int, tuple[int, int]]
    edge_rows: list[Row]


@dataclass(frozen=True)
class _BlockEdgeContext:
    """Per-block inputs for CFG edge derivation."""

    src_start: int
    src_end: int
    end: int
    last: dis.Instruction
    last_index: int


@dataclass
class BytecodeRowBuffers:
    """Mutable row buffers for bytecode extraction."""

    code_unit_rows: list[Row]
    instruction_rows: list[Row]
    exception_rows: list[Row]
    block_rows: list[Row]
    edge_rows: list[Row]
    error_rows: list[Row]


_CODE_UNITS_FIELDS = [
    ArrowFieldSpec(name="code_unit_id", dtype=pa.string()),
    ArrowFieldSpec(name="parent_code_unit_id", dtype=pa.string()),
    ArrowFieldSpec(name="qualpath", dtype=pa.string()),
    ArrowFieldSpec(name="co_name", dtype=pa.string()),
    ArrowFieldSpec(name="firstlineno", dtype=pa.int32()),
    ArrowFieldSpec(name="argcount", dtype=pa.int32()),
    ArrowFieldSpec(name="posonlyargcount", dtype=pa.int32()),
    ArrowFieldSpec(name="kwonlyargcount", dtype=pa.int32()),
    ArrowFieldSpec(name="nlocals", dtype=pa.int32()),
    ArrowFieldSpec(name="flags", dtype=pa.int32()),
    ArrowFieldSpec(name="stacksize", dtype=pa.int32()),
    ArrowFieldSpec(name="code_len", dtype=pa.int32()),
]

_INSTR_FIELDS = [
    ArrowFieldSpec(name="instr_id", dtype=pa.string()),
    ArrowFieldSpec(name="code_unit_id", dtype=pa.string()),
    ArrowFieldSpec(name="instr_index", dtype=pa.int32()),
    ArrowFieldSpec(name="offset", dtype=pa.int32()),
    ArrowFieldSpec(name="opname", dtype=pa.string()),
    ArrowFieldSpec(name="opcode", dtype=pa.int32()),
    ArrowFieldSpec(name="arg", dtype=pa.int32()),
    ArrowFieldSpec(name="argval_str", dtype=pa.string()),
    ArrowFieldSpec(name="argrepr", dtype=pa.string()),
    ArrowFieldSpec(name="is_jump_target", dtype=pa.bool_()),
    ArrowFieldSpec(name="jump_target_offset", dtype=pa.int32()),
    ArrowFieldSpec(name="starts_line", dtype=pa.int32()),
    ArrowFieldSpec(name="pos_start_line", dtype=pa.int32()),
    ArrowFieldSpec(name="pos_end_line", dtype=pa.int32()),
    ArrowFieldSpec(name="pos_start_col", dtype=pa.int32()),
    ArrowFieldSpec(name="pos_end_col", dtype=pa.int32()),
]

_EXC_FIELDS = [
    ArrowFieldSpec(name="exc_entry_id", dtype=pa.string()),
    ArrowFieldSpec(name="code_unit_id", dtype=pa.string()),
    ArrowFieldSpec(name="exc_index", dtype=pa.int32()),
    ArrowFieldSpec(name="start_offset", dtype=pa.int32()),
    ArrowFieldSpec(name="end_offset", dtype=pa.int32()),
    ArrowFieldSpec(name="target_offset", dtype=pa.int32()),
    ArrowFieldSpec(name="depth", dtype=pa.int32()),
    ArrowFieldSpec(name="lasti", dtype=pa.bool_()),
]

_BLOCKS_FIELDS = [
    ArrowFieldSpec(name="block_id", dtype=pa.string()),
    ArrowFieldSpec(name="code_unit_id", dtype=pa.string()),
    ArrowFieldSpec(name="start_offset", dtype=pa.int32()),
    ArrowFieldSpec(name="end_offset", dtype=pa.int32()),
    ArrowFieldSpec(name="kind", dtype=pa.string()),
]

_CFG_EDGES_FIELDS = [
    ArrowFieldSpec(name="edge_id", dtype=pa.string()),
    ArrowFieldSpec(name="code_unit_id", dtype=pa.string()),
    ArrowFieldSpec(name="src_block_id", dtype=pa.string()),
    ArrowFieldSpec(name="dst_block_id", dtype=pa.string()),
    ArrowFieldSpec(name="kind", dtype=pa.string()),
    ArrowFieldSpec(name="cond_instr_id", dtype=pa.string()),
    ArrowFieldSpec(name="exc_index", dtype=pa.int32()),
]

_ERRORS_FIELDS = [
    ArrowFieldSpec(name="error_type", dtype=pa.string()),
    ArrowFieldSpec(name="message", dtype=pa.string()),
]

_BC_METADATA_EXTRA = {
    b"extractor_name": b"bytecode",
    b"extractor_version": str(SCHEMA_VERSION).encode("utf-8"),
}

_CODE_UNITS_BASE_COLUMNS = tuple(
    field.name for field in (*file_identity_bundle().fields, *_CODE_UNITS_FIELDS)
)
_INSTR_BASE_COLUMNS = tuple(
    field.name for field in (*file_identity_bundle().fields, *_INSTR_FIELDS)
)
_EXC_BASE_COLUMNS = tuple(field.name for field in (*file_identity_bundle().fields, *_EXC_FIELDS))
_BLOCKS_BASE_COLUMNS = tuple(
    field.name for field in (*file_identity_bundle().fields, *_BLOCKS_FIELDS)
)
_CFG_EDGES_BASE_COLUMNS = tuple(
    field.name for field in (*file_identity_bundle().fields, *_CFG_EDGES_FIELDS)
)
_ERRORS_BASE_COLUMNS = tuple(
    field.name for field in (*file_identity_bundle().fields, *_ERRORS_FIELDS)
)

_CODE_UNITS_METADATA = ordering_metadata_spec(
    OrderingLevel.IMPLICIT,
    keys=infer_ordering_keys(_CODE_UNITS_BASE_COLUMNS),
    extra=_BC_METADATA_EXTRA,
)
_INSTR_METADATA = ordering_metadata_spec(
    OrderingLevel.IMPLICIT,
    keys=infer_ordering_keys(_INSTR_BASE_COLUMNS),
    extra=_BC_METADATA_EXTRA,
)
_EXC_METADATA = ordering_metadata_spec(
    OrderingLevel.IMPLICIT,
    keys=infer_ordering_keys(_EXC_BASE_COLUMNS),
    extra=_BC_METADATA_EXTRA,
)
_BLOCKS_METADATA = ordering_metadata_spec(
    OrderingLevel.IMPLICIT,
    keys=infer_ordering_keys(_BLOCKS_BASE_COLUMNS),
    extra=_BC_METADATA_EXTRA,
)
_CFG_EDGES_METADATA = ordering_metadata_spec(
    OrderingLevel.IMPLICIT,
    keys=infer_ordering_keys(_CFG_EDGES_BASE_COLUMNS),
    extra=_BC_METADATA_EXTRA,
)
_ERRORS_METADATA = ordering_metadata_spec(
    OrderingLevel.IMPLICIT,
    keys=infer_ordering_keys(_ERRORS_BASE_COLUMNS),
    extra=_BC_METADATA_EXTRA,
)

CODE_UNITS_SPEC = register_dataset(
    name="py_bc_code_units_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(),),
    fields=_CODE_UNITS_FIELDS,
    registration=DatasetRegistration(
        query_spec=QuerySpec(
            projection=ProjectionSpec(
                base=tuple(
                    field.name
                    for field in (*file_identity_bundle().fields, *_CODE_UNITS_FIELDS)
                    if field.name not in {"code_unit_id", "parent_code_unit_id"}
                ),
                derived={
                    "code_unit_id": MaskedHashExprSpec(
                        spec=BC_CODE_UNIT_ID_SPEC,
                        required=("file_id", "qualpath", "firstlineno", "co_name"),
                    ),
                    "parent_code_unit_id": MaskedHashExprSpec(
                        spec=BC_PARENT_CODE_UNIT_ID_SPEC,
                        required=(
                            "file_id",
                            "parent_qualpath",
                            "parent_firstlineno",
                            "parent_co_name",
                        ),
                    ),
                },
            )
        ),
        metadata_spec=_CODE_UNITS_METADATA,
    ),
)

INSTR_SPEC = register_dataset(
    name="py_bc_instructions_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(),),
    fields=_INSTR_FIELDS,
    registration=DatasetRegistration(
        query_spec=QuerySpec(
            projection=ProjectionSpec(
                base=tuple(
                    field.name
                    for field in (*file_identity_bundle().fields, *_INSTR_FIELDS)
                    if field.name not in {"instr_id", "code_unit_id"}
                ),
                derived={
                    "code_unit_id": MaskedHashExprSpec(
                        spec=BC_CODE_UNIT_ID_SPEC,
                        required=("file_id", "qualpath", "firstlineno", "co_name"),
                    ),
                    "instr_id": MaskedHashExprSpec(
                        spec=BC_INSTR_ID_SPEC,
                        required=("code_unit_id", "instr_index", "offset"),
                    ),
                },
            )
        ),
        metadata_spec=_INSTR_METADATA,
    ),
)

EXC_SPEC = register_dataset(
    name="py_bc_exception_table_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(),),
    fields=_EXC_FIELDS,
    registration=DatasetRegistration(
        query_spec=QuerySpec(
            projection=ProjectionSpec(
                base=tuple(
                    field.name
                    for field in (*file_identity_bundle().fields, *_EXC_FIELDS)
                    if field.name not in {"exc_entry_id", "code_unit_id"}
                ),
                derived={
                    "code_unit_id": MaskedHashExprSpec(
                        spec=BC_CODE_UNIT_ID_SPEC,
                        required=("file_id", "qualpath", "firstlineno", "co_name"),
                    ),
                    "exc_entry_id": MaskedHashExprSpec(
                        spec=BC_EXC_ENTRY_ID_SPEC,
                        required=(
                            "code_unit_id",
                            "exc_index",
                            "start_offset",
                            "end_offset",
                            "target_offset",
                        ),
                    ),
                },
            )
        ),
        metadata_spec=_EXC_METADATA,
    ),
)

BLOCKS_SPEC = register_dataset(
    name="py_bc_blocks_v1",
    version=SCHEMA_VERSION,
    bundles=(),
    fields=_BLOCKS_FIELDS,
    registration=DatasetRegistration(
        query_spec=QuerySpec(
            projection=ProjectionSpec(
                base=tuple(
                    field.name
                    for field in _BLOCKS_FIELDS
                    if field.name not in {"block_id", "code_unit_id"}
                ),
                derived={
                    "code_unit_id": MaskedHashExprSpec(
                        spec=BC_CODE_UNIT_ID_SPEC,
                        required=("file_id", "qualpath", "firstlineno", "co_name"),
                    ),
                    "block_id": MaskedHashExprSpec(
                        spec=BC_BLOCK_ID_SPEC,
                        required=("code_unit_id", "start_offset", "end_offset"),
                    ),
                },
            )
        ),
        metadata_spec=_BLOCKS_METADATA,
    ),
)

CFG_EDGES_SPEC = register_dataset(
    name="py_bc_cfg_edges_v1",
    version=SCHEMA_VERSION,
    bundles=(),
    fields=_CFG_EDGES_FIELDS,
    registration=DatasetRegistration(
        query_spec=QuerySpec(
            projection=ProjectionSpec(
                base=tuple(
                    field.name
                    for field in _CFG_EDGES_FIELDS
                    if field.name
                    not in {
                        "edge_id",
                        "code_unit_id",
                        "src_block_id",
                        "dst_block_id",
                        "cond_instr_id",
                    }
                ),
                derived={
                    "code_unit_id": MaskedHashExprSpec(
                        spec=BC_CODE_UNIT_ID_SPEC,
                        required=("file_id", "qualpath", "firstlineno", "co_name"),
                    ),
                    "src_block_id": MaskedHashExprSpec(
                        spec=BC_SRC_BLOCK_ID_SPEC,
                        required=("code_unit_id", "src_block_start", "src_block_end"),
                    ),
                    "dst_block_id": MaskedHashExprSpec(
                        spec=BC_DST_BLOCK_ID_SPEC,
                        required=("code_unit_id", "dst_block_start", "dst_block_end"),
                    ),
                    "cond_instr_id": MaskedHashExprSpec(
                        spec=BC_COND_INSTR_ID_SPEC,
                        required=("code_unit_id", "cond_instr_index", "cond_instr_offset"),
                    ),
                    "edge_id": MaskedHashExprSpec(
                        spec=BC_EDGE_ID_SPEC,
                        required=(
                            "code_unit_id",
                            "src_block_id",
                            "dst_block_id",
                            "kind",
                            "edge_key",
                            "exc_index",
                        ),
                    ),
                },
            )
        ),
        metadata_spec=_CFG_EDGES_METADATA,
    ),
)

ERRORS_SPEC = register_dataset(
    name="py_bc_errors_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(),),
    fields=_ERRORS_FIELDS,
    registration=DatasetRegistration(
        query_spec=QuerySpec(
            projection=ProjectionSpec(
                base=tuple(
                    field.name for field in (*file_identity_bundle().fields, *_ERRORS_FIELDS)
                )
            )
        ),
        metadata_spec=_ERRORS_METADATA,
    ),
)

CODE_UNITS_SCHEMA = CODE_UNITS_SPEC.schema()
INSTR_SCHEMA = INSTR_SPEC.schema()
EXC_SCHEMA = EXC_SPEC.schema()
BLOCKS_SCHEMA = BLOCKS_SPEC.schema()
CFG_EDGES_SCHEMA = CFG_EDGES_SPEC.schema()
ERRORS_SCHEMA = ERRORS_SPEC.schema()


def _bytecode_metadata_specs(
    options: BytecodeExtractOptions,
) -> dict[str, SchemaMetadataSpec]:
    run_meta = options_metadata_spec(options=options)
    return {
        "py_bc_code_units": merge_metadata_specs(_CODE_UNITS_METADATA, run_meta),
        "py_bc_instructions": merge_metadata_specs(_INSTR_METADATA, run_meta),
        "py_bc_exception_table": merge_metadata_specs(_EXC_METADATA, run_meta),
        "py_bc_blocks": merge_metadata_specs(_BLOCKS_METADATA, run_meta),
        "py_bc_cfg_edges": merge_metadata_specs(_CFG_EDGES_METADATA, run_meta),
        "py_bc_errors": merge_metadata_specs(_ERRORS_METADATA, run_meta),
    }


CODE_UNITS_ROWS_SCHEMA = pa.schema(
    [
        pa.field("file_id", pa.string()),
        pa.field("path", pa.string()),
        pa.field("file_sha256", pa.string()),
        pa.field("qualpath", pa.string()),
        pa.field("co_name", pa.string()),
        pa.field("firstlineno", pa.int32()),
        pa.field("parent_qualpath", pa.string()),
        pa.field("parent_co_name", pa.string()),
        pa.field("parent_firstlineno", pa.int32()),
        pa.field("argcount", pa.int32()),
        pa.field("posonlyargcount", pa.int32()),
        pa.field("kwonlyargcount", pa.int32()),
        pa.field("nlocals", pa.int32()),
        pa.field("flags", pa.int32()),
        pa.field("stacksize", pa.int32()),
        pa.field("code_len", pa.int32()),
    ]
)

INSTR_ROWS_SCHEMA = pa.schema(
    [
        pa.field("file_id", pa.string()),
        pa.field("path", pa.string()),
        pa.field("file_sha256", pa.string()),
        pa.field("qualpath", pa.string()),
        pa.field("co_name", pa.string()),
        pa.field("firstlineno", pa.int32()),
        pa.field("instr_index", pa.int32()),
        pa.field("offset", pa.int32()),
        pa.field("opname", pa.string()),
        pa.field("opcode", pa.int32()),
        pa.field("arg", pa.int32()),
        pa.field("argval_str", pa.string()),
        pa.field("argrepr", pa.string()),
        pa.field("is_jump_target", pa.bool_()),
        pa.field("jump_target_offset", pa.int32()),
        pa.field("starts_line", pa.int32()),
        pa.field("pos_start_line", pa.int32()),
        pa.field("pos_end_line", pa.int32()),
        pa.field("pos_start_col", pa.int32()),
        pa.field("pos_end_col", pa.int32()),
    ]
)

EXC_ROWS_SCHEMA = pa.schema(
    [
        pa.field("file_id", pa.string()),
        pa.field("path", pa.string()),
        pa.field("file_sha256", pa.string()),
        pa.field("qualpath", pa.string()),
        pa.field("co_name", pa.string()),
        pa.field("firstlineno", pa.int32()),
        pa.field("exc_index", pa.int32()),
        pa.field("start_offset", pa.int32()),
        pa.field("end_offset", pa.int32()),
        pa.field("target_offset", pa.int32()),
        pa.field("depth", pa.int32()),
        pa.field("lasti", pa.bool_()),
    ]
)

BLOCKS_ROWS_SCHEMA = pa.schema(
    [
        pa.field("file_id", pa.string()),
        pa.field("path", pa.string()),
        pa.field("file_sha256", pa.string()),
        pa.field("qualpath", pa.string()),
        pa.field("co_name", pa.string()),
        pa.field("firstlineno", pa.int32()),
        pa.field("start_offset", pa.int32()),
        pa.field("end_offset", pa.int32()),
        pa.field("kind", pa.string()),
    ]
)

CFG_EDGES_ROWS_SCHEMA = pa.schema(
    [
        pa.field("file_id", pa.string()),
        pa.field("path", pa.string()),
        pa.field("file_sha256", pa.string()),
        pa.field("qualpath", pa.string()),
        pa.field("co_name", pa.string()),
        pa.field("firstlineno", pa.int32()),
        pa.field("src_block_start", pa.int32()),
        pa.field("src_block_end", pa.int32()),
        pa.field("dst_block_start", pa.int32()),
        pa.field("dst_block_end", pa.int32()),
        pa.field("kind", pa.string()),
        pa.field("edge_key", pa.string()),
        pa.field("cond_instr_index", pa.int32()),
        pa.field("cond_instr_offset", pa.int32()),
        pa.field("exc_index", pa.int32()),
    ]
)

CODE_UNITS_QUERY = CODE_UNITS_SPEC.query()
INSTR_QUERY = INSTR_SPEC.query()
EXC_QUERY = EXC_SPEC.query()
BLOCKS_QUERY = BLOCKS_SPEC.query()
CFG_EDGES_QUERY = CFG_EDGES_SPEC.query()
ERRORS_QUERY = ERRORS_SPEC.query()


def _context_from_file_ctx(
    file_ctx: FileContext,
    options: BytecodeExtractOptions,
) -> BytecodeFileContext | None:
    if not file_ctx.file_id or not file_ctx.path:
        return None
    return BytecodeFileContext(file_ctx=file_ctx, options=options)


def _row_context(
    rf: dict[str, object], options: BytecodeExtractOptions
) -> BytecodeFileContext | None:
    file_ctx = FileContext.from_repo_row(rf)
    return _context_from_file_ctx(file_ctx, options)


def _build_code_units_table(
    rows: list[Row],
    *,
    exec_ctx: ExecutionContext,
) -> TableLike:
    return materialize_plan(
        _build_code_units_plan(rows, exec_ctx=exec_ctx),
        ctx=exec_ctx,
        attach_ordering_metadata=True,
    )


def _build_code_units_plan(
    rows: list[Row],
    *,
    exec_ctx: ExecutionContext,
) -> Plan:
    if not rows:
        return Plan.table_source(empty_table(CODE_UNITS_SCHEMA))
    plan = plan_from_rows(rows, schema=CODE_UNITS_ROWS_SCHEMA, label="bc_code_units_raw")
    plan = project_columns(
        plan,
        base=CODE_UNITS_ROWS_SCHEMA.names,
        extras=[
            (
                MaskedHashExprSpec(
                    spec=BC_CODE_UNIT_ID_SPEC,
                    required=("file_id", "qualpath", "firstlineno", "co_name"),
                ).to_expression(),
                "code_unit_id",
            ),
            (
                MaskedHashExprSpec(
                    spec=BC_PARENT_CODE_UNIT_ID_SPEC,
                    required=(
                        "file_id",
                        "parent_qualpath",
                        "parent_firstlineno",
                        "parent_co_name",
                    ),
                ).to_expression(),
                "parent_code_unit_id",
            ),
        ],
        ctx=exec_ctx,
    )
    plan = CODE_UNITS_QUERY.apply_to_plan(plan, ctx=exec_ctx)
    return align_plan(
        plan,
        schema=CODE_UNITS_SCHEMA,
        ctx=exec_ctx,
    )


def _build_instructions_table(
    rows: list[Row],
    *,
    exec_ctx: ExecutionContext,
) -> TableLike:
    return materialize_plan(
        _build_instructions_plan(rows, exec_ctx=exec_ctx),
        ctx=exec_ctx,
        attach_ordering_metadata=True,
    )


def _build_instructions_plan(
    rows: list[Row],
    *,
    exec_ctx: ExecutionContext,
) -> Plan:
    if not rows:
        return Plan.table_source(empty_table(INSTR_SCHEMA))
    plan = plan_from_rows(rows, schema=INSTR_ROWS_SCHEMA, label="bc_instructions_raw")
    plan = project_columns(
        plan,
        base=INSTR_ROWS_SCHEMA.names,
        extras=[
            (
                MaskedHashExprSpec(
                    spec=BC_CODE_UNIT_ID_SPEC,
                    required=("file_id", "qualpath", "firstlineno", "co_name"),
                ).to_expression(),
                "code_unit_id",
            )
        ],
        ctx=exec_ctx,
    )
    plan = INSTR_QUERY.apply_to_plan(plan, ctx=exec_ctx)
    return align_plan(
        plan,
        schema=INSTR_SCHEMA,
        ctx=exec_ctx,
    )


def _build_exceptions_table(
    rows: list[Row],
    *,
    exec_ctx: ExecutionContext,
) -> TableLike:
    return materialize_plan(
        _build_exceptions_plan(rows, exec_ctx=exec_ctx),
        ctx=exec_ctx,
        attach_ordering_metadata=True,
    )


def _build_exceptions_plan(
    rows: list[Row],
    *,
    exec_ctx: ExecutionContext,
) -> Plan:
    if not rows:
        return Plan.table_source(empty_table(EXC_SCHEMA))
    plan = plan_from_rows(rows, schema=EXC_ROWS_SCHEMA, label="bc_exceptions_raw")
    plan = project_columns(
        plan,
        base=EXC_ROWS_SCHEMA.names,
        extras=[
            (
                MaskedHashExprSpec(
                    spec=BC_CODE_UNIT_ID_SPEC,
                    required=("file_id", "qualpath", "firstlineno", "co_name"),
                ).to_expression(),
                "code_unit_id",
            )
        ],
        ctx=exec_ctx,
    )
    plan = EXC_QUERY.apply_to_plan(plan, ctx=exec_ctx)
    return align_plan(
        plan,
        schema=EXC_SCHEMA,
        ctx=exec_ctx,
    )


def _build_blocks_table(
    rows: list[Row],
    *,
    exec_ctx: ExecutionContext,
) -> TableLike:
    return materialize_plan(
        _build_blocks_plan(rows, exec_ctx=exec_ctx),
        ctx=exec_ctx,
        attach_ordering_metadata=True,
    )


def _build_blocks_plan(
    rows: list[Row],
    *,
    exec_ctx: ExecutionContext,
) -> Plan:
    if not rows:
        return Plan.table_source(empty_table(BLOCKS_SCHEMA))
    plan = plan_from_rows(rows, schema=BLOCKS_ROWS_SCHEMA, label="bc_blocks_raw")
    plan = project_columns(
        plan,
        base=BLOCKS_ROWS_SCHEMA.names,
        extras=[
            (
                MaskedHashExprSpec(
                    spec=BC_CODE_UNIT_ID_SPEC,
                    required=("file_id", "qualpath", "firstlineno", "co_name"),
                ).to_expression(),
                "code_unit_id",
            )
        ],
        ctx=exec_ctx,
    )
    plan = BLOCKS_QUERY.apply_to_plan(plan, ctx=exec_ctx)
    return align_plan(
        plan,
        schema=BLOCKS_SCHEMA,
        ctx=exec_ctx,
    )


def _build_cfg_edges_table(
    rows: list[Row],
    *,
    exec_ctx: ExecutionContext,
) -> TableLike:
    return materialize_plan(
        _build_cfg_edges_plan(rows, exec_ctx=exec_ctx),
        ctx=exec_ctx,
        attach_ordering_metadata=True,
    )


def _build_cfg_edges_plan(
    rows: list[Row],
    *,
    exec_ctx: ExecutionContext,
) -> Plan:
    if not rows:
        return Plan.table_source(empty_table(CFG_EDGES_SCHEMA))
    plan = plan_from_rows(rows, schema=CFG_EDGES_ROWS_SCHEMA, label="bc_cfg_edges_raw")
    plan = project_columns(
        plan,
        base=CFG_EDGES_ROWS_SCHEMA.names,
        extras=[
            (
                MaskedHashExprSpec(
                    spec=BC_CODE_UNIT_ID_SPEC,
                    required=("file_id", "qualpath", "firstlineno", "co_name"),
                ).to_expression(),
                "code_unit_id",
            ),
            (
                MaskedHashExprSpec(
                    spec=BC_SRC_BLOCK_ID_SPEC,
                    required=("code_unit_id", "src_block_start", "src_block_end"),
                ).to_expression(),
                "src_block_id",
            ),
            (
                MaskedHashExprSpec(
                    spec=BC_DST_BLOCK_ID_SPEC,
                    required=("code_unit_id", "dst_block_start", "dst_block_end"),
                ).to_expression(),
                "dst_block_id",
            ),
        ],
        ctx=exec_ctx,
    )
    plan = CFG_EDGES_QUERY.apply_to_plan(plan, ctx=exec_ctx)
    return align_plan(
        plan,
        schema=CFG_EDGES_SCHEMA,
        ctx=exec_ctx,
    )


def _compile_text(
    text: str, ctx: BytecodeFileContext
) -> tuple[pytypes.CodeType | None, Row | None]:
    try:
        top = compile(
            text,
            ctx.path,
            "exec",
            dont_inherit=ctx.options.dont_inherit,
            optimize=ctx.options.optimize,
        )
    except (OverflowError, SyntaxError, ValueError) as exc:
        return (
            None,
            {
                **ctx.identity_row(),
                "error_type": type(exc).__name__,
                "message": str(exc),
            },
        )
    return top, None


def _qual_for(co: pytypes.CodeType, parent_qual: str | None) -> str:
    if parent_qual is None:
        return "<module>"
    return f"{parent_qual}.{co.co_name}@{co.co_firstlineno}"


def _assign_code_units(
    top: pytypes.CodeType,
    ctx: BytecodeFileContext,
    cu_rows: list[Row],
) -> dict[int, CodeUnitKey]:
    co_to_key: dict[int, CodeUnitKey] = {}
    co_to_qual: dict[int, str] = {}

    for co, parent in _iter_code_objects(top):
        parent_key = co_to_key.get(id(parent)) if parent is not None else None
        parent_qual = co_to_qual.get(id(parent)) if parent is not None else None
        qual = _qual_for(co, parent_qual)
        key = CodeUnitKey(
            qualpath=qual,
            co_name=co.co_name,
            firstlineno=int(co.co_firstlineno or 0),
        )
        co_to_key[id(co)] = key
        co_to_qual[id(co)] = qual

        cu_rows.append(
            {
                **ctx.identity_row(),
                "qualpath": qual,
                "co_name": co.co_name,
                "firstlineno": int(co.co_firstlineno or 0),
                "parent_qualpath": parent_key.qualpath if parent_key is not None else None,
                "parent_co_name": parent_key.co_name if parent_key is not None else None,
                "parent_firstlineno": parent_key.firstlineno if parent_key is not None else None,
                "argcount": int(getattr(co, "co_argcount", 0)),
                "posonlyargcount": int(getattr(co, "co_posonlyargcount", 0)),
                "kwonlyargcount": int(getattr(co, "co_kwonlyargcount", 0)),
                "nlocals": int(getattr(co, "co_nlocals", 0)),
                "flags": int(getattr(co, "co_flags", 0)),
                "stacksize": int(getattr(co, "co_stacksize", 0)),
                "code_len": len(co.co_code),
            }
        )

    return co_to_key


def _instruction_data(co: pytypes.CodeType, options: BytecodeExtractOptions) -> InstructionData:
    instructions = list(dis.get_instructions(co, adaptive=options.adaptive))
    index_by_offset = {int(ins.offset): idx for idx, ins in enumerate(instructions)}
    return InstructionData(instructions=instructions, index_by_offset=index_by_offset)


def _exception_entries(co: pytypes.CodeType) -> Sequence[object]:
    bc = dis.Bytecode(co)
    return list(getattr(bc, "exception_entries", ()))


def _build_code_unit_context(
    co: pytypes.CodeType,
    ctx: BytecodeFileContext,
    code_unit_key: CodeUnitKey,
) -> CodeUnitContext:
    return CodeUnitContext(
        code_unit_key=code_unit_key,
        file_ctx=ctx,
        instruction_data=_instruction_data(co, ctx.options),
        exc_entries=_exception_entries(co),
        code_len=len(co.co_code),
    )


def _code_unit_key_columns(unit_ctx: CodeUnitContext) -> dict[str, RowValue]:
    key = unit_ctx.code_unit_key
    return {
        "qualpath": key.qualpath,
        "co_name": key.co_name,
        "firstlineno": key.firstlineno,
    }


def _append_exception_rows(unit_ctx: CodeUnitContext, exc_rows: list[Row]) -> None:
    identity = unit_ctx.file_ctx.identity_row()
    for k, ex in enumerate(unit_ctx.exc_entries):
        exc_rows.append(
            {
                **identity,
                **_code_unit_key_columns(unit_ctx),
                "exc_index": int(k),
                "start_offset": int(getattr(ex, "start", 0)),
                "end_offset": int(getattr(ex, "end", 0)),
                "target_offset": int(getattr(ex, "target", 0)),
                "depth": int(getattr(ex, "depth", 0)),
                "lasti": bool(getattr(ex, "lasti", False)),
            }
        )


def _append_instruction_rows(unit_ctx: CodeUnitContext, ins_rows: list[Row]) -> None:
    identity = unit_ctx.file_ctx.identity_row()
    for idx, ins in enumerate(unit_ctx.instruction_data.instructions):
        jt = _jump_target(ins)
        pos = getattr(ins, "positions", None)

        pos_start_line = getattr(pos, "lineno", None) if pos else None
        pos_end_line = getattr(pos, "end_lineno", None) if pos else None
        pos_start_col = getattr(pos, "col_offset", None) if pos else None
        pos_end_col = getattr(pos, "end_col_offset", None) if pos else None

        ins_rows.append(
            {
                **identity,
                **_code_unit_key_columns(unit_ctx),
                "instr_index": int(idx),
                "offset": int(ins.offset),
                "opname": ins.opname,
                "opcode": int(ins.opcode),
                "arg": int(ins.arg) if ins.arg is not None else None,
                "argval_str": str(ins.argval) if ins.argval is not None else None,
                "argrepr": ins.argrepr,
                "is_jump_target": bool(ins.is_jump_target),
                "jump_target_offset": int(jt) if jt is not None else None,
                "starts_line": int(ins.starts_line) if ins.starts_line is not None else None,
                "pos_start_line": int(pos_start_line) if pos_start_line is not None else None,
                "pos_end_line": int(pos_end_line) if pos_end_line is not None else None,
                "pos_start_col": int(pos_start_col) if pos_start_col is not None else None,
                "pos_end_col": int(pos_end_col) if pos_end_col is not None else None,
            }
        )


def _is_block_terminator(ins: dis.Instruction, terminator_opnames: Sequence[str]) -> bool:
    return ins.opname in terminator_opnames or _is_unconditional_jump(ins.opname)


def _next_instruction_offset(
    ins: dis.Instruction,
    index_by_offset: Mapping[int, int],
    instructions: Sequence[dis.Instruction],
) -> int | None:
    current_index = index_by_offset.get(int(ins.offset))
    if current_index is None:
        return None
    next_index = current_index + 1
    if next_index < len(instructions):
        return int(instructions[next_index].offset)
    return None


def _boundary_offsets(unit_ctx: CodeUnitContext, ins_offsets: set[int]) -> list[int]:
    boundaries: set[int] = {0, unit_ctx.code_len}
    instructions = unit_ctx.instruction_data.instructions
    for ins in instructions:
        jt = _jump_target(ins)
        if jt is not None:
            boundaries.add(int(jt))
        if _is_block_terminator(ins, unit_ctx.file_ctx.options.terminator_opnames):
            next_offset = _next_instruction_offset(
                ins, unit_ctx.instruction_data.index_by_offset, instructions
            )
            if next_offset is not None:
                boundaries.add(next_offset)
    for ex in unit_ctx.exc_entries:
        boundaries.update(
            {
                int(getattr(ex, "start", 0)),
                int(getattr(ex, "end", 0)),
                int(getattr(ex, "target", 0)),
            }
        )
    return sorted(
        offset for offset in boundaries if offset in ins_offsets or offset == unit_ctx.code_len
    )


def _build_blocks(boundaries: Sequence[int]) -> list[tuple[int, int]]:
    return [
        (boundaries[index], boundaries[index + 1])
        for index in range(len(boundaries) - 1)
        if boundaries[index] < boundaries[index + 1]
    ]


def _append_block_rows(
    unit_ctx: CodeUnitContext,
    blocks: Sequence[tuple[int, int]],
    ins_offsets: set[int],
    blk_rows: list[Row],
) -> dict[int, tuple[int, int]]:
    identity = unit_ctx.file_ctx.identity_row()
    target_offsets = {int(getattr(ex, "target", 0)) for ex in unit_ctx.exc_entries}
    off_to_block: dict[int, tuple[int, int]] = {}

    for start, end in blocks:
        kind = "entry" if start == 0 else ("handler" if start in target_offsets else "normal")
        blk_rows.append(
            {
                **identity,
                **_code_unit_key_columns(unit_ctx),
                "start_offset": int(start),
                "end_offset": int(end),
                "kind": kind,
            }
        )
        for offset in sorted(offset for offset in ins_offsets if start <= offset < end):
            off_to_block[offset] = (start, end)

    return off_to_block


def _append_cfg_edge(unit_ctx: CodeUnitContext, spec: CfgEdgeSpec, edge_rows: list[Row]) -> None:
    edge_rows.append(
        {
            **unit_ctx.file_ctx.identity_row(),
            **_code_unit_key_columns(unit_ctx),
            "src_block_start": spec.src_block_start,
            "src_block_end": spec.src_block_end,
            "dst_block_start": spec.dst_block_start,
            "dst_block_end": spec.dst_block_end,
            "kind": spec.kind,
            "edge_key": spec.edge_key,
            "cond_instr_index": spec.cond_instr_index,
            "cond_instr_offset": spec.cond_instr_offset,
            "exc_index": spec.exc_index,
        }
    )


def _append_jump_edges(ctx: _CfgEdgeContext, block: _BlockEdgeContext) -> bool:
    jt = _jump_target(block.last)
    if jt is None:
        return False
    dst_block = ctx.off_to_block.get(int(jt))
    if dst_block is None:
        return False
    dst_start, dst_end = dst_block
    kind = "jump" if _is_unconditional_jump(block.last.opname) else "branch"
    _append_cfg_edge(
        ctx.unit_ctx,
        CfgEdgeSpec(
            src_block_start=block.src_start,
            src_block_end=block.src_end,
            dst_block_start=dst_start,
            dst_block_end=dst_end,
            kind=kind,
            edge_key=str(block.last.offset),
            cond_instr_index=block.last_index if kind == "branch" else None,
            cond_instr_offset=int(block.last.offset) if kind == "branch" else None,
            exc_index=None,
        ),
        ctx.edge_rows,
    )
    if kind != "branch":
        return True
    next_block = ctx.off_to_block.get(block.end)
    if next_block is None:
        return True
    next_start, next_end = next_block
    _append_cfg_edge(
        ctx.unit_ctx,
        CfgEdgeSpec(
            src_block_start=block.src_start,
            src_block_end=block.src_end,
            dst_block_start=next_start,
            dst_block_end=next_end,
            kind="next",
            edge_key=str(block.end),
            cond_instr_index=block.last_index,
            cond_instr_offset=int(block.last.offset),
            exc_index=None,
        ),
        ctx.edge_rows,
    )
    return True


def _append_fallthrough_edge(ctx: _CfgEdgeContext, block: _BlockEdgeContext) -> None:
    next_block = ctx.off_to_block.get(block.end)
    if next_block is None:
        return
    next_start, next_end = next_block
    _append_cfg_edge(
        ctx.unit_ctx,
        CfgEdgeSpec(
            src_block_start=block.src_start,
            src_block_end=block.src_end,
            dst_block_start=next_start,
            dst_block_end=next_end,
            kind="next",
            edge_key=str(block.end),
            cond_instr_index=None,
            cond_instr_offset=None,
            exc_index=None,
        ),
        ctx.edge_rows,
    )


def _append_edges_for_block(ctx: _CfgEdgeContext, start: int, end: int) -> None:
    ins_in_block = [ins for ins in ctx.instructions if start <= int(ins.offset) < end]
    if not ins_in_block:
        return
    first_offset = int(ins_in_block[0].offset)
    src_block = ctx.off_to_block.get(first_offset)
    if src_block is None:
        return
    src_start, src_end = src_block
    last = ins_in_block[-1]
    last_index = ctx.index_by_offset.get(int(last.offset))
    if last_index is None:
        return
    block = _BlockEdgeContext(
        src_start=src_start,
        src_end=src_end,
        end=end,
        last=last,
        last_index=int(last_index),
    )
    if _append_jump_edges(ctx, block):
        return
    if last.opname in ctx.terminators:
        return
    _append_fallthrough_edge(ctx, block)


def _append_normal_edges(
    unit_ctx: CodeUnitContext,
    blocks: Sequence[tuple[int, int]],
    off_to_block: Mapping[int, tuple[int, int]],
    edge_rows: list[Row],
) -> None:
    ctx = _CfgEdgeContext(
        unit_ctx=unit_ctx,
        instructions=unit_ctx.instruction_data.instructions,
        index_by_offset=unit_ctx.instruction_data.index_by_offset,
        terminators=unit_ctx.file_ctx.options.terminator_opnames,
        off_to_block=off_to_block,
        edge_rows=edge_rows,
    )

    for start, end in blocks:
        _append_edges_for_block(ctx, start, end)


def _append_exception_edges(
    unit_ctx: CodeUnitContext,
    blocks: Sequence[tuple[int, int]],
    off_to_block: Mapping[int, tuple[int, int]],
    edge_rows: list[Row],
) -> None:
    for k, ex in enumerate(unit_ctx.exc_entries):
        start = int(getattr(ex, "start", 0))
        end = int(getattr(ex, "end", 0))
        target = int(getattr(ex, "target", 0))
        handler_block = off_to_block.get(target)
        if handler_block is None:
            continue
        handler_start, handler_end = handler_block
        for block_start, block_end in blocks:
            if block_end <= start or block_start >= end:
                continue
            src_block = off_to_block.get(block_start)
            if src_block is None:
                continue
            src_start, src_end = src_block
            _append_cfg_edge(
                unit_ctx,
                CfgEdgeSpec(
                    src_block_start=src_start,
                    src_block_end=src_end,
                    dst_block_start=handler_start,
                    dst_block_end=handler_end,
                    kind="exc",
                    edge_key=str(k),
                    cond_instr_index=None,
                    cond_instr_offset=None,
                    exc_index=int(k),
                ),
                edge_rows,
            )


def _append_cfg_rows(unit_ctx: CodeUnitContext, blk_rows: list[Row], edge_rows: list[Row]) -> None:
    instructions = unit_ctx.instruction_data.instructions
    if not instructions:
        return
    ins_offsets = {int(ins.offset) for ins in instructions}
    boundaries = _boundary_offsets(unit_ctx, ins_offsets)
    blocks = _build_blocks(boundaries)
    if not blocks:
        return
    off_to_block = _append_block_rows(unit_ctx, blocks, ins_offsets, blk_rows)
    _append_normal_edges(unit_ctx, blocks, off_to_block, edge_rows)
    _append_exception_edges(unit_ctx, blocks, off_to_block, edge_rows)


def _extract_code_unit_rows(
    top: pytypes.CodeType,
    ctx: BytecodeFileContext,
    code_unit_keys: Mapping[int, CodeUnitKey],
    buffers: BytecodeRowBuffers,
) -> None:
    for co, _parent in _iter_code_objects(top):
        key = code_unit_keys.get(id(co))
        if key is None:
            continue
        unit_ctx = _build_code_unit_context(co, ctx, key)
        _append_exception_rows(unit_ctx, buffers.exception_rows)
        _append_instruction_rows(unit_ctx, buffers.instruction_rows)
        if ctx.options.include_cfg_derivations:
            _append_cfg_rows(unit_ctx, buffers.block_rows, buffers.edge_rows)


def _build_bytecode_result(
    buffers: BytecodeRowBuffers,
    *,
    exec_ctx: ExecutionContext,
) -> BytecodeExtractResult:
    plans = _build_bytecode_plans(buffers, exec_ctx=exec_ctx)
    return BytecodeExtractResult(
        py_bc_code_units=materialize_plan(
            plans["py_bc_code_units"],
            ctx=exec_ctx,
            attach_ordering_metadata=True,
        ),
        py_bc_instructions=materialize_plan(
            plans["py_bc_instructions"],
            ctx=exec_ctx,
            attach_ordering_metadata=True,
        ),
        py_bc_exception_table=materialize_plan(
            plans["py_bc_exception_table"],
            ctx=exec_ctx,
            attach_ordering_metadata=True,
        ),
        py_bc_blocks=materialize_plan(
            plans["py_bc_blocks"],
            ctx=exec_ctx,
            attach_ordering_metadata=True,
        ),
        py_bc_cfg_edges=materialize_plan(
            plans["py_bc_cfg_edges"],
            ctx=exec_ctx,
            attach_ordering_metadata=True,
        ),
        py_bc_errors=materialize_plan(
            plans["py_bc_errors"],
            ctx=exec_ctx,
            attach_ordering_metadata=True,
        ),
    )


def _build_bytecode_plans(
    buffers: BytecodeRowBuffers,
    *,
    exec_ctx: ExecutionContext,
) -> dict[str, Plan]:
    code_units = _build_code_units_plan(buffers.code_unit_rows, exec_ctx=exec_ctx)
    instructions = _build_instructions_plan(buffers.instruction_rows, exec_ctx=exec_ctx)
    exceptions = _build_exceptions_plan(buffers.exception_rows, exec_ctx=exec_ctx)
    blocks = _build_blocks_plan(buffers.block_rows, exec_ctx=exec_ctx)
    edges = _build_cfg_edges_plan(buffers.edge_rows, exec_ctx=exec_ctx)
    errors_plan = plan_from_rows(buffers.error_rows, schema=ERRORS_SCHEMA, label="bc_errors")
    errors_plan = ERRORS_QUERY.apply_to_plan(errors_plan, ctx=exec_ctx)
    errors_plan = align_plan(
        errors_plan,
        schema=ERRORS_SCHEMA,
        available=ERRORS_SCHEMA.names,
        ctx=exec_ctx,
    )
    return {
        "py_bc_code_units": code_units,
        "py_bc_instructions": instructions,
        "py_bc_exception_table": exceptions,
        "py_bc_blocks": blocks,
        "py_bc_cfg_edges": edges,
        "py_bc_errors": errors_plan,
    }


def _jump_target(ins: dis.Instruction) -> int | None:
    jt = getattr(ins, "jump_target", None)
    if isinstance(jt, int):
        return jt
    jt = getattr(ins, "jump_target_offset", None)
    if isinstance(jt, int):
        return jt
    if (ins.opcode in dis.hasjabs or ins.opcode in dis.hasjrel) and isinstance(ins.argval, int):
        return int(ins.argval)
    return None


def _is_unconditional_jump(opname: str) -> bool:
    if not opname.startswith("JUMP"):
        return False
    # heuristic: "IF" jumps are conditional, FOR_ITER is conditional-ish
    return ("IF" not in opname) and (opname != "FOR_ITER")


def _iter_code_objects(
    co: pytypes.CodeType, parent: pytypes.CodeType | None = None
) -> Iterator[tuple[pytypes.CodeType, pytypes.CodeType | None]]:
    yield co, parent
    for c in co.co_consts:
        if isinstance(c, pytypes.CodeType):
            yield from _iter_code_objects(c, co)


def extract_bytecode(
    repo_files: TableLike,
    options: BytecodeExtractOptions | None = None,
    *,
    file_contexts: Iterable[FileContext] | None = None,
    ctx: ExecutionContext | None = None,
) -> BytecodeExtractResult:
    """Extract bytecode tables from repository files.

    Returns
    -------
    BytecodeExtractResult
        Tables for bytecode code units, instructions, exception data, and edges.
    """
    options = options or BytecodeExtractOptions()
    exec_ctx = ctx or execution_context_factory("default")
    plans = extract_bytecode_plans(
        repo_files,
        options=options,
        file_contexts=file_contexts,
        ctx=exec_ctx,
    )
    metadata_specs = _bytecode_metadata_specs(options)
    return BytecodeExtractResult(
        py_bc_code_units=materialize_plan(
            plans["py_bc_code_units"],
            ctx=exec_ctx,
            metadata_spec=metadata_specs["py_bc_code_units"],
            attach_ordering_metadata=True,
        ),
        py_bc_instructions=materialize_plan(
            plans["py_bc_instructions"],
            ctx=exec_ctx,
            metadata_spec=metadata_specs["py_bc_instructions"],
            attach_ordering_metadata=True,
        ),
        py_bc_exception_table=materialize_plan(
            plans["py_bc_exception_table"],
            ctx=exec_ctx,
            metadata_spec=metadata_specs["py_bc_exception_table"],
            attach_ordering_metadata=True,
        ),
        py_bc_blocks=materialize_plan(
            plans["py_bc_blocks"],
            ctx=exec_ctx,
            metadata_spec=metadata_specs["py_bc_blocks"],
            attach_ordering_metadata=True,
        ),
        py_bc_cfg_edges=materialize_plan(
            plans["py_bc_cfg_edges"],
            ctx=exec_ctx,
            metadata_spec=metadata_specs["py_bc_cfg_edges"],
            attach_ordering_metadata=True,
        ),
        py_bc_errors=materialize_plan(
            plans["py_bc_errors"],
            ctx=exec_ctx,
            metadata_spec=metadata_specs["py_bc_errors"],
            attach_ordering_metadata=True,
        ),
    )


def extract_bytecode_plans(
    repo_files: TableLike,
    options: BytecodeExtractOptions | None = None,
    *,
    file_contexts: Iterable[FileContext] | None = None,
    ctx: ExecutionContext | None = None,
) -> dict[str, Plan]:
    """Extract bytecode plans from repository files.

    Returns
    -------
    dict[str, Plan]
        Plan bundle keyed by bytecode output name.
    """
    options = options or BytecodeExtractOptions()
    exec_ctx = ctx or execution_context_factory("default")

    buffers = BytecodeRowBuffers(
        code_unit_rows=[],
        instruction_rows=[],
        exception_rows=[],
        block_rows=[],
        edge_rows=[],
        error_rows=[],
    )

    for file_ctx in iter_contexts(repo_files, file_contexts):
        bc_ctx = _context_from_file_ctx(file_ctx, options)
        if bc_ctx is None:
            continue
        text = text_from_file_ctx(bc_ctx.file_ctx)
        if text is None:
            continue
        top, err = _compile_text(text, bc_ctx)
        if err is not None:
            buffers.error_rows.append(err)
            continue
        if top is None:
            continue
        code_unit_keys = _assign_code_units(top, bc_ctx, buffers.code_unit_rows)
        _extract_code_unit_rows(top, bc_ctx, code_unit_keys, buffers)

    return _build_bytecode_plans(buffers, exec_ctx=exec_ctx)


class _BytecodeTableKwargs(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: BytecodeExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    ctx: ExecutionContext | None
    prefer_reader: bool


class _BytecodeTableKwargsTable(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: BytecodeExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    ctx: ExecutionContext | None
    prefer_reader: Literal[False]


class _BytecodeTableKwargsReader(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: BytecodeExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    ctx: ExecutionContext | None
    prefer_reader: Required[Literal[True]]


@overload
def extract_bytecode_table(
    **kwargs: Unpack[_BytecodeTableKwargsTable],
) -> TableLike: ...


@overload
def extract_bytecode_table(
    **kwargs: Unpack[_BytecodeTableKwargsReader],
) -> TableLike | RecordBatchReaderLike: ...


def extract_bytecode_table(
    **kwargs: Unpack[_BytecodeTableKwargs],
) -> TableLike | RecordBatchReaderLike:
    """Extract bytecode instruction facts as a single table.

    Parameters
    ----------
    kwargs:
        Keyword-only arguments for extraction (repo_files, options, file_contexts, ctx,
        prefer_reader).

    Returns
    -------
    TableLike | RecordBatchReaderLike
        Bytecode instruction output.
    """
    repo_files = kwargs["repo_files"]
    options = kwargs.get("options") or BytecodeExtractOptions()
    file_contexts = kwargs.get("file_contexts")
    exec_ctx = kwargs.get("ctx") or execution_context_factory("default")
    prefer_reader = kwargs.get("prefer_reader", False)
    plans = extract_bytecode_plans(
        repo_files,
        options=options,
        file_contexts=file_contexts,
        ctx=exec_ctx,
    )
    metadata_specs = _bytecode_metadata_specs(options)
    return run_plan_bundle(
        {"py_bc_instructions": plans["py_bc_instructions"]},
        ctx=exec_ctx,
        prefer_reader=prefer_reader,
        metadata_specs={"py_bc_instructions": metadata_specs["py_bc_instructions"]},
        attach_ordering_metadata=True,
    )["py_bc_instructions"]
