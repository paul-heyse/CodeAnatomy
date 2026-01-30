"""Execution helper utilities for DataFusion-native planning and IO."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping
from typing import TYPE_CHECKING, cast

import msgspec
import pyarrow as pa
import pyarrow.ipc as pa_ipc
from datafusion import DataFrameWriteOptions, SessionContext
from datafusion.dataframe import DataFrame

from arrow_utils.core.streaming import to_reader
from datafusion_engine.arrow_interop import RecordBatchReaderLike, TableLike
from engine.plan_cache import PlanCacheKey
from schema_spec.policies import DataFusionWritePolicy
from serde_msgspec import loads_msgpack, to_builtins
from serde_msgspec_ext import SubstraitBytes
from utils.hashing import hash_msgpack_canonical, hash_sha256_hex

try:
    import pyarrow.substrait as pa_substrait
except ImportError:  # pragma: no cover - optional dependency
    pa_substrait = None

try:
    from datafusion.substrait import Consumer as SubstraitConsumer
    from datafusion.substrait import Serde as SubstraitSerde
except ImportError:  # pragma: no cover - optional dependency
    SubstraitConsumer = None
    SubstraitSerde = None

if TYPE_CHECKING:
    from datafusion.substrait import Consumer as SubstraitConsumerType
    from datafusion.substrait import Serde as SubstraitSerdeType

    from datafusion_engine.plan_bundle import DataFusionPlanBundle


def plan_fingerprint_from_bundle(
    *,
    substrait_bytes: bytes,
    optimized: object,
) -> str:
    """Compute plan fingerprint from DataFusion plan bundle data.

    Parameters
    ----------
    substrait_bytes : bytes
        Substrait serialization bytes.
    optimized : object
        Optimized logical plan (unused when Substrait is required).

    Returns
    -------
    str
        SHA256 fingerprint of the plan.
    """
    _ = optimized
    return hash_sha256_hex(substrait_bytes)


def _delta_inputs_payload(bundle: DataFusionPlanBundle) -> list[dict[str, object]]:
    payloads: list[dict[str, object]] = [
        {
            "dataset_name": pin.dataset_name,
            "version": pin.version,
            "timestamp": pin.timestamp,
            "protocol": (
                to_builtins(pin.protocol, str_keys=True) if pin.protocol is not None else None
            ),
            "delta_scan_config": (
                to_builtins(pin.delta_scan_config) if pin.delta_scan_config is not None else None
            ),
            "delta_scan_config_hash": pin.delta_scan_config_hash,
            "datafusion_provider": pin.datafusion_provider,
            "protocol_compatible": pin.protocol_compatible,
            "protocol_compatibility": (
                to_builtins(pin.protocol_compatibility)
                if pin.protocol_compatibility is not None
                else None
            ),
        }
        for pin in bundle.delta_inputs
    ]
    payloads.sort(
        key=lambda row: (
            str(row["dataset_name"]),
            row["version"] or -1,
            row["timestamp"] or "",
        )
    )
    return payloads


def _delta_protocol_payload(
    protocol: object | None,
) -> dict[str, object] | None:
    if protocol is None:
        return None
    if isinstance(protocol, Mapping):
        payload = {str(key): value for key, value in protocol.items()}
        return payload or None
    if isinstance(protocol, msgspec.Struct):
        resolved = to_builtins(protocol, str_keys=True)
        if isinstance(resolved, Mapping):
            return {str(key): value for key, value in resolved.items()} or None
    return None


def plan_bundle_cache_key(
    *,
    bundle: DataFusionPlanBundle,
    profile_hash: str,
) -> PlanCacheKey:
    """Build plan cache key from DataFusion plan bundle.

    Parameters
    ----------
    bundle : DataFusionPlanBundle
        Plan bundle containing Substrait bytes and plan fingerprint.
    profile_hash : str
        Runtime profile hash for cache key.

    Returns
    -------
    PlanCacheKey
        Cache key based on plan bundle.
    """
    substrait_hash = hash_sha256_hex(bundle.substrait_bytes)
    required_udfs_hash = hash_msgpack_canonical(tuple(sorted(bundle.required_udfs)))
    required_tags_hash = hash_msgpack_canonical(tuple(sorted(bundle.required_rewrite_tags)))
    settings_items = tuple(sorted(bundle.artifacts.df_settings.items()))
    settings_hash = hash_msgpack_canonical(settings_items)
    delta_inputs_hash = hash_msgpack_canonical(_delta_inputs_payload(bundle))
    return PlanCacheKey(
        profile_hash=profile_hash,
        substrait_hash=substrait_hash,
        plan_fingerprint=bundle.plan_fingerprint,
        udf_snapshot_hash=bundle.artifacts.udf_snapshot_hash,
        function_registry_hash=bundle.artifacts.function_registry_hash,
        information_schema_hash=bundle.artifacts.information_schema_hash,
        required_udfs_hash=required_udfs_hash,
        required_rewrite_tags_hash=required_tags_hash,
        settings_hash=settings_hash,
        delta_inputs_hash=delta_inputs_hash,
    )


def _reader_from_table_like(value: TableLike | RecordBatchReaderLike) -> pa.RecordBatchReader:
    return to_reader(value)


def _fingerprint_reader(reader: pa.RecordBatchReader) -> tuple[str, int]:
    sink = pa.BufferOutputStream()
    row_count = 0
    with pa_ipc.new_stream(sink, reader.schema) as writer:
        for batch in reader:
            row_count += batch.num_rows
            writer.write_batch(batch)
    payload = sink.getvalue().to_pybytes()
    return hash_sha256_hex(payload), row_count


def _fingerprint_table(value: TableLike | RecordBatchReaderLike) -> tuple[str, int]:
    reader = _reader_from_table_like(value)
    return _fingerprint_reader(reader)


def _substrait_validation_payload(
    plan_bytes: bytes,
    *,
    df: DataFrame,
) -> Mapping[str, object]:
    if pa_substrait is None:
        return {
            "status": "unavailable",
            "error": "pyarrow.substrait is unavailable",
        }
    from datafusion_engine.streaming_executor import StreamingExecutionResult

    try:
        df_reader = StreamingExecutionResult(df=df).to_arrow_stream()
    except (RuntimeError, TypeError, ValueError) as exc:
        return {
            "status": "error",
            "stage": "datafusion",
            "error": str(exc),
        }
    try:
        substrait_result = pa_substrait.run_query(plan_bytes, use_threads=True)
    except (RuntimeError, TypeError, ValueError) as exc:
        return {
            "status": "error",
            "stage": "pyarrow_substrait",
            "error": str(exc),
        }
    df_hash, df_rows = _fingerprint_table(df_reader)
    substrait_hash, substrait_rows = _fingerprint_table(
        cast("TableLike | RecordBatchReaderLike", substrait_result)
    )
    match = df_rows == substrait_rows and df_hash == substrait_hash
    return {
        "status": "match" if match else "mismatch",
        "match": match,
        "datafusion_rows": df_rows,
        "datafusion_hash": df_hash,
        "substrait_rows": substrait_rows,
        "substrait_hash": substrait_hash,
    }


def validate_substrait_plan(plan_bytes: bytes, *, df: DataFrame) -> Mapping[str, object]:
    """Validate a Substrait plan by comparing PyArrow and DataFusion outputs.

    Returns
    -------
    Mapping[str, object]
        Validation payload containing hashes, row counts, and status fields.
    """
    return _substrait_validation_payload(plan_bytes, df=df)


def datafusion_to_reader(df: DataFrame) -> pa.RecordBatchReader:
    """Return a RecordBatchReader for a DataFusion DataFrame.

    Parameters
    ----------
    df : datafusion.dataframe.DataFrame
        DataFusion DataFrame to stream.

    Returns
    -------
    pyarrow.RecordBatchReader
        Arrow stream for the DataFusion result.
    """
    from datafusion_engine.streaming_executor import StreamingExecutionResult

    return StreamingExecutionResult(df=df).to_arrow_stream()


async def datafusion_to_async_batches(df: DataFrame) -> AsyncIterator[pa.RecordBatch]:
    """Yield DataFusion record batches asynchronously.

    Parameters
    ----------
    df : datafusion.dataframe.DataFrame
        DataFusion DataFrame to stream.

    Yields
    ------
    pyarrow.RecordBatch
        Record batches from the DataFusion result.
    """
    reader = datafusion_to_reader(df)
    for batch in reader:
        yield batch
        await asyncio.sleep(0)


def datafusion_write_options(
    policy: DataFusionWritePolicy | None,
) -> DataFrameWriteOptions:
    """Build DataFusion write options from a policy.

    Parameters
    ----------
    policy : DataFusionWritePolicy | None
        DataFusion write policy to convert.

    Returns
    -------
    DataFrameWriteOptions
        DataFusion write options derived from the policy.
    """
    resolved = policy or DataFusionWritePolicy()
    return DataFrameWriteOptions(
        partition_by=list(resolved.partition_by),
        sort_by=None,
        single_file_output=resolved.single_file_output,
    )


def replay_substrait_bytes(ctx: SessionContext, plan_bytes: bytes) -> DataFrame:
    """Replay a Substrait plan into a DataFusion DataFrame.

    Returns
    -------
    datafusion.dataframe.DataFrame
        DataFrame constructed from the Substrait plan bytes.

    Raises
    ------
    RuntimeError
        Raised when the DataFusion Substrait helpers are unavailable.
    """
    if SubstraitConsumer is None or SubstraitSerde is None:
        msg = "DataFusion Substrait helpers are unavailable."
        raise RuntimeError(msg)
    consumer = cast("SubstraitConsumerType", SubstraitConsumer)
    serde = cast("SubstraitSerdeType", SubstraitSerde)
    plan = serde.deserialize_bytes(plan_bytes)
    logical_plan = consumer.from_substrait_plan(ctx, plan)
    return ctx.create_dataframe_from_logical_plan(logical_plan)


def rehydrate_plan_artifacts(ctx: SessionContext, *, payload: Mapping[str, object]) -> DataFrame:
    """Rehydrate a DataFusion DataFrame from plan artifact payloads.

    Returns
    -------
    datafusion.dataframe.DataFrame
        DataFrame reconstructed from Substrait artifacts.

    Raises
    ------
    TypeError
        Raised when the payload does not contain msgpack bytes.
    ValueError
        Raised when the Substrait payload cannot be decoded.
    """
    substrait_msgpack = payload.get("substrait_msgpack")
    if not substrait_msgpack:
        msg = "Substrait payload missing from plan artifacts."
        raise ValueError(msg)
    if not isinstance(substrait_msgpack, (bytes, bytearray, memoryview)):
        msg = "Substrait payload must be msgpack bytes."
        raise TypeError(msg)
    try:
        payload_bytes = bytes(substrait_msgpack)
        substrait = loads_msgpack(payload_bytes, target_type=SubstraitBytes)
    except (ValueError, TypeError) as exc:
        msg = "Invalid msgpack payload for Substrait artifacts."
        raise ValueError(msg) from exc
    return replay_substrait_bytes(ctx, substrait.data)


__all__ = [
    "datafusion_to_async_batches",
    "datafusion_to_reader",
    "datafusion_write_options",
    "plan_bundle_cache_key",
    "plan_fingerprint_from_bundle",
    "rehydrate_plan_artifacts",
    "replay_substrait_bytes",
    "validate_substrait_plan",
]
