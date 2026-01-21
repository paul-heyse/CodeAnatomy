"""Schema inference harness for contract handshakes."""

from __future__ import annotations

from dataclasses import dataclass

from arrowdsl.core.interop import SchemaLike, TableLike
from schema_spec.catalog_registry import dataset_spec as catalog_spec
from schema_spec.system import DatasetSpec, dataset_spec_from_schema, ddl_fingerprint_from_schema


class SchemaHandshakeError(ValueError):
    """Raised when inferred schema does not match the canonical contract."""


@dataclass(frozen=True)
class SchemaInferenceResult:
    """Inference output containing a dataset spec and DDL fingerprint."""

    spec: DatasetSpec
    ddl_fingerprint: str


@dataclass(frozen=True)
class SchemaInferenceHarness:
    """Resolve inferred schemas to canonical dataset specs."""

    def infer(self, name: str, table: TableLike) -> SchemaInferenceResult:
        """Infer a dataset spec and DDL fingerprint from a table-like object.

        Parameters
        ----------
        name:
            Dataset name to assign to the inferred spec.
        table:
            Table-like object to infer the schema from.

        Returns
        -------
        SchemaInferenceResult
            Inference result with spec and DDL fingerprint.
        """
        return self.infer_from_schema(name, table.schema)

    @staticmethod
    def infer_from_schema(name: str, schema: SchemaLike) -> SchemaInferenceResult:
        """Infer a dataset spec and DDL fingerprint from a schema.

        Parameters
        ----------
        name:
            Dataset name to assign to the inferred spec.
        schema:
            Arrow schema to use for inference.

        Returns
        -------
        SchemaInferenceResult
            Inference result with spec and DDL fingerprint.
        """
        ddl_fingerprint = ddl_fingerprint_from_schema(name, schema)
        spec = dataset_spec_from_schema(name, schema)
        return SchemaInferenceResult(spec=spec, ddl_fingerprint=ddl_fingerprint)

    @staticmethod
    def require_handshake(name: str, schema: SchemaLike) -> SchemaInferenceResult:
        """Enforce a schema handshake against the canonical dataset spec.

        Parameters
        ----------
        name:
            Dataset name to validate.
        schema:
            Inferred Arrow schema to validate.

        Returns
        -------
        SchemaInferenceResult
            Inference result after the handshake check.

        Raises
        ------
        SchemaHandshakeError
            Raised when the inferred schema fingerprint mismatches the canonical spec.
        """
        result = SchemaInferenceHarness.infer_from_schema(name, schema)
        try:
            expected = catalog_spec(name)
        except KeyError:
            return result
        expected_fingerprint = ddl_fingerprint_from_schema(name, expected.schema())
        if expected_fingerprint != result.ddl_fingerprint:
            msg = (
                f"Schema handshake failed for {name!r}: expected "
                f"{expected_fingerprint}, got {result.ddl_fingerprint}."
            )
            raise SchemaHandshakeError(msg)
        return result


__all__ = ["SchemaHandshakeError", "SchemaInferenceHarness", "SchemaInferenceResult"]
