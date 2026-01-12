"""Tests for SCIP vectorized ID helpers."""

from __future__ import annotations

import arrowdsl.pyarrow_core as pa
from extract.scip_extract import (
    SCIP_DOCUMENTS_SCHEMA,
    SCIP_OCCURRENCES_SCHEMA,
    add_scip_document_ids,
    add_scip_occurrence_ids,
)


def test_scip_occurrence_ids_stable() -> None:
    """Occurrence IDs are deterministic and consistent across tables."""
    occ_rows = [
        {
            "occurrence_id": None,
            "document_id": None,
            "path": "a.py",
            "symbol": "sym",
            "symbol_roles": 0,
            "syntax_kind": None,
            "override_documentation": [],
            "start_line": 0,
            "start_char": 0,
            "end_line": 0,
            "end_char": 1,
            "range_len": 3,
            "enc_start_line": None,
            "enc_start_char": None,
            "enc_end_line": None,
            "enc_end_char": None,
            "enc_range_len": None,
        },
        {
            "occurrence_id": None,
            "document_id": None,
            "path": "a.py",
            "symbol": "sym",
            "symbol_roles": 0,
            "syntax_kind": None,
            "override_documentation": [],
            "start_line": 1,
            "start_char": 0,
            "end_line": 1,
            "end_char": 2,
            "range_len": 3,
            "enc_start_line": None,
            "enc_start_char": None,
            "enc_end_line": None,
            "enc_end_char": None,
            "enc_range_len": None,
        },
    ]
    occ_table = pa.Table.from_pylist(occ_rows, schema=SCIP_OCCURRENCES_SCHEMA)
    with_docs = add_scip_document_ids(occ_table)
    with_ids = add_scip_occurrence_ids(with_docs)
    ids_first = with_ids["occurrence_id"].to_pylist()

    with_docs_repeat = add_scip_document_ids(occ_table)
    ids_second = add_scip_occurrence_ids(with_docs_repeat)["occurrence_id"].to_pylist()

    assert ids_first == ids_second
    assert all(ids_first)

    doc_rows = [
        {
            "document_id": None,
            "path": "a.py",
            "language": None,
            "position_encoding": None,
        }
    ]
    doc_table = pa.Table.from_pylist(doc_rows, schema=SCIP_DOCUMENTS_SCHEMA)
    doc_ids = add_scip_document_ids(doc_table)["document_id"].to_pylist()
    assert doc_ids[0] == with_ids["document_id"].to_pylist()[0]
