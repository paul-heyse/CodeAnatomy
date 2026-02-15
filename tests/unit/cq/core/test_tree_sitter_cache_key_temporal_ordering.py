from __future__ import annotations

from tools.cq.utils.uuid_temporal_contracts import resolve_run_identity_contract


def test_run_identity_temporal_ordering_is_non_decreasing() -> None:
    rows = [resolve_run_identity_contract() for _ in range(32)]
    created = [row.run_created_ms for row in rows]
    assert all(value >= 0 for value in created)
    assert all(created[idx] <= created[idx + 1] for idx in range(len(created) - 1))
    assert all(row.run_uuid_version == 7 for row in rows)
