# ruff: noqa: D100, D103, INP001
from __future__ import annotations

from dataclasses import dataclass, field
from typing import cast

from datafusion import SessionContext

from datafusion_engine.io.adapter import DataFusionIOAdapter, ObjectStoreRegistries


@dataclass(eq=False)
class _FakeSessionContext:
    calls: list[tuple[str, object, str | None]] = field(default_factory=list)

    def register_object_store(self, scheme: str, store: object, host: str | None) -> None:
        self.calls.append((scheme, store, host))


def test_register_object_store_deduplicates_with_injected_registry() -> None:
    ctx = _FakeSessionContext()
    registries = ObjectStoreRegistries()
    adapter = DataFusionIOAdapter(
        ctx=cast("SessionContext", ctx),
        profile=None,
        object_store_registries=registries,
    )

    adapter.register_object_store(scheme="s3", store=object(), host="bucket")
    adapter.register_object_store(scheme="s3", store=object(), host="bucket")

    assert len(ctx.calls) == 1
