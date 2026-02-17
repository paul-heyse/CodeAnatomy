"""End-to-end checks for incremental provider modes."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.search.enrichment.incremental_provider import (
    IncrementalAnchorRequestV1,
    enrich_incremental_anchor,
)
from tools.cq.search.pipeline.enrichment_contracts import IncrementalEnrichmentModeV1


@pytest.mark.parametrize(
    "mode",
    [
        IncrementalEnrichmentModeV1.TS_ONLY,
        IncrementalEnrichmentModeV1.TS_SYM,
        IncrementalEnrichmentModeV1.TS_SYM_DIS,
        IncrementalEnrichmentModeV1.FULL,
    ],
)
def test_incremental_provider_modes(mode: IncrementalEnrichmentModeV1, tmp_path: Path) -> None:
    """Build incremental payload successfully across all supported modes."""
    p = tmp_path / "sample.py"
    p.write_text(
        "def outer(x):\n    y = x + 1\n    def inner(z):\n        return y + z\n    return inner\n",
        encoding="utf-8",
    )
    source = p.read_text(encoding="utf-8")
    bundle = enrich_incremental_anchor(
        IncrementalAnchorRequestV1(
            root=tmp_path,
            file_path=p,
            source=source,
            line=4,
            col=15,
            match_text="y",
            mode=mode,
            runtime_enrichment=False,
        )
    )
    assert bundle is not None
    assert bundle.mode is mode
