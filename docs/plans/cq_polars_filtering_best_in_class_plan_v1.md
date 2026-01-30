# cq Polars Filtering Best-in-Class Plan v1

## Overview
This plan defines a best-in-class, Polars-only filtering system for cq output with two new high-signal search parameters:

- `--impact`: derived analytic impact score and bucket (low/med/high)
- `--confidence`: evidence quality score and bucket (low/med/high)

Plus three previously aligned parameters:

- `--include`: file path include (glob or regex)
- `--exclude`: file path exclude (glob or regex)
- `--limit`: max results

This is a design-phase plan and allows breaking changes to achieve a clean end-state.

## Target UX (CLI)

```bash
# filter by high impact and high confidence in core modules
cq impact build_graph_product --param repo_root \
  --impact high --confidence high \
  --include "tools/cq/.*" --limit 50

# suppress tests and cap results
cq exceptions --exclude "tests/" --impact med --limit 100
```

## Target Data Model

All macros emit findings that are flattened into a single, queryable table.

```python
from dataclasses import dataclass

@dataclass(frozen=True)
class FindingRecord:
    macro: str
    group: str  # key_findings | section | evidence
    category: str
    message: str
    file: str | None
    line: int | None
    col: int | None
    impact_score: float
    impact_bucket: str  # low|med|high
    confidence_score: float
    confidence_bucket: str  # low|med|high
    evidence_kind: str  # resolved_ast|bytecode|heuristic|rg_only|unresolved
    details: dict[str, object]
```

Impact and confidence are computed from existing signals (counts, depth, resolution state). No new scanners are required.

## Scoring Policy (best-in-class target)

- **Impact**: normalized [0.0, 1.0] using macro-specific signals (site counts, depth, spread, breakage).
- **Confidence**: normalized [0.0, 1.0] using evidence quality (AST resolution vs heuristic match).

Buckets (consistent across macros):

```python
def bucket(score: float) -> str:
    if score >= 0.7:
        return "high"
    if score >= 0.4:
        return "med"
    return "low"
```

## Scope Item 1: Add scoring utilities + finding record schema

**Goal**: Centralize impact/confidence scoring and a canonical FindingRecord representation.

**Representative code snippets**

```python
# tools/cq/core/scoring.py
from dataclasses import dataclass

@dataclass(frozen=True)
class ImpactSignals:
    sites: int
    files: int
    depth: int
    breakages: int
    ambiguities: int

@dataclass(frozen=True)
class ConfidenceSignals:
    evidence_kind: str  # resolved_ast|bytecode|heuristic|rg_only|unresolved


def impact_score(signals: ImpactSignals) -> float:
    # Example weighting; tune by macro
    score = 0.0
    score += min(signals.sites / 50.0, 1.0) * 0.45
    score += min(signals.files / 20.0, 1.0) * 0.25
    score += min(signals.depth / 5.0, 1.0) * 0.15
    score += min(signals.breakages / 10.0, 1.0) * 0.15
    return min(score, 1.0)


def confidence_score(signals: ConfidenceSignals) -> float:
    mapping = {
        "resolved_ast": 0.95,
        "bytecode": 0.9,
        "heuristic": 0.6,
        "rg_only": 0.45,
        "unresolved": 0.3,
    }
    return mapping.get(signals.evidence_kind, 0.5)
```

**Target files to modify**
- `tools/cq/core/schema.py` (add optional impact/confidence fields to Finding or introduce FindingRecord dataclass)
- `tools/cq/core/scoring.py` (new)

**Code/modules to delete**
- None

**Implementation checklist**
- [ ] Add `ImpactSignals` and `ConfidenceSignals` dataclasses
- [ ] Add scoring + bucket helpers
- [ ] Decide on keeping `Finding.severity` or deprecating it (breaking change allowed)
- [ ] Define a canonical `FindingRecord` representation

## Scope Item 2: Instrument macros to emit signals

**Goal**: Each macro emits per-finding impact/confidence signals derived from existing analysis.

**Representative code snippets**

```python
# tools/cq/macros/calls.py (example)
from tools.cq.core.scoring import ImpactSignals, ConfidenceSignals, impact_score, confidence_score, bucket

signals = ImpactSignals(
    sites=len(all_sites),
    files=len(by_file),
    depth=0,
    breakages=0,
    ambiguities=0,
)
conf = ConfidenceSignals(evidence_kind="resolved_ast")

finding.details["impact_score"] = impact_score(signals)
finding.details["impact_bucket"] = bucket(finding.details["impact_score"])
finding.details["confidence_score"] = confidence_score(conf)
finding.details["confidence_bucket"] = bucket(finding.details["confidence_score"])
finding.details["evidence_kind"] = conf.evidence_kind
```

**Target files to modify**
- `tools/cq/macros/impact.py`
- `tools/cq/macros/calls.py`
- `tools/cq/macros/imports.py`
- `tools/cq/macros/exceptions.py`
- `tools/cq/macros/sig_impact.py`
- `tools/cq/macros/side_effects.py`
- `tools/cq/macros/scopes.py`
- `tools/cq/macros/async_hazards.py`
- `tools/cq/macros/bytecode.py`

**Code/modules to delete**
- None

**Implementation checklist**
- [ ] Define per-macro signal derivations (impact + confidence)
- [ ] Emit `impact_score`, `impact_bucket`, `confidence_score`, `confidence_bucket`, `evidence_kind` in all findings
- [ ] Ensure evidence kinds reflect actual resolution path (AST vs rg-only vs heuristic)

## Scope Item 3: Polars findings table + filter engine

**Goal**: Build a single Polars query surface for all findings and apply the five filters.

**Representative code snippets**

```python
# tools/cq/core/findings_table.py
import fnmatch
import polars as pl


def _pattern_to_regex(pattern: str) -> str:
    # Accept glob or regex; treat patterns with regex metachar as regex
    if any(ch in pattern for ch in [".", "^", "$", "["]):
        return pattern
    return fnmatch.translate(pattern)


def build_frame(records: list[FindingRecord]) -> pl.DataFrame:
    return pl.DataFrame([r.__dict__ for r in records])


def apply_filters(
    df: pl.DataFrame,
    *,
    include: list[str],
    exclude: list[str],
    impact: list[str],
    confidence: list[str],
    limit: int | None,
) -> pl.DataFrame:
    lf = df.lazy()
    if include:
        inc = pl.concat_list([pl.col("file").str.contains(_pattern_to_regex(p)) for p in include])
        lf = lf.filter(inc.list.any())
    if exclude:
        exc = pl.concat_list([pl.col("file").str.contains(_pattern_to_regex(p)) for p in exclude])
        lf = lf.filter(~exc.list.any())
    if impact:
        lf = lf.filter(pl.col("impact_bucket").is_in(impact))
    if confidence:
        lf = lf.filter(pl.col("confidence_bucket").is_in(confidence))
    if limit is not None:
        lf = lf.limit(limit)
    return lf.collect()
```

**Target files to modify**
- `tools/cq/core/findings_table.py` (new)
- `tools/cq/core/artifacts.py` (optional: persist findings table as parquet)

**Code/modules to delete**
- None

**Implementation checklist**
- [ ] Implement `FindingRecord` -> Polars table conversion
- [ ] Implement include/exclude path filtering (glob + regex)
- [ ] Implement `impact` and `confidence` bucket filters
- [ ] Implement limit
- [ ] Add optional parquet artifact for the filtered table

## Scope Item 4: CLI integration + report output

**Goal**: Wire Polars filtering into CLI and make impact/confidence visible in output.

**Representative code snippets**

```python
# tools/cq/cli.py
parser.add_argument("--impact", type=str, default="", help="Impact buckets: low,med,high")
parser.add_argument("--confidence", type=str, default="", help="Confidence buckets: low,med,high")
parser.add_argument("--include", action="append", default=[], help="Include path pattern")
parser.add_argument("--exclude", action="append", default=[], help="Exclude path pattern")
parser.add_argument("--limit", type=int, default=None, help="Max results")

# after result computed:
records = build_records(result)
filtered = apply_filters(...)
result = rehydrate_result(result, filtered)
```

```python
# tools/cq/core/report.py
# show impact/confidence tags instead of severity icons
line = f"- [impact:{impact_bucket}] [conf:{confidence_bucket}] {message} ({loc})"
```

**Target files to modify**
- `tools/cq/cli.py`
- `tools/cq/core/report.py`
- `tools/cq/core/schema.py` (if moving away from severity icons)

**Code/modules to delete**
- None

**Implementation checklist**
- [ ] Add CLI flags for the five filters
- [ ] Parse `--impact`/`--confidence` into bucket lists
- [ ] Filter findings with Polars before rendering
- [ ] Render impact/confidence tags in report
- [ ] Decide whether to keep or remove severity icons in final output

## Scope Item 5: Documentation updates

**Goal**: Update skill docs with new filters and usage examples.

**Representative code snippets**

```markdown
/cq impact build_graph_product --param repo_root --impact high --confidence high --include "tools/cq/"
```

**Target files to modify**
- `.codex/skills/cq/SKILL.md`
- `.claude/skills/cq/SKILL.md`

**Code/modules to delete**
- None

**Implementation checklist**
- [ ] Replace old options with new filter descriptions
- [ ] Add 2-3 minimal examples

## Scope Item 6: Tests (design-phase but required for correctness)

**Goal**: Validate scoring and filtering logic.

**Representative code snippets**

```python
def test_confidence_buckets() -> None:
    assert bucket(0.9) == "high"
    assert bucket(0.5) == "med"
    assert bucket(0.1) == "low"
```

**Target files to modify**
- `tools/cq/core/tests/test_scoring.py` (new)
- `tools/cq/core/tests/test_findings_table.py` (new)

**Code/modules to delete**
- None

**Implementation checklist**
- [ ] Add unit tests for bucket thresholds
- [ ] Add unit tests for include/exclude/limit filtering
- [ ] Add unit tests for impact/confidence bucket filtering

## Scope Item 7: Deferred deletions (after all scope items complete)

These removals should only happen once the new scoring and reporting pipeline is fully integrated.

**Deferred deletions**
- Remove `_severity_icon` and severity-based rendering from `tools/cq/core/report.py`
- Remove or deprecate `Finding.severity` if impact/confidence becomes the sole prioritization signal
- Remove any stale severity/category CLI flags if they are introduced during transition

**Checklist**
- [ ] Confirm all findings include impact/confidence fields
- [ ] Confirm report output no longer depends on severity
- [ ] Delete deprecated fields/functions and update schema accordingly
