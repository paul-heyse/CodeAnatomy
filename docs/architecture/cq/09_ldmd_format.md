# 09 — LDMD Format and Progressive Disclosure

**Version:** 0.5.0
**Last Updated:** 2026-02-15

## Executive Summary

The LDMD (Long Document Markdown) subsystem (`tools/cq/ldmd/`) provides progressive disclosure for long CQ analysis artifacts. It embeds structured markers into markdown, enabling random-access retrieval, preview/body separation, and navigation without loading entire documents.

**Core capabilities:**
- Strict attribute-based marker grammar with stack validation
- Byte-offset indexing for O(1) section access
- Preview/body separation with TLDR blocks
- Three extraction modes (full, preview, tldr) with depth control
- UTF-8-safe truncation preserving character boundaries
- Protocol commands: index, get, search, neighbors
- Integration via `--format ldmd` (see [01_cli_rendering.md](01_cli_rendering.md))

**Design philosophy:** LDMD documents are simultaneously human-readable and machine-navigable. Markers are invisible HTML comments when rendered, but parseable for programmatic access.

---

## 1. Module Map

### Core Implementation

| Module | LOC | Responsibility |
|--------|-----|----------------|
| `format.py` | 376 | Stack-validated parser, byte-offset indexing, section extraction |
| `writer.py` | 358 | Document generation, preview/body split, CqResult/SNB rendering |
| `__init__.py` | 21 | Public API re-exports |

### CLI Integration

| Module | LOC | Integration Point |
|--------|-----|-------------------|
| `cli_app/commands/ldmd.py` | 274 | Protocol commands (index, get, search, neighbors) |
| `cli_app/result.py` | 381 | LDMD renderer dispatch in `render_result()` |
| `cli_app/types.py` | 288 | `OutputFormat.ldmd` enum member + `LdmdSliceMode` |
| `cli_app/context.py` | 189 | `CliTextResult` for protocol command output |

### Architecture

```
CQ Analysis → --format ldmd → writer.py → LDMD Document
                                              ↓
                          ┌─────────────────────────────┐
                          │  <!--LDMD:BEGIN id="..." -->│
                          │  Section content            │
                          │  <!--LDMD:END id="..."   -->│
                          └─────────────────────────────┘
                                     ↓
                ┌────────────────────┼────────────────────┐
                │                    │                    │
            INDEX               GET/SEARCH           NEIGHBORS
                │                    │                    │
                └────────────────────┴────────────────────┘
                                     ↓
                                 format.py
                          (build_index, get_slice)
```

**Data flow:**
1. CQ command produces `CqResult` or semantic neighborhood bundle
2. Writer renders with LDMD markers
3. Parser validates structure and builds byte-offset index
4. Protocol commands retrieve targeted sections

---

## 2. Format Specification

### Marker Grammar

```html
<!--LDMD:BEGIN id="section_id" title="Section Title" level="1" parent="parent_id" tags="tag1,tag2"-->
Section content (markdown)
<!--LDMD:END id="section_id"-->
```

**BEGIN attributes:**

| Attribute | Required | Type | Description |
|-----------|----------|------|-------------|
| `id` | Yes | string | Unique section identifier |
| `title` | No | string | Human-readable title |
| `level` | No | int | Nesting level (0=root) |
| `parent` | No | string | Parent section ID |
| `tags` | No | csv | Comma-separated tags |

**END attributes:**

| Attribute | Required | Description |
|-----------|----------|-------------|
| `id` | Yes | Must match paired BEGIN |

**Regex patterns** (`format.py:13-21`):

```python
_BEGIN_RE = re.compile(
    r'^<!--LDMD:BEGIN\s+id="([^"]+)"'
    r'(?:\s+title="([^"]*)")?'
    r'(?:\s+level="(\d+)")?'
    r'(?:\s+parent="([^"]*)")?'
    r'(?:\s+tags="([^"]*)")?'
    r"\s*-->$"
)
_END_RE = re.compile(r'^<!--LDMD:END\s+id="([^"]+)"\s*-->$')
```

### Nesting Rules

1. Every BEGIN must have matching END with same ID
2. ENDs must close in LIFO order (stack discipline)
3. Section IDs must be globally unique
4. Depth computed from stack size at END marker

**Valid:**
```html
<!--LDMD:BEGIN id="outer"-->
  <!--LDMD:BEGIN id="inner"-->
  <!--LDMD:END id="inner"-->
<!--LDMD:END id="outer"-->
```

**Invalid (mismatched):**
```html
<!--LDMD:BEGIN id="a"-->
  <!--LDMD:BEGIN id="b"-->
  <!--LDMD:END id="a"-->  <!-- ERROR: Expected 'b' -->
```

### ID Conventions

- Top-level: `run_meta`, `summary`, `key_findings`, `insight_card`
- TLDR blocks: `{parent}_tldr`
- Body blocks: `{parent}_body`
- Indexed: `section_0`, `section_1`
- Slice types: `definitions`, `structural_references`
- SNB sections: `target_tldr`, `neighborhood_summary`, `diagnostics`, `provenance`

---

## 3. Parser Architecture

### Stack Validation

**Function:** `build_index(content: bytes) -> LdmdIndex` (`format.py:76-151`)

**Algorithm:**
```python
open_stack: list[str] = []           # Active section IDs
seen_ids: set[str] = set()           # Enforce uniqueness
sections: list[SectionMeta] = []     # Finalized sections
open_sections: dict[str, _OpenSectionMeta] = {}  # Metadata for open sections

for line in content.splitlines(keepends=True):
    if BEGIN match:
        - Check duplicate ID → error if exists
        - Push to stack, record start offset

    if END match:
        - Check stack top matches → error if not
        - Pop stack, finalize section
        - Depth = len(stack) at END

if stack not empty:
    raise LdmdParseError("Unclosed sections")
```

**Invariants:**
- Byte offsets from raw bytes (not decoded text)
- Stack discipline: `open_stack[-1]` always matches expected END
- Single forward pass: O(n) for document size n
- UTF-8 multi-byte sequences preserved via `keepends=True` and raw byte tracking

### Index Structure

**Dataclasses** (`format.py:24-40`):

```python
@dataclass(frozen=True)
class SectionMeta:
    id: str
    start_offset: int  # Byte offset of BEGIN marker
    end_offset: int    # Byte offset after END marker
    depth: int         # Nesting depth (0=root)
    collapsed: bool    # Default collapse state

@dataclass(frozen=True)
class LdmdIndex:
    sections: list[SectionMeta]
    total_bytes: int
```

**Internal metadata** (`format.py:43-49`):

```python
@dataclass(frozen=True)
class _OpenSectionMeta:
    byte_start: int
    title: str
    level: int
    parent: str
    tags: str
```

**Collapse policy** (`format.py:52-73`):

Uses lazy import to `neighborhood.section_layout` for collapse hints. Falls back to `collapsed=True` if module unavailable. This avoids circular dependencies while allowing neighborhood-specific collapse policies.

```python
def _is_collapsed(section_id: str) -> bool:
    try:
        from tools.cq.neighborhood.section_layout import (
            _DYNAMIC_COLLAPSE_SECTIONS,
            _UNCOLLAPSED_SECTIONS,
        )
        if section_id in _UNCOLLAPSED_SECTIONS:
            return False
        if section_id in _DYNAMIC_COLLAPSE_SECTIONS:
            return True
    except ImportError:
        pass
    return True
```

### Section Extraction

**Function:** `get_slice(content, index, section_id, mode, depth, limit_bytes)` (`format.py:195-247`)

**Modes:**

| Mode | Depth | Limit | Behavior |
|------|-------|-------|----------|
| `full` | caller | 0 | Extract entire section with nested content |
| `preview` | max(depth,1) | 4096 | Limited depth and bytes |
| `tldr` | 0 | from TLDR | Extract `{id}_tldr` subsection, fallback to preview |

**Algorithm:**
1. Validate mode ∈ {full, preview, tldr}
2. Resolve section ID (supports `"root"` → earliest section)
3. Locate section by ID → error if not found
4. Extract byte range: `content[start:end]`
5. TLDR mode: recursively parse slice, find `_tldr` subsection, fallback to preview if not found
6. Preview mode: constrain depth=1, limit=4096
7. Apply depth filter (collapse nested sections beyond depth)
8. Apply byte limit with UTF-8 safety

**Root resolution** (`format.py:154-160`):

```python
def _resolve_section_id(index: LdmdIndex, section_id: str) -> str:
    if section_id != "root":
        return section_id
    if not index.sections:
        raise LdmdParseError("Document has no sections")
    return min(index.sections, key=lambda section: section.start_offset).id
```

### Depth Filtering

**Function:** `_apply_depth_filter(content, max_depth)` (`format.py:250-290`)

Collapses nested sections beyond `max_depth` relative to first section marker:

- `depth=0`: Target section body only (no nested markers)
- `depth=1`: Direct child sections included
- `depth=N`: Include sections nested up to N levels

```python
stack: list[str] = []
for line in lines:
    relative_depth = max(0, len(stack) - 1)
    if relative_depth <= max_depth:
        kept.append(line)
```

**Mechanism:**
- Tracks section stack depth
- Computes relative depth from first BEGIN marker
- Keeps lines when relative depth ≤ max_depth
- Preserves document structure while pruning deep nesting

### UTF-8 Safe Truncation

**Function:** `_safe_utf8_truncate(data, limit)` (`format.py:163-192`)

Backs up from limit until valid UTF-8 boundary found:

```python
if len(data) <= limit:
    return data

# Back up from limit to find valid boundary
while limit > 0:
    candidate = data[:limit]
    try:
        candidate.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        limit -= 1
    else:
        return candidate
return b""
```

**Rationale:** Naive byte truncation splits multi-byte UTF-8 sequences. This ensures clean decode by backing up to the nearest valid character boundary.

### Search and Navigation

**Search:** `search_sections(content, index, query)` (`format.py:293-335`)

- Simple case-insensitive substring matching
- Searches within each section's content
- Returns list of matches with section ID, line number, and text
- No regex or AST awareness

```python
def search_sections(
    content: bytes,
    index: LdmdIndex,
    *,
    query: str,
) -> list[dict[str, object]]:
    matches: list[dict[str, object]] = []
    query_lower = query.lower()

    for section in index.sections:
        section_content = content[section.start_offset : section.end_offset]
        section_text = section_content.decode("utf-8", errors="replace")

        if query_lower in section_text.lower():
            lines = section_text.split("\n")
            for line_idx, line in enumerate(lines):
                if query_lower in line.lower():
                    matches.append({
                        "section_id": section.id,
                        "line": line_idx,
                        "text": line.strip(),
                    })
    return matches
```

**Navigation:** `get_neighbors(index, section_id)` (`format.py:338-376`)

- Returns previous and next sections in document order (not hierarchical)
- Supports `"root"` as section_id (resolves to earliest section)
- Returns `{"section_id": str, "prev": str|None, "next": str|None}`

```python
def get_neighbors(
    index: LdmdIndex,
    *,
    section_id: str,
) -> dict[str, str | None]:
    resolved_id = _resolve_section_id(index, section_id)

    # Find section index
    section_idx = None
    for idx, s in enumerate(index.sections):
        if s.id == resolved_id:
            section_idx = idx
            break

    if section_idx is None:
        raise LdmdParseError(f"Section not found: {resolved_id}")

    prev_id = None if section_idx == 0 else index.sections[section_idx - 1].id
    next_id = None if section_idx >= len(index.sections) - 1 else index.sections[section_idx + 1].id

    return {"section_id": resolved_id, "prev": prev_id, "next": next_id}
```

---

## 4. Writer Architecture

### Document Generation

**Primary renderers:**

| Function | Input | Output | Location |
|----------|-------|--------|----------|
| `render_ldmd_from_cq_result(result)` | `CqResult` | LDMD markdown | `writer.py:159-182` |
| `render_ldmd_document(bundle)` | SNB | LDMD markdown | `writer.py:14-46` |

**CqResult structure:**
```
<!--LDMD:BEGIN id="run_meta" title="Run" level="1"-->
## {macro name}
**Root:** {root}  **Elapsed:** {ms}ms
<!--LDMD:END id="run_meta"-->

<!--LDMD:BEGIN id="insight_card" title="Insight Card" level="1"-->
(FrontDoorInsightV1 card rendering)
<!--LDMD:END id="insight_card"-->

<!--LDMD:BEGIN id="summary" title="Summary" level="1"-->
## Summary
- **key:** value
<!--LDMD:END id="summary"-->

<!--LDMD:BEGIN id="diagnostic_artifacts" title="Diagnostic Artifacts" level="2"-->
(offloaded artifact refs)
<!--LDMD:END id="diagnostic_artifacts"-->

<!--LDMD:BEGIN id="key_findings" title="Key Findings" level="1"-->
## Key Findings (N)
  <!--LDMD:BEGIN id="key_findings_tldr" parent="key_findings" level="2"-->
  1. [severity] message @ file:line
  <!--LDMD:END id="key_findings_tldr"-->
  <!--LDMD:BEGIN id="key_findings_body" parent="key_findings" level="2"-->
  (remaining items)
  <!--LDMD:END id="key_findings_body"-->
<!--LDMD:END id="key_findings"-->

<!--LDMD:BEGIN id="section_0" title="..." level="1">
  ... (with _tldr/_body split)
<!--LDMD:END id="section_0"-->

<!--LDMD:BEGIN id="artifacts" title="Artifacts" level="1"-->
## Artifacts
- `artifact.json` (format)
<!--LDMD:END id="artifacts"-->
```

### Preview/Body Separation

**Constant:** `_CQ_PREVIEW_LIMIT = 5` (`writer.py:187`)

Sections with >5 items split into:
- **TLDR:** First 5 items, numbered (`1. [severity] message`)
- **Body:** Remaining items, bulleted (`- [severity] message`)

**Implementation** (`_emit_key_findings`, `writer.py:308-325`):

```python
preview = result.key_findings[:_CQ_PREVIEW_LIMIT]
lines.append(_ldmd_begin("key_findings_tldr", parent="key_findings", level=2))
lines.extend(_finding_line(f, numbered=True, index=i) for i, f in enumerate(preview, 1))
lines.append(_ldmd_end("key_findings_tldr"))

if len(result.key_findings) > _CQ_PREVIEW_LIMIT:
    lines.append(_ldmd_begin("key_findings_body", parent="key_findings", level=2))
    lines.extend(_finding_line(f) for f in result.key_findings[_CQ_PREVIEW_LIMIT:])
    lines.append(_ldmd_end("key_findings_body"))
```

**Rationale:** LLM agents retrieve TLDR for overview, then full body only if needed. Reduces context consumption from thousands to dozens of tokens per section.

### Helper Functions

**Marker generators** (`writer.py:216-234`):

```python
def _ldmd_begin(
    section_id: str,
    *,
    title: str = "",
    level: int = 0,
    parent: str = "",
) -> str:
    attrs = [f'id="{section_id}"']
    if title:
        attrs.append(f'title="{title}"')
    if level > 0:
        attrs.append(f'level="{level}"')
    if parent:
        attrs.append(f'parent="{parent}"')
    return f"<!--LDMD:BEGIN {' '.join(attrs)}-->"

def _ldmd_end(section_id: str) -> str:
    return f'<!--LDMD:END id="{section_id}"-->'
```

**Finding formatter** (`writer.py:237-246`):

```python
def _finding_line(finding: object, *, numbered: bool = False, index: int = 0) -> str:
    from tools.cq.core.schema import Anchor, Finding

    if not isinstance(finding, Finding):
        return ""
    anchor_ref = ""
    if isinstance(finding.anchor, Anchor):
        anchor_ref = f" @ {finding.anchor.file}:{finding.anchor.line}"
    prefix = f"{index}." if numbered else "-"
    return f"{prefix} [{finding.severity}] {finding.message}{anchor_ref}"
```

### Insight Card Rendering

**Function:** `_emit_insight_card(lines, result)` (`writer.py:190-213`)

Extracts and renders FrontDoorInsightV1 card if present in summary:

```python
def _emit_insight_card(lines: list[str], result: CqResult) -> None:
    raw = result.summary.get("front_door_insight") if isinstance(result.summary, dict) else None
    if raw is None:
        return

    from tools.cq.core.front_door_insight import FrontDoorInsightV1, render_insight_card

    insight: FrontDoorInsightV1 | None = None
    if isinstance(raw, FrontDoorInsightV1):
        insight = raw
    elif isinstance(raw, dict):
        try:
            insight = convert_lax(raw, type_=FrontDoorInsightV1)
        except BoundaryDecodeError:
            return
    if insight is None:
        return

    card_lines = render_insight_card(insight)
    lines.append(_ldmd_begin("insight_card", title="Insight Card", level=1))
    lines.extend(card_lines)
    lines.append(_ldmd_end("insight_card"))
```

### SNB Rendering

**Function:** `render_ldmd_document(bundle)` (`writer.py:14-46`)

**Structure:**
- Target section: subject name, file, byte span (`_emit_snb_target`)
- Neighborhood summary: node/edge counts (`_emit_snb_summary`)
- Slices: definitions, references, imports (each with TLDR/body) (`_emit_snb_slices`)
- Diagnostics: stage-specific messages (`_emit_snb_diagnostics`)
- Provenance manifest: section count, timestamps, artifact pointers (`_emit_snb_provenance`)

**Slice rendering** (`_emit_snb_slice`, `writer.py:87-116`):

```python
lines.append(_ldmd_begin(kind, title=kind.replace("_", " ").title(), level=2))
lines.append(f"### {kind.replace('_', ' ').title()} ({total})")

if preview:
    lines.append(_ldmd_begin(f"{kind}_tldr", parent=kind, level=3))
    for i, node in enumerate(preview, 1):
        lines.append(
            f"{i}. {getattr(node, 'name', 'unknown')} ({getattr(node, 'kind', 'unknown')})"
        )
    lines.append(_ldmd_end(f"{kind}_tldr"))

lines.append(_ldmd_begin(f"{kind}_body", parent=kind, level=3))
if total > len(preview):
    lines.append(f"*Full data: see artifact `slice_{kind}.json` ({total} items)*")
else:
    for node in preview:
        node_file = getattr(node, "file_path", "")
        lines.append(
            f"- **{getattr(node, 'name', 'unknown')}** ({getattr(node, 'kind', 'unknown')})"
        )
        if node_file:
            lines.append(f"  - {node_file}")
lines.append(_ldmd_end(f"{kind}_body"))
```

**Provenance** (`_emit_snb_provenance`, `writer.py:133-156`):

Links to full JSON artifacts for slices exceeding preview limits:

```python
lines.append(_ldmd_begin("provenance", title="Provenance", level=1))
lines.append("## Provenance")
lines.append(f"- **Sections:** {len(slices)}")

meta = getattr(bundle, "meta", None)
if meta:
    created_at = getattr(meta, "created_at_ms", None)
    if created_at is not None:
        lines.append(f"- **Created at (ms):** {created_at}")

artifacts = getattr(bundle, "artifacts", [])
if artifacts:
    lines.append("- **Artifacts:**")
    for art in artifacts:
        storage_path = getattr(art, "storage_path", None)
        byte_size = getattr(art, "byte_size", 0)
        if storage_path:
            lines.append(f"  - `{Path(storage_path).name}` ({byte_size} bytes)")
        else:
            artifact_id = getattr(art, "artifact_id", "unknown")
            lines.append(f"  - `{artifact_id}` ({byte_size} bytes)")
```

---

## 5. Protocol Commands

All commands live in `cli_app/commands/ldmd.py`. All return `CliTextResult` (bypasses standard rendering).

### INDEX

**Signature:** `cq ldmd index <path>`

**Implementation:** `index()` (`ldmd.py:31-84`)

**Output:** JSON section metadata

```json
{
  "sections": [
    {"id": "key_findings", "start_offset": 123, "end_offset": 456, "depth": 0, "collapsed": true}
  ],
  "total_bytes": 12345
}
```

**Mechanism:**
1. Read document bytes
2. Call `build_index(content)`
3. Serialize SectionMeta to JSON
4. Return as `CliTextResult` with `application/json` media type

### GET

**Signature:** `cq ldmd get <path> --id <section_id> [--mode {full|preview|tldr}] [--depth N] [--limit-bytes N]`

**Implementation:** `get()` (`ldmd.py:87-150`)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--id` | Required | Section ID to extract |
| `--mode` | `full` | Extraction mode (LdmdSliceMode enum) |
| `--depth` | `0` | Include nested sections up to depth |
| `--limit-bytes` | `0` | Max bytes (0=unlimited) |

**Modes:**
- `full`: Entire section with nested content (respects depth)
- `preview`: Depth=1, limit=4096
- `tldr`: Extract `{id}_tldr` subsection, fallback to preview

**Output:** Extracted markdown section

**Mechanism:**
1. Read document bytes
2. Build index
3. Call `get_slice(content, index, section_id=id, mode=mode, depth=depth, limit_bytes=limit_bytes)`
4. Decode bytes to UTF-8
5. Return as `CliTextResult`

### SEARCH

**Signature:** `cq ldmd search <path> --query <text>`

**Implementation:** `search()` (`ldmd.py:153-196`)

**Output:** JSON array of matches

```json
[
  {"section_id": "key_findings", "line": 5, "text": "High-impact call site in process_batch()"}
]
```

**Mechanism:**
1. Read document bytes
2. Build index
3. Call `search_sections(content, index, query=query)`
4. Serialize matches to JSON
5. Return as `CliTextResult` with `application/json` media type

### NEIGHBORS

**Signature:** `cq ldmd neighbors <path> --id <section_id>`

**Implementation:** `neighbors()` (`ldmd.py:199-239`)

**Output:** JSON navigation object

```json
{
  "section_id": "key_findings",
  "prev": "summary",
  "next": "section_0"
}
```

**Mechanism:**
1. Read document bytes
2. Build index
3. Call `get_neighbors(index, section_id=id)`
4. Serialize to JSON
5. Return as `CliTextResult` with `application/json` media type

---

## 6. CQ Integration

### Output Format

**Registration** (`cli_app/types.py:13-36`):

```python
class OutputFormat(StrEnum):
    """Output format for CLI results."""

    md = "md"
    json = "json"
    both = "both"
    summary = "summary"
    mermaid = "mermaid"
    mermaid_class = "mermaid-class"
    dot = "dot"
    ldmd = "ldmd"  # LLM-friendly progressive disclosure markdown

    def __str__(self) -> str:
        return self.value
```

**LdmdSliceMode** (`cli_app/types.py:165-180`):

```python
class LdmdSliceMode(StrEnum):
    """LDMD extraction mode token."""

    full = "full"
    preview = "preview"
    tldr = "tldr"

    def __str__(self) -> str:
        return self.value
```

**Usage:**
```bash
cq search build_graph --format ldmd
cq q "entity=function name=~^build" --format ldmd
cq calls process_batch --format ldmd
```

### Renderer Dispatch

**Location:** `cli_app/result.py:133-137`

```python
# LDMD format uses lazy import (implemented in R6)
if format_value == "ldmd":
    from tools.cq.ldmd.writer import render_ldmd_from_cq_result

    return render_ldmd_from_cq_result(result)
```

**Lazy import rationale:** Avoids circular dependencies, allows R6 (neighborhood) to evolve independently.

### CliTextResult

**Definition** (`context.py:135-147`):

```python
class CliTextResult(CqStruct, frozen=True):
    """Text payload for non-analysis protocol commands.

    Parameters
    ----------
    text
        The text content to output.
    media_type
        MIME type of the content (default: text/plain).
    """

    text: str
    media_type: str = "text/plain"
```

**Handling** (in `result.py` rendering pipeline):

Protocol commands bypass standard rendering pipeline (which expects `CqResult`) by returning `CliTextResult`. The CLI infrastructure writes the text directly to stdout.

### Artifact Storage

LDMD output saved to `.cq/artifacts/` unless `--no-save-artifact` specified. Artifact storage integrated with standard CQ artifact persistence mechanisms.

---

## 7. Usage Patterns

### Basic Generation

```bash
# Generate LDMD documents
cq search build_graph --format ldmd > output.ldmd.md
cq calls process_batch --format ldmd > calls.ldmd.md
cq q "entity=function name=~^build" --format ldmd > query.ldmd.md
```

### Progressive Disclosure Workflow

**1. Index:**
```bash
cq ldmd index output.ldmd.md
# → JSON with section metadata
```

**2. Quick overview (TLDR):**
```bash
cq ldmd get output.ldmd.md --id key_findings --mode tldr
# → First 5 key findings only
```

**3. Full section if needed:**
```bash
cq ldmd get output.ldmd.md --id key_findings --mode full
# → All findings
```

**4. Search:**
```bash
cq ldmd search output.ldmd.md --query "process_batch"
# → JSON matches with section IDs
```

**5. Navigate:**
```bash
cq ldmd neighbors output.ldmd.md --id key_findings
# → {"prev": "summary", "next": "section_0"}
```

### Agent Consumption

LLM agents analyzing large CQ output:

1. **Index** to see available sections
2. **TLDR** all top-level sections (quick scan)
3. **Full sections** only for relevant findings
4. **Search** for specific symbols
5. **Navigate** to related sections

Reduces context from tens of thousands of tokens to hundreds.

---

## 8. Architectural Analysis

### Design Strengths

| Strength | Impact |
|----------|--------|
| Zero stdlib dependencies | Suitable for standalone tools |
| Byte-offset indexing | O(1) random access after indexing |
| UTF-8 safety | No performance penalty for common case |
| Invisible markers | Markdown renders cleanly in viewers |
| Preview/body split | Aligns with LLM context economics |
| Fail-fast validation | Precise error diagnostics via stack validation |
| Root resolution | `--id root` auto-finds earliest section |
| Lazy collapse policy | Avoids circular deps via runtime import |

### Design Tensions

| Tension | Trade-off |
|---------|-----------|
| Marker verbosity | ~70 bytes/section; 10KB overhead for 100s of sections |
| Flat index | No tree structure; hierarchical queries need post-processing |
| Simple search | Substring only; no regex or AST awareness |
| TLDR lookup | Recursive parse; could cascade for deep nesting |
| Collapse policy | Couples parser to neighborhood layout via lazy import |
| No version marker | Format evolution breaks old parsers silently |

### Extension Vectors

1. **Hierarchical navigation:** Add `get_children()/get_parent()` functions using parent attributes
2. **Multi-document linking:** Support cross-doc refs via `<!--LDMD:REF doc="..." section="..."-->`
3. **Diff-aware updates:** In-place section replacement preserving offsets
4. **Structured metadata:** Generalize `tags` to JSON metadata for filtering/sorting
5. **Compression-aware:** Compress sections on-disk, decompress on-demand
6. **Streaming writer:** Reduce memory for multi-GB outputs via generator-based emission
7. **Version markers:** Add format version to BEGIN markers for graceful evolution
8. **Hierarchical search:** Use parent/level for tree-aware navigation

### Subsystem Relationships

| Subsystem | LDMD Integration |
|-----------|------------------|
| **Search ([02](02_search_subsystem.md))** | Groups by function, TLDR=top-5 per group |
| **Query ([03](03_query_subsystem.md))** | Retrieve only relevant entity type sections |
| **Analysis ([04](04_analysis_commands.md))** | TLDR=high-impact nodes, body=full graph |
| **Multi-step ([05](05_multi_step_execution.md))** | Each step as section, TLDR=step outcomes |
| **Neighborhood ([08](08_neighborhood_subsystem.md))** | Primary driver; dozens of slices need preview/body split |

---

## Summary

LDMD provides lightweight progressive disclosure for long markdown documents. Its strict marker grammar, byte-offset indexing, and preview/body separation enable efficient random access—critical for LLM agents within context constraints.

**Key innovations:**
- Attribute-based markers (human-readable HTML comments, machine-parseable)
- Stack-validated nesting (immediate error detection with precise diagnostics)
- UTF-8-safe truncation (no performance penalty, graceful boundary handling)
- Preview/body separation (agent context economics via 5-item TLDR blocks)
- Zero-runtime dependencies (stdlib only: re, dataclasses, pathlib)
- Root resolution (auto-find earliest section for agent convenience)
- Lazy collapse policy (runtime coupling via try/except import)

Integrates with CQ via `--format ldmd` and protocol commands (`index`, `get`, `search`, `neighbors`). Foundation for efficient agent consumption of large analysis artifacts, especially semantic neighborhood bundles.

**File references:**
- Core parser: `/Users/paulheyse/CodeAnatomy/tools/cq/ldmd/format.py` (376 LOC)
- Writer: `/Users/paulheyse/CodeAnatomy/tools/cq/ldmd/writer.py` (358 LOC)
- Protocol commands: `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/commands/ldmd.py` (274 LOC)
- Renderer dispatch: `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/result.py:133-137`
