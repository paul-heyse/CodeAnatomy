# 09 — LDMD Format and Progressive Disclosure

**Version:** 0.4.0
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
| `format.py` | 383 | Stack-validated parser, byte-offset indexing, section extraction |
| `writer.py` | 289 | Document generation, preview/body split, CqResult/SNB rendering |
| `__init__.py` | 21 | Public API re-exports |

### CLI Integration

| Module | LOC | Integration Point |
|--------|-----|-------------------|
| `cli_app/commands/ldmd.py` | 258 | Protocol commands (index, get, search, neighbors) |
| `cli_app/result.py` | 162 | LDMD renderer dispatch in `render_result()` |
| `cli_app/types.py` | 235 | `OutputFormat.ldmd` enum member |
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

- Top-level: `run_meta`, `summary`, `key_findings`
- TLDR blocks: `{parent}_tldr`
- Body blocks: `{parent}_body`
- Indexed: `section_0`, `section_1`
- Slice types: `definitions`, `structural_references`

---

## 3. Parser Architecture

### Stack Validation

**Function:** `build_index(content: bytes) -> LdmdIndex` (`format.py:65-145`)

**Algorithm:**
```python
open_stack: list[str] = []           # Active section IDs
seen_ids: set[str] = set()           # Enforce uniqueness
sections: list[SectionMeta] = []     # Finalized sections
open_sections: dict[str, dict] = {}  # Metadata for open sections

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
- UTF-8 multi-byte sequences preserved

### Index Structure

**Dataclasses** (`format.py:25-40`):

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

**Collapse policy** (`format.py:52-71`):

Uses lazy import to `neighborhood.section_layout` for collapse hints. Falls back to `collapsed=True` if module unavailable.

### Section Extraction

**Function:** `get_slice(content, index, section_id, mode, depth, limit_bytes)` (`format.py:180-248`)

**Modes:**

| Mode | Depth | Limit | Behavior |
|------|-------|-------|----------|
| `full` | caller | 0 | Extract entire section with nested content |
| `preview` | max(depth,1) | 4096 | Limited depth and bytes |
| `tldr` | 0 | from TLDR | Extract `{id}_tldr` subsection, fallback to preview |

**Algorithm:**
1. Locate section by ID → error if not found
2. Extract byte range: `content[start:end]`
3. TLDR mode: recursively parse slice, find `_tldr` subsection
4. Preview mode: constrain depth=1, limit=4096
5. Apply depth filter (collapse nested sections beyond depth)
6. Apply byte limit with UTF-8 safety

### Depth Filtering

**Function:** `_apply_depth_filter(content, max_depth)` (`format.py:251-288`)

Collapses nested sections beyond `max_depth` relative to first section marker:

- `depth=0`: Target section body only (no nested markers)
- `depth=1`: Direct child sections included

```python
stack: list[str] = []
for line in lines:
    relative_depth = max(0, len(stack) - 1)
    if relative_depth <= max_depth:
        kept.append(line)
```

### UTF-8 Safe Truncation

**Function:** `_safe_utf8_truncate(data, limit)` (`format.py:148-177`)

Backs up from limit until valid UTF-8 boundary found:

```python
candidate = data[:limit]
while limit > 0:
    try:
        candidate.decode("utf-8", errors="strict")
        return candidate
    except UnicodeDecodeError:
        limit -= 1
        candidate = data[:limit]
```

**Rationale:** Naive byte truncation splits multi-byte UTF-8 sequences. This ensures clean decode.

### Search and Navigation

**Search:** `search_sections(content, index, query)` (`format.py:291-333`)

- Simple case-insensitive substring matching
- Returns `[{"section_id": str, "line": int, "text": str}]`

**Navigation:** `get_neighbors(index, section_id)` (`format.py:336-383`)

- Returns `{"section_id": str, "prev": str|None, "next": str|None}`
- Sequential neighbors in document order (not hierarchical)

---

## 4. Writer Architecture

### Document Generation

**Primary renderers:**

| Function | Input | Output |
|----------|-------|--------|
| `render_ldmd_from_cq_result(result)` | `CqResult` | LDMD markdown |
| `render_ldmd_document(bundle)` | SNB | LDMD markdown |

**CqResult structure:**
```
<!--LDMD:BEGIN id="run_meta"-->
## {macro name}
**Root:** {root}  **Elapsed:** {ms}ms
<!--LDMD:END id="run_meta"-->

<!--LDMD:BEGIN id="summary"-->
## Summary
- **key:** value
<!--LDMD:END id="summary"-->

<!--LDMD:BEGIN id="key_findings"-->
## Key Findings (N)
  <!--LDMD:BEGIN id="key_findings_tldr"-->
  1. [severity] message @ file:line
  <!--LDMD:END id="key_findings_tldr"-->
  <!--LDMD:BEGIN id="key_findings_body"-->
  (remaining items)
  <!--LDMD:END id="key_findings_body"-->
<!--LDMD:END id="key_findings"-->

<!--LDMD:BEGIN id="section_0" title="...">
  ... (with _tldr/_body split)
<!--LDMD:END id="section_0"-->

<!--LDMD:BEGIN id="artifacts"-->
## Artifacts
- `artifact.json` (format)
<!--LDMD:END id="artifacts"-->
```

### Preview/Body Separation

**Constant:** `_CQ_PREVIEW_LIMIT = 5` (`writer.py:184`)

Sections with >5 items split into:
- **TLDR:** First 5 items, numbered (`1. [severity] message`)
- **Body:** Remaining items, bulleted (`- [severity] message`)

**Implementation** (`_emit_key_findings`, `writer.py:239-256`):

```python
preview = result.key_findings[:_CQ_PREVIEW_LIMIT]
lines.append(_ldmd_begin("key_findings_tldr", parent="key_findings"))
lines.extend(_finding_line(f, numbered=True, index=i) for i, f in enumerate(preview, 1))
lines.append(_ldmd_end("key_findings_tldr"))

if len(result.key_findings) > _CQ_PREVIEW_LIMIT:
    lines.append(_ldmd_begin("key_findings_body", parent="key_findings"))
    lines.extend(_finding_line(f) for f in result.key_findings[_CQ_PREVIEW_LIMIT:])
    lines.append(_ldmd_end("key_findings_body"))
```

**Rationale:** LLM agents retrieve TLDR for overview, then full body only if needed. Reduces context consumption.

### Helper Functions

**Marker generators** (`writer.py:187-205`):

```python
def _ldmd_begin(section_id: str, *, title="", level=0, parent="") -> str:
    attrs = [f'id="{section_id}"']
    if title: attrs.append(f'title="{title}"')
    if level > 0: attrs.append(f'level="{level}"')
    if parent: attrs.append(f'parent="{parent}"')
    return f"<!--LDMD:BEGIN {' '.join(attrs)}-->"

def _ldmd_end(section_id: str) -> str:
    return f'<!--LDMD:END id="{section_id}"-->'
```

**Finding formatter** (`writer.py:208-217`):

```python
def _finding_line(finding: Finding, *, numbered=False, index=0) -> str:
    anchor_ref = f" @ {finding.anchor.file}:{finding.anchor.line}" if finding.anchor else ""
    prefix = f"{index}." if numbered else "-"
    return f"{prefix} [{finding.severity}] {finding.message}{anchor_ref}"
```

### SNB Rendering

**Function:** `render_ldmd_document(bundle)` (`writer.py:12-44`)

**Structure:**
- Target section: subject name, file, byte span
- Neighborhood summary: node/edge counts
- Slices: definitions, references, imports (each with TLDR/body)
- Diagnostics: stage-specific messages
- Provenance manifest: section count, timestamps, artifact pointers

**Slice rendering** (`_emit_snb_slice`, `writer.py:85-114`):

```python
lines.append(_ldmd_begin(kind, title=kind.title(), level=2))
lines.append(f"### {kind.title()} ({total})")

if preview:
    lines.append(_ldmd_begin(f"{kind}_tldr", parent=kind, level=3))
    for i, node in enumerate(preview, 1):
        lines.append(f"{i}. {node.name} ({node.kind})")
    lines.append(_ldmd_end(f"{kind}_tldr"))

lines.append(_ldmd_begin(f"{kind}_body", parent=kind, level=3))
if total > len(preview):
    lines.append(f"*Full data: see artifact `slice_{kind}.json` ({total} items)*")
lines.append(_ldmd_end(f"{kind}_body"))
```

**Provenance** (`_emit_snb_provenance`, `writer.py:131-154`):

Links to full JSON artifacts for slices exceeding preview limits.

---

## 5. Protocol Commands

All commands live in `cli_app/commands/ldmd.py`. All return `CliTextResult` (bypasses standard rendering).

### INDEX

**Signature:** `cq ldmd index <path>`

**Output:** JSON section metadata

```json
{
  "sections": [
    {"id": "key_findings", "start_offset": 123, "end_offset": 456, "depth": 0, "collapsed": true}
  ],
  "total_bytes": 12345
}
```

### GET

**Signature:** `cq ldmd get <path> --id <section_id> [--mode {full|preview|tldr}] [--depth N] [--limit-bytes N]`

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--id` | Required | Section ID to extract |
| `--mode` | `full` | Extraction mode |
| `--depth` | `0` | Include nested sections up to depth |
| `--limit-bytes` | `0` | Max bytes (0=unlimited) |

**Modes:**
- `full`: Entire section with nested content (respects depth)
- `preview`: Depth=1, limit=4096
- `tldr`: Extract `{id}_tldr` subsection, fallback to preview

**Output:** Extracted markdown section

### SEARCH

**Signature:** `cq ldmd search <path> --query <text>`

**Output:** JSON array of matches

```json
[
  {"section_id": "key_findings", "line": 5, "text": "High-impact call site in process_batch()"}
]
```

### NEIGHBORS

**Signature:** `cq ldmd neighbors <path> --id <section_id>`

**Output:** JSON navigation object

```json
{
  "section_id": "key_findings",
  "prev": "summary",
  "next": "section_0"
}
```

---

## 6. CQ Integration

### Output Format

**Registration** (`cli_app/types.py:26`):

```python
class OutputFormat(StrEnum):
    md = "md"
    json = "json"
    both = "both"
    summary = "summary"
    mermaid = "mermaid"
    mermaid_class = "mermaid-class"
    dot = "dot"
    ldmd = "ldmd"  # LLM-friendly progressive disclosure
```

**Usage:**
```bash
cq search build_graph --format ldmd
cq q "entity=function name=~^build" --format ldmd
cq calls process_batch --format ldmd
```

### Renderer Dispatch

**Location:** `cli_app/result.py:100-104`

```python
if format_value == "ldmd":
    from tools.cq.ldmd.writer import render_ldmd_from_cq_result
    return render_ldmd_from_cq_result(result)
```

**Lazy import rationale:** Avoids circular dependencies, allows R6 (neighborhood) to evolve independently.

### CliTextResult

**Definition** (`context.py:135-147`):

```python
class CliTextResult(CqStruct, frozen=True):
    """Text payload for non-analysis protocol commands."""
    text: str
    media_type: str = "text/plain"
```

**Handling** (`result.py:133-136`):

```python
if not cli_result.is_cq_result:
    if isinstance(cli_result.result, CliTextResult):
        sys.stdout.write(f"{cli_result.result.text}\n")
    return cli_result.get_exit_code()
```

Protocol commands bypass standard rendering pipeline (which expects `CqResult`).

### Artifact Storage

LDMD output saved to `.cq/artifacts/` unless `--no-save-artifact` specified (`result.py:154-156`).

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
| Fail-fast validation | Precise error diagnostics |

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

1. **Hierarchical navigation:** Add `get_children()/get_parent()` functions
2. **Multi-document linking:** Support cross-doc refs via `<!--LDMD:REF doc="..." section="..."-->`
3. **Diff-aware updates:** In-place section replacement preserving offsets
4. **Structured metadata:** Generalize `tags` to JSON metadata for filtering/sorting
5. **Compression-aware:** Compress sections on-disk, decompress on-demand
6. **Streaming writer:** Reduce memory for multi-GB outputs

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
- Stack-validated nesting (immediate error detection)
- UTF-8-safe truncation (no performance penalty)
- Preview/body separation (agent context economics)
- Zero-runtime dependencies (stdlib only)

Integrates with CQ via `--format ldmd` and protocol commands (`index`, `get`, `search`, `neighbors`). Foundation for efficient agent consumption of large analysis artifacts, especially semantic neighborhood bundles.
