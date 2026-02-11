# 09 — LDMD Format and Progressive Disclosure

## Executive Summary

The LDMD (Long Document Markdown) subsystem (`tools/cq/ldmd/`) provides a progressive disclosure protocol for long CQ analysis artifacts. It addresses a fundamental constraint of LLM-based code analysis: context window limits make large outputs difficult to consume. LDMD embeds structured section markers into markdown documents, enabling random-access retrieval, preview/body separation, and navigation without loading entire documents.

**Key characteristics:**

- Strict attribute-based marker grammar with stack-validated nesting
- Byte-offset indexing for efficient random access without full parse
- Preview/body separation with TLDR blocks for progressive disclosure
- Three extraction modes (full, preview, tldr) with depth control
- UTF-8-safe truncation preserving character boundaries
- Protocol commands for index, get, search, and neighbors operations
- Integration with CQ's output format system via `OutputFormat.ldmd`
- Provenance manifests linking to full artifact payloads

**Design philosophy:** LDMD documents are **simultaneously human-readable and machine-navigable**. Markers are HTML comments, invisible when rendered, but parseable for programmatic access. A reader can consume the full markdown naturally, while an LLM agent can retrieve targeted sections on-demand.

**Target audience:** Advanced LLM programmers proposing architectural improvements.

---

## 1. Module Map

### Core Modules

| Module | Size (LOC) | Responsibility |
|--------|------------|----------------|
| `format.py` | 383 | Strict parser with stack validation, byte-offset indexing, section extraction |
| `writer.py` | 289 | Document generation, preview/body split, CqResult/SNB rendering |
| `__init__.py` | 21 | Public API re-exports |

### CLI Integration

| Module | Size (LOC) | Responsibility |
|--------|------------|----------------|
| `cli_app/commands/ldmd.py` | 258 | Protocol commands (index, get, search, neighbors) |
| `cli_app/result.py` | 162 | LDMD renderer dispatch in `render_result()` |
| `cli_app/types.py` | 235 | `OutputFormat.ldmd` enum member (line 26) |
| `cli_app/context.py` | 189 | `CliTextResult` for protocol command output |

### Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────┐
│                      CQ Analysis Pipeline                        │
│  (search, query, calls, impact, run, neighborhood)               │
└──────────────────────────┬───────────────────────────────────────┘
                           │
                           │ --format ldmd
                           ▼
                  ┌────────────────────┐
                  │  writer.py         │
                  │  ─────────────     │
                  │  render_ldmd_*()   │──┐
                  └────────────────────┘  │
                           │              │
                           │ LDMD markers │
                           ▼              │
                  ┌────────────────────┐  │
                  │  LDMD Document     │◀─┘
                  │  (markdown + <!--) │
                  │                    │
                  │  <!--LDMD:BEGIN    │
                  │  Section content   │
                  │  <!--LDMD:END      │
                  └─────┬──────────────┘
                        │
          ┌─────────────┼─────────────┐
          │             │             │
          ▼             ▼             ▼
    ┌─────────┐  ┌──────────┐  ┌──────────┐
    │ INDEX   │  │   GET    │  │  SEARCH  │
    └─────────┘  └──────────┘  └──────────┘
          │             │             │
          │             │             │
          └─────────────┴─────────────┘
                        │
                        ▼
              ┌──────────────────┐
              │   format.py      │
              │   ─────────      │
              │   build_index()  │
              │   get_slice()    │
              │   search_*()     │
              └──────────────────┘
```

**Data flow:**

1. CQ command produces `CqResult` or semantic neighborhood bundle
2. Writer renders document with LDMD markers (`<!--LDMD:BEGIN id="..." ...-->`)
3. Parser validates structure and builds byte-offset index (`LdmdIndex`)
4. Protocol commands retrieve targeted sections without full document load

---

## 2. Format Specification

### Marker Grammar

LDMD uses HTML comment markers with attribute-based metadata:

```html
<!--LDMD:BEGIN id="section_id" title="Section Title" level="1" parent="parent_id" tags="tag1,tag2"-->
Section content goes here.
Nested sections allowed.
<!--LDMD:END id="section_id"-->
```

**BEGIN marker attributes:**

| Attribute | Required | Description | Example |
|-----------|----------|-------------|---------|
| `id` | Yes | Unique section identifier | `"key_findings"` |
| `title` | No | Human-readable title | `"Key Findings"` |
| `level` | No | Nesting level (0=root) | `"1"` |
| `parent` | No | Parent section ID | `"summary"` |
| `tags` | No | Comma-separated tags | `"preview,critical"` |

**END marker attributes:**

| Attribute | Required | Description |
|-----------|----------|-------------|
| `id` | Yes | Must match paired BEGIN |

**Regex patterns** (from `format.py:14-22`):

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

LDMD enforces strict stack-based nesting:

1. Every BEGIN must have a matching END with the same ID
2. ENDs must close in LIFO order (stack discipline)
3. Section IDs must be globally unique within a document
4. Depth is computed from stack size at END marker

**Valid nesting:**

```html
<!--LDMD:BEGIN id="outer"-->
  Content A
  <!--LDMD:BEGIN id="inner"-->
    Content B
  <!--LDMD:END id="inner"-->
  Content C
<!--LDMD:END id="outer"-->
```

**Invalid (mismatched END):**

```html
<!--LDMD:BEGIN id="a"-->
  <!--LDMD:BEGIN id="b"-->
  <!--LDMD:END id="a"-->  <!-- ERROR: Expected 'b', got 'a' -->
<!--LDMD:END id="b"-->
```

**Invalid (duplicate ID):**

```html
<!--LDMD:BEGIN id="summary"-->
<!--LDMD:END id="summary"-->
<!--LDMD:BEGIN id="summary"-->  <!-- ERROR: Duplicate ID -->
<!--LDMD:END id="summary"-->
```

### ID Scheme Convention

While IDs can be arbitrary strings, writers follow these conventions:

- **Top-level sections:** Descriptive names (`run_meta`, `summary`, `key_findings`)
- **TLDR blocks:** Parent ID + `_tldr` suffix (`key_findings_tldr`)
- **Body blocks:** Parent ID + `_body` suffix (`key_findings_body`)
- **Indexed sections:** Prefix + index (`section_0`, `section_1`)
- **Slice types:** Lowercase with underscores (`definitions`, `structural_references`)

---

## 3. Parser Architecture

### Stack-Based Validation

**Function:** `build_index()` (`format.py:65-145`)

**Signature:**

```python
def build_index(content: bytes) -> LdmdIndex:
    """Build section index in single forward pass over raw bytes.

    Validates:
    - BEGIN/END nesting via stack
    - Duplicate section IDs
    - Byte offsets from raw content (not decoded text)

    Parameters
    ----------
    content
        Raw LDMD document as bytes.

    Returns
    -------
    LdmdIndex
        Index of sections with byte offsets.

    Raises
    ------
    LdmdParseError
        If structure is invalid (mismatched nesting, duplicate IDs, unclosed sections).
    """
```

**Algorithm** (simplified from `format.py:92-145`):

```python
open_stack: list[str] = []           # Active section IDs
seen_ids: set[str] = set()           # All section IDs encountered
sections: list[SectionMeta] = []     # Finalized sections
open_sections: dict[str, dict] = {}  # Metadata for open sections

byte_offset = 0
for raw_line in content.splitlines(keepends=True):
    line_byte_len = len(raw_line)
    line = raw_line.rstrip(b"\r\n").decode("utf-8", errors="replace")

    # BEGIN marker: push to stack, record start offset
    begin_match = _BEGIN_RE.match(line)
    if begin_match:
        sid = begin_match.group(1)
        if sid in seen_ids:
            raise LdmdParseError(f"Duplicate section ID: {sid}")
        seen_ids.add(sid)
        open_stack.append(sid)
        open_sections[sid] = {
            "byte_start": byte_offset,
            "title": begin_match.group(2) or "",
            "level": int(begin_match.group(3)) if begin_match.group(3) else 0,
            # ... other attributes
        }

    # END marker: pop from stack, finalize section
    end_match = _END_RE.match(line)
    if end_match:
        sid = end_match.group(1)
        if not open_stack or open_stack[-1] != sid:
            expected = open_stack[-1] if open_stack else "none"
            raise LdmdParseError(f"Mismatched END: expected '{expected}', got '{sid}'")
        open_stack.pop()
        section_meta = open_sections.pop(sid)
        sections.append(
            SectionMeta(
                id=sid,
                start_offset=section_meta["byte_start"],
                end_offset=byte_offset + line_byte_len,
                depth=len(open_stack),  # Depth at END determines nesting
                collapsed=_is_collapsed(sid),
            )
        )

    byte_offset += line_byte_len

# Validate all sections closed
if open_stack:
    raise LdmdParseError(f"Unclosed sections: {open_stack}")

return LdmdIndex(sections=sections, total_bytes=len(content))
```

**Key invariants:**

1. **Byte offsets from raw content:** Offsets computed from `raw_line` bytes, not decoded text. UTF-8 multi-byte sequences preserved.
2. **Stack discipline:** `open_stack[-1]` always matches expected END ID.
3. **Depth = stack size at END:** A section's depth reflects how many ancestors are still open when it closes.
4. **Single forward pass:** No backtracking; O(n) complexity for document size n.

### Index Structure

**Dataclass:** `LdmdIndex` (`format.py:37-42`)

```python
@dataclass(frozen=True)
class LdmdIndex:
    """Index of LDMD sections with byte offsets."""

    sections: list[SectionMeta]
    total_bytes: int
```

**Section metadata:** `SectionMeta` (`format.py:25-34`)

```python
@dataclass(frozen=True)
class SectionMeta:
    """Metadata for a single LDMD section."""

    id: str
    start_offset: int  # Byte offset of BEGIN marker
    end_offset: int    # Byte offset after END marker
    depth: int         # Nesting depth (0=root)
    collapsed: bool    # Whether section should be collapsed by default
```

**Collapse policy** (`format.py:44-62`):

The `collapsed` field uses a lazy import to consult `section_layout.py` (from neighborhood subsystem):

```python
def _is_collapsed(section_id: str) -> bool:
    """Determine if section should be collapsed by default.

    Uses lazy import to avoid dependency on R5 section_layout module.
    Falls back to True if the module isn't available yet.
    """
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
    return True  # Default: collapsed
```

This allows neighborhood outputs to control which sections are expanded by default without coupling the parser to R5 internals.

### Section Extraction

**Function:** `get_slice()` (`format.py:180-248`)

**Signature:**

```python
def get_slice(
    content: bytes,
    index: LdmdIndex,
    *,
    section_id: str,
    mode: str = "full",
    depth: int = 0,
    limit_bytes: int = 0,
) -> bytes:
    """Extract content from a section by ID.

    Parameters
    ----------
    content
        Full LDMD document bytes.
    index
        Pre-built section index.
    section_id
        Target section ID.
    mode
        Extraction mode: "full", "preview", or "tldr".
    depth
        Include nested sections up to this depth (0 = no nesting).
    limit_bytes
        Max bytes to return (0 = unlimited, uses safe UTF-8 truncation).

    Returns
    -------
    bytes
        Section content as bytes.

    Raises
    ------
    LdmdParseError
        If section_id is not found.
    """
```

**Extraction modes:**

| Mode | Behavior | Default Depth | Default Limit |
|------|----------|---------------|---------------|
| `full` | Extract entire section with all nested content | 0 (caller controls) | 0 (unlimited) |
| `preview` | Extract section with limited depth/bytes | max(depth, 1) | 4096 bytes |
| `tldr` | Extract `{section_id}_tldr` subsection if exists, else preview | 0 | From TLDR bounds |

**Algorithm** (`format.py:216-248`):

```python
# 1. Locate section by ID
section = next((s for s in index.sections if s.id == section_id), None)
if section is None:
    raise LdmdParseError(f"Section not found: {section_id}")

# 2. Extract byte range
slice_data = content[section.start_offset : section.end_offset]

# 3. TLDR mode: look for nested _tldr subsection
if mode == "tldr":
    tldr_id = f"{section_id}_tldr"
    try:
        local_index = build_index(slice_data)
        tldr_section = next((s for s in local_index.sections if s.id == tldr_id), None)
        if tldr_section is not None:
            slice_data = slice_data[tldr_section.start_offset : tldr_section.end_offset]
            depth = 0  # TLDR is self-contained
        else:
            mode = "preview"  # Fallback: no TLDR found
    except LdmdParseError:
        mode = "preview"

# 4. Preview mode: constrain depth and limit
if mode == "preview":
    depth = max(0, min(depth if depth > 0 else 1, 1))
    if limit_bytes <= 0:
        limit_bytes = 4096

# 5. Apply depth filter (collapse nested sections beyond depth)
slice_data = _apply_depth_filter(slice_data, max_depth=max(0, depth))

# 6. Apply byte limit with UTF-8 safety
if limit_bytes > 0:
    slice_data = _safe_utf8_truncate(slice_data, limit_bytes)

return slice_data
```

### Depth Filtering

**Function:** `_apply_depth_filter()` (`format.py:251-288`)

Collapses nested sections beyond `max_depth` relative to the first section marker:

```python
def _apply_depth_filter(content: bytes, *, max_depth: int) -> bytes:
    """Filter LDMD content to include markers/content up to max nested depth.

    Depth is relative to the first section marker in the provided slice:
    - depth 0 keeps only the target section body
    - depth 1 includes direct child sections
    """
    if max_depth < 0:
        return b""

    lines = content.splitlines(keepends=True)
    stack: list[str] = []
    kept: list[bytes] = []

    for raw_line in lines:
        line = raw_line.rstrip(b"\r\n").decode("utf-8", errors="replace")
        begin_match = _BEGIN_RE.match(line)
        if begin_match:
            relative_depth = max(0, len(stack) - 1)
            if relative_depth <= max_depth:
                kept.append(raw_line)
            stack.append(begin_match.group(1))
            continue

        end_match = _END_RE.match(line)
        if end_match:
            relative_depth = max(0, len(stack) - 1)
            if relative_depth <= max_depth:
                kept.append(raw_line)
            if stack and stack[-1] == end_match.group(1):
                stack.pop()
            continue

        relative_depth = max(0, len(stack) - 1)
        if relative_depth <= max_depth:
            kept.append(raw_line)

    return b"".join(kept)
```

**Example:**

Given:
```html
<!--LDMD:BEGIN id="outer"-->
Outer content
<!--LDMD:BEGIN id="inner"-->
Inner content
<!--LDMD:END id="inner"-->
More outer content
<!--LDMD:END id="outer"-->
```

- `max_depth=0`: `"Outer content\nMore outer content\n"` (no nested markers)
- `max_depth=1`: Full content (includes `inner` section)

### UTF-8 Safe Truncation

**Function:** `_safe_utf8_truncate()` (`format.py:148-177`)

Truncates byte arrays at UTF-8 character boundaries:

```python
def _safe_utf8_truncate(data: bytes, limit: int) -> bytes:
    """Truncate bytes at limit, preserving UTF-8 boundaries.

    Uses strict decode validation after boundary adjustment.

    Parameters
    ----------
    data
        Bytes to truncate.
    limit
        Maximum byte length.

    Returns
    -------
    bytes
        Truncated data with valid UTF-8 boundaries.
    """
    if len(data) <= limit:
        return data

    # Back up from limit to find valid boundary
    candidate = data[:limit]
    while limit > 0:
        try:
            candidate.decode("utf-8", errors="strict")
            return candidate
        except UnicodeDecodeError:
            limit -= 1
            candidate = data[:limit]
    return b""
```

**Rationale:** Naive byte truncation can split multi-byte UTF-8 sequences, causing decode errors. This function walks backward from the limit until it finds a valid boundary, ensuring the truncated result decodes cleanly.

### Search and Navigation

**Search function:** `search_sections()` (`format.py:291-333`)

```python
def search_sections(
    content: bytes,
    index: LdmdIndex,
    *,
    query: str,
) -> list[dict[str, object]]:
    """Search within LDMD sections for text matching query.

    Parameters
    ----------
    content
        Full LDMD document bytes.
    index
        Pre-built section index.
    query
        Search query string (simple text search).

    Returns
    -------
    list[dict]
        List of matches with section_id and match context.
    """
    matches: list[dict[str, object]] = []
    query_lower = query.lower()

    for section in index.sections:
        section_content = content[section.start_offset : section.end_offset]
        section_text = section_content.decode("utf-8", errors="replace")

        if query_lower in section_text.lower():
            # Find all occurrences in this section
            lines = section_text.split("\n")
            for line_idx, line in enumerate(lines):
                if query_lower in line.lower():
                    matches.append(
                        {
                            "section_id": section.id,
                            "line": line_idx,
                            "text": line.strip(),
                        }
                    )

    return matches
```

**Navigation function:** `get_neighbors()` (`format.py:336-383`)

```python
def get_neighbors(
    index: LdmdIndex,
    *,
    section_id: str,
) -> dict[str, str | None]:
    """Get neighboring sections for navigation.

    Parameters
    ----------
    index
        Pre-built section index.
    section_id
        Target section ID.

    Returns
    -------
    dict
        Navigation info with prev/next section IDs.

    Raises
    ------
    LdmdParseError
        If section_id is not found.
    """
    # Find section index
    section_idx = None
    for idx, s in enumerate(index.sections):
        if s.id == section_id:
            section_idx = idx
            break

    if section_idx is None:
        raise LdmdParseError(f"Section not found: {section_id}")

    prev_id = None
    next_id = None

    if section_idx > 0:
        prev_id = index.sections[section_idx - 1].id

    if section_idx < len(index.sections) - 1:
        next_id = index.sections[section_idx + 1].id

    return {
        "section_id": section_id,
        "prev": prev_id,
        "next": next_id,
    }
```

Returns sequential neighbors in document order (not hierarchical parent/child).

---

## 4. Writer Architecture

### Document Generation API

The writer module (`writer.py`) provides two primary renderers:

1. **`render_ldmd_from_cq_result()`** (`writer.py:157-179`) - Renders `CqResult` structures
2. **`render_ldmd_document()`** (`writer.py:12-44`) - Renders semantic neighborhood bundles (SNB)

**CqResult renderer signature:**

```python
def render_ldmd_from_cq_result(result: CqResult) -> str:
    """Render CqResult as LDMD-marked markdown.

    Adapts CqResult's sections/findings structure into LDMD progressive
    disclosure format. Used by the --format ldmd dispatch path.

    Parameters
    ----------
    result
        CQ analysis result to render.

    Returns
    -------
    str
        LDMD-marked markdown string.
    """
    lines: list[str] = []
    _emit_run_meta(lines, result)
    _emit_summary(lines, result)
    _emit_key_findings(lines, result)
    _emit_sections(lines, result)
    _emit_artifacts(lines, result)
    return "\n".join(lines)
```

**Document structure for CqResult:**

```
<!--LDMD:BEGIN id="run_meta"-->
## {macro name}
**Root:** {root path}
**Elapsed:** {elapsed_ms}ms
<!--LDMD:END id="run_meta"-->

<!--LDMD:BEGIN id="summary"-->
## Summary
- **key:** value
<!--LDMD:END id="summary"-->

<!--LDMD:BEGIN id="key_findings"-->
## Key Findings (N)
  <!--LDMD:BEGIN id="key_findings_tldr"-->
  1. [severity] message @ file:line
  2. ...
  <!--LDMD:END id="key_findings_tldr"-->
  <!--LDMD:BEGIN id="key_findings_body"-->
  - [severity] message @ file:line
  <!--LDMD:END id="key_findings_body"-->
<!--LDMD:END id="key_findings"-->

<!--LDMD:BEGIN id="section_0" title="...">
## {section title}
  <!--LDMD:BEGIN id="section_0_tldr"-->
  (preview items)
  <!--LDMD:END id="section_0_tldr"-->
  <!--LDMD:BEGIN id="section_0_body"-->
  (remaining items)
  <!--LDMD:END id="section_0_body"-->
<!--LDMD:END id="section_0"-->

<!--LDMD:BEGIN id="artifacts"-->
## Artifacts
- `artifact.json` (format)
<!--LDMD:END id="artifacts"-->
```

### Preview/Body Separation

**Preview limit constant:** `_CQ_PREVIEW_LIMIT = 5` (`writer.py:184`)

Sections with more than 5 findings split into:

- **TLDR block:** First 5 items, numbered (`1. [severity] message`)
- **Body block:** Remaining items, bulleted (`- [severity] message`)

**Example split** (`_emit_key_findings`, `writer.py:239-256`):

```python
def _emit_key_findings(lines: list[str], result: CqResult) -> None:
    if not result.key_findings:
        return
    lines.append(_ldmd_begin("key_findings", title="Key Findings", level=1))
    lines.append(f"## Key Findings ({len(result.key_findings)})")

    preview = result.key_findings[:_CQ_PREVIEW_LIMIT]
    lines.append(_ldmd_begin("key_findings_tldr", parent="key_findings", level=2))
    lines.extend(_finding_line(f, numbered=True, index=i) for i, f in enumerate(preview, 1))
    lines.append(_ldmd_end("key_findings_tldr"))

    if len(result.key_findings) > _CQ_PREVIEW_LIMIT:
        lines.append(_ldmd_begin("key_findings_body", parent="key_findings", level=2))
        lines.extend(_finding_line(f) for f in result.key_findings[_CQ_PREVIEW_LIMIT:])
        lines.append(_ldmd_end("key_findings_body"))

    lines.append(_ldmd_end("key_findings"))
    lines.append("")
```

**Rationale:** LLM agents can retrieve the TLDR block for an overview, then fetch the full body only if needed. This reduces context consumption for large results.

### Marker Helper Functions

**BEGIN marker generator** (`writer.py:187-201`):

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
```

**END marker generator** (`writer.py:204-205`):

```python
def _ldmd_end(section_id: str) -> str:
    return f'<!--LDMD:END id="{section_id}"-->'
```

**Finding formatter** (`writer.py:208-217`):

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

### Semantic Neighborhood Bundle Rendering

**Function:** `render_ldmd_document()` (`writer.py:12-44`)

Renders neighborhood bundles with:

- **Target section:** Subject name, file, byte span
- **Neighborhood summary:** Node/edge counts
- **Slices:** Definitions, references, imports, etc. (each with TLDR/body split)
- **Diagnostics:** Stage-specific messages
- **Provenance manifest:** Section count, timestamps, artifact pointers

**Example slice rendering** (`_emit_snb_slice`, `writer.py:85-114`):

```python
def _emit_snb_slice(lines: list[str], s: object) -> None:
    kind = getattr(s, "kind", "unknown")
    total = getattr(s, "total", 0)
    preview = getattr(s, "preview", [])

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
    lines.append(_ldmd_end(kind))
    lines.append("")
```

**Provenance manifest** (`_emit_snb_provenance`, `writer.py:131-154`):

```python
def _emit_snb_provenance(lines: list[str], bundle: object) -> None:
    slices = getattr(bundle, "slices", [])
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
    lines.append(_ldmd_end("provenance"))
```

Links to full JSON artifacts for slices that exceed preview limits.

---

## 5. Protocol Commands

LDMD exposes four protocol commands via `cli_app/commands/ldmd.py`:

### INDEX Command

**Function:** `index()` (`ldmd.py:56-113`)

**Purpose:** Build section index and return metadata as JSON.

**Signature:**

```bash
cq ldmd index <path>
```

**CLI parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `path` | str | Yes | Path to LDMD document |

**Output:** JSON with section metadata

```json
{
  "sections": [
    {
      "id": "key_findings",
      "start_offset": 123,
      "end_offset": 456,
      "depth": 0,
      "collapsed": true
    },
    ...
  ],
  "total_bytes": 12345
}
```

**Implementation:**

```python
@ldmd_app.command
def index(
    path: Annotated[str, cyclopts.Parameter(help="Path to LDMD document")],
    *,
    ctx: Annotated[CliContext | None, cyclopts.Parameter(parse=False)] = None,
) -> CliResult:
    """Index an LDMD document and return section metadata."""
    if ctx is None:
        msg = "CliContext not injected"
        raise RuntimeError(msg)
    doc_path = Path(path)
    if not doc_path.exists():
        return _text_result(ctx, f"Document not found: {path}", exit_code=2)

    content = doc_path.read_bytes()
    try:
        idx = build_index(content)
    except LdmdParseError as exc:
        return _text_result(ctx, f"Failed to index document: {exc}", exit_code=2)

    sections_json = [
        {
            "id": s.id,
            "start_offset": s.start_offset,
            "end_offset": s.end_offset,
            "depth": s.depth,
            "collapsed": s.collapsed,
        }
        for s in idx.sections
    ]

    return _text_result(
        ctx,
        json.dumps(
            {
                "sections": sections_json,
                "total_bytes": idx.total_bytes,
            },
            indent=2,
        ),
        media_type="application/json",
    )
```

### GET Command

**Function:** `get()` (`ldmd.py:116-172`)

**Purpose:** Extract section content with mode/depth/limit controls.

**Signature:**

```bash
cq ldmd get <path> --id <section_id> [--mode {full|preview|tldr}] [--depth N] [--limit-bytes N]
```

**CLI parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `path` | str | Required | Path to LDMD document |
| `--id` | str | Required | Section ID to extract |
| `--mode` | str | `"full"` | Extraction mode: `full`, `preview`, `tldr` |
| `--depth` | int | `0` | Include nested sections up to this depth |
| `--limit-bytes` | int | `0` | Max bytes to return (0=unlimited) |

**Mode behaviors:**

- **`full`:** Extract entire section with all nested content (respects `depth`)
- **`preview`:** Extract section with `depth=1` (at most), `limit_bytes=4096` (default)
- **`tldr`:** Extract `{section_id}_tldr` subsection if exists, else fall back to `preview`

**Output:** Extracted markdown section

**Implementation:**

```python
@ldmd_app.command
def get(
    path: Annotated[str, cyclopts.Parameter(help="Path to LDMD document")],
    *,
    section_id: Annotated[str, cyclopts.Parameter(name="--id")],
    mode: str = "full",
    depth: int = 0,
    limit_bytes: int = 0,
    ctx: Annotated[CliContext | None, cyclopts.Parameter(parse=False)] = None,
) -> CliResult:
    """Extract content from a section by ID."""
    if ctx is None:
        msg = "CliContext not injected"
        raise RuntimeError(msg)
    doc_path = Path(path)
    if not doc_path.exists():
        return _text_result(ctx, f"Document not found: {path}", exit_code=2)

    content = doc_path.read_bytes()
    try:
        idx = build_index(content)
        slice_data = get_slice(
            content,
            idx,
            section_id=section_id,
            mode=mode,
            depth=depth,
            limit_bytes=limit_bytes,
        )
    except LdmdParseError as exc:
        return _text_result(ctx, f"Failed to extract section: {exc}", exit_code=2)

    return _text_result(ctx, slice_data.decode("utf-8", errors="replace"))
```

### SEARCH Command

**Function:** `search()` (`ldmd.py:175-215`)

**Purpose:** Find sections containing a query string.

**Signature:**

```bash
cq ldmd search <path> --query <text>
```

**CLI parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `path` | str | Yes | Path to LDMD document |
| `--query` | str | Yes | Search query string |

**Output:** JSON array of matches

```json
[
  {
    "section_id": "key_findings",
    "line": 5,
    "text": "High-impact call site in process_batch()"
  },
  ...
]
```

**Implementation:**

```python
@ldmd_app.command
def search(
    path: Annotated[str, cyclopts.Parameter(help="Path to LDMD document")],
    *,
    query: str,
    ctx: Annotated[CliContext | None, cyclopts.Parameter(parse=False)] = None,
) -> CliResult:
    """Search within LDMD sections."""
    if ctx is None:
        msg = "CliContext not injected"
        raise RuntimeError(msg)
    doc_path = Path(path)
    if not doc_path.exists():
        return _text_result(ctx, f"Document not found: {path}", exit_code=2)

    content = doc_path.read_bytes()
    try:
        idx = build_index(content)
        matches = search_sections(content, idx, query=query)
    except LdmdParseError as exc:
        return _text_result(ctx, f"Search failed: {exc}", exit_code=2)

    return _text_result(ctx, json.dumps(matches, indent=2), media_type="application/json")
```

### NEIGHBORS Command

**Function:** `neighbors()` (`ldmd.py:218-258`)

**Purpose:** Get sequential prev/next section IDs for navigation.

**Signature:**

```bash
cq ldmd neighbors <path> --id <section_id>
```

**CLI parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `path` | str | Yes | Path to LDMD document |
| `--id` | str | Yes | Section ID to get neighbors for |

**Output:** JSON navigation object

```json
{
  "section_id": "key_findings",
  "prev": "summary",
  "next": "section_0"
}
```

**Implementation:**

```python
@ldmd_app.command
def neighbors(
    path: Annotated[str, cyclopts.Parameter(help="Path to LDMD document")],
    *,
    section_id: Annotated[str, cyclopts.Parameter(name="--id")],
    ctx: Annotated[CliContext | None, cyclopts.Parameter(parse=False)] = None,
) -> CliResult:
    """Get neighboring sections for navigation."""
    if ctx is None:
        msg = "CliContext not injected"
        raise RuntimeError(msg)
    doc_path = Path(path)
    if not doc_path.exists():
        return _text_result(ctx, f"Document not found: {path}", exit_code=2)

    content = doc_path.read_bytes()
    try:
        idx = build_index(content)
        nav = get_neighbors(idx, section_id=section_id)
    except LdmdParseError as exc:
        return _text_result(ctx, f"Navigation failed: {exc}", exit_code=2)

    return _text_result(ctx, json.dumps(nav, indent=2), media_type="application/json")
```

---

## 6. CQ Integration

### Output Format Enum

LDMD is registered as an output format in `cli_app/types.py:26`:

```python
class OutputFormat(StrEnum):
    """Output format options.

    Member names are the CLI tokens (e.g., --format md).
    """

    md = "md"
    json = "json"
    both = "both"
    summary = "summary"
    mermaid = "mermaid"
    mermaid_class = "mermaid-class"
    dot = "dot"
    ldmd = "ldmd"  # LLM-friendly progressive disclosure markdown
```

This allows users to invoke any CQ command with `--format ldmd`:

```bash
cq search build_graph --format ldmd
cq q "entity=function name=~^build" --format ldmd
cq calls process_batch --format ldmd
```

### Renderer Dispatch

**Location:** `cli_app/result.py:100-104`

```python
# LDMD format uses lazy import (implemented in R6)
if format_value == "ldmd":
    from tools.cq.ldmd.writer import render_ldmd_from_cq_result

    return render_ldmd_from_cq_result(result)
```

The renderer dispatch occurs in `render_result()`, which is called by `handle_result()` after filters are applied.

**Full dispatch table** (`result.py:91-107`):

```python
renderers: dict[str, Callable[[CqResult], str]] = {
    "json": lambda payload: dumps_json(payload, indent=2),
    "md": render_markdown,
    "summary": render_summary,
    "mermaid": render_mermaid_flowchart,
    "mermaid-class": render_mermaid_class_diagram,
    "dot": render_dot,
}

# LDMD format uses lazy import (implemented in R6)
if format_value == "ldmd":
    from tools.cq.ldmd.writer import render_ldmd_from_cq_result

    return render_ldmd_from_cq_result(result)

renderer = renderers.get(format_value, render_markdown)
return renderer(result)
```

**Lazy import rationale:** LDMD is imported on-demand to avoid circular dependencies and allow R6 (neighborhood subsystem) to evolve independently.

### CliTextResult for Protocol Commands

LDMD protocol commands (`index`, `get`, `search`, `neighbors`) return `CliTextResult` instead of `CqResult`:

**Struct definition** (`context.py:135-147`):

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

**Handling in result pipeline** (`result.py:133-136`):

```python
# For non-CqResult (e.g., admin command with int exit code, or CliTextResult)
if not cli_result.is_cq_result:
    if isinstance(cli_result.result, CliTextResult):
        sys.stdout.write(f"{cli_result.result.text}\n")
    return cli_result.get_exit_code()
```

This allows protocol commands to bypass the standard rendering pipeline (which expects `CqResult`) and output raw text or JSON directly.

### Artifact Storage

When LDMD output is generated via `--format ldmd`, the rendered markdown is saved to the artifact directory (default: `.cq/artifacts/`) unless `--no-save-artifact` is specified.

**Artifact saving** (`result.py:154-156`):

```python
# Save artifact unless disabled
if not no_save:
    artifact = save_artifact_json(result, artifact_dir)
    result.artifacts.append(artifact)
```

The LDMD document itself is persisted as a `.md` artifact, with the artifact reference appended to `result.artifacts`.

---

## 7. Usage Patterns

### Basic LDMD Generation

```bash
# Generate LDMD document for a search
cq search build_graph --format ldmd > output.ldmd.md

# Generate LDMD for call analysis
cq calls process_batch --format ldmd > calls.ldmd.md

# Generate LDMD for query results
cq q "entity=function name=~^build" --format ldmd > query.ldmd.md
```

### Progressive Disclosure Workflow

**1. Index the document:**

```bash
cq ldmd index output.ldmd.md
```

Output:
```json
{
  "sections": [
    {"id": "run_meta", "start_offset": 0, "end_offset": 123, "depth": 0, "collapsed": false},
    {"id": "summary", "start_offset": 124, "end_offset": 456, "depth": 0, "collapsed": false},
    {"id": "key_findings", "start_offset": 457, "end_offset": 1234, "depth": 0, "collapsed": true},
    {"id": "key_findings_tldr", "start_offset": 500, "end_offset": 700, "depth": 1, "collapsed": false},
    ...
  ],
  "total_bytes": 12345
}
```

**2. Retrieve TLDR for quick overview:**

```bash
cq ldmd get output.ldmd.md --id key_findings --mode tldr
```

Output:
```markdown
<!--LDMD:BEGIN id="key_findings_tldr" parent="key_findings" level="2"-->
1. [high] High-impact call site in process_batch() @ src/batch.py:45
2. [high] Dynamic dispatch pattern detected @ src/executor.py:120
3. [med] Potential side effect in __init__ @ src/core.py:34
4. [med] Complex closure with 3 captured variables @ src/compiler.py:89
5. [low] Import-time computation @ src/registry.py:12
<!--LDMD:END id="key_findings_tldr"-->
```

**3. Retrieve full section if needed:**

```bash
cq ldmd get output.ldmd.md --id key_findings --mode full
```

**4. Search for specific content:**

```bash
cq ldmd search output.ldmd.md --query "process_batch"
```

Output:
```json
[
  {"section_id": "key_findings_tldr", "line": 0, "text": "1. [high] High-impact call site in process_batch() @ src/batch.py:45"},
  {"section_id": "section_0", "line": 12, "text": "Call from process_batch() to validate_batch()"}
]
```

**5. Navigate sequentially:**

```bash
cq ldmd neighbors output.ldmd.md --id key_findings
```

Output:
```json
{
  "section_id": "key_findings",
  "prev": "summary",
  "next": "section_0"
}
```

### Agent Consumption Pattern

An LLM agent analyzing a large CQ output can:

1. **Request index** to see available sections
2. **Retrieve TLDRs** for all top-level sections (quick scan)
3. **Fetch full sections** only for relevant findings
4. **Search** for specific symbols or patterns
5. **Navigate** to related sections via neighbors

This reduces context window consumption from potentially tens of thousands of tokens to hundreds.

---

## 8. Architectural Observations

### Design Strengths

**1. Zero-runtime dependencies for parsing**

The parser uses only stdlib (`re`, `dataclasses`). No dependency on tree-sitter, LibCST, or other heavy libraries. This makes LDMD suitable for standalone tools and minimal environments.

**2. Byte-offset indexing for O(1) random access**

Once indexed, section retrieval is constant-time slice operation on the byte array. No need to re-parse the entire document to extract a single section.

**3. UTF-8 safety without performance penalty**

Truncation backs up from the limit only if needed, preserving multi-byte sequences. Most documents won't hit truncation, so the common case is fast.

**4. Invisible markers preserve markdown readability**

LDMD documents render cleanly in markdown viewers (GitHub, VS Code, etc.) because markers are HTML comments. Humans can ignore the protocol and read the document naturally.

**5. Preview/body separation aligns with LLM context economics**

The TLDR/body split is tuned for agent workflows: get a quick overview (TLDR), then drill down (body) only if justified. This matches the way agents should consume large outputs to minimize token waste.

**6. Fail-fast validation with precise error messages**

Stack-based validation catches nesting errors immediately with clear diagnostics ("expected 'X', got 'Y'"). No silent corruption or deferred failures.

### Design Tensions

**1. Marker verbosity vs. compactness**

LDMD markers are readable but verbose. A typical BEGIN marker:

```html
<!--LDMD:BEGIN id="key_findings_tldr" parent="key_findings" level="2"-->
```

This adds ~70 bytes per section. For documents with hundreds of sections, marker overhead can exceed 10KB. Alternative (JSON-based side index) would be more compact but lose self-contained document property.

**2. No structural hierarchy in index**

`LdmdIndex.sections` is a flat list. Parent-child relationships are encoded via `parent` attribute but not materialized into a tree. Navigation is sequential (prev/next), not hierarchical (parent/children). This simplifies parsing but makes tree-based queries (e.g., "all children of X") require post-processing.

**3. Simple text search, no regex or AST awareness**

`search_sections()` uses case-insensitive substring matching. No support for regex, word boundaries, or structural patterns. This is intentional (keep parser simple), but limits search expressiveness.

**4. TLDR lookup requires nested parse**

TLDR mode calls `build_index()` recursively on the extracted slice to find the `_tldr` subsection. For deeply nested sections, this can cascade into multiple parses. Alternative (encode TLDR offsets in top-level index) would eliminate recursion but complicate index structure.

**5. Collapse policy couples parser to neighborhood layout**

The `_is_collapsed()` function imports from `neighborhood.section_layout`, creating a soft dependency. If that module is unavailable, all sections default to collapsed. This lazy import pattern works but obscures the dependency relationship.

**6. No version marker in format**

LDMD documents have no format version indicator. If the marker grammar evolves (e.g., new attributes, changed regex), old parsers will silently fail or produce incorrect results. Adding a version header (`<!--LDMD:VERSION 1.0-->`) would enable forward compatibility.

### Extension Vectors

**1. Hierarchical navigation**

Add `get_children(section_id)` and `get_parent(section_id)` functions to materialize tree structure. Requires building a parent→children map during indexing.

**2. Multi-document linking**

Support cross-document references via `<!--LDMD:REF doc="other.ldmd.md" section="summary"-->` markers. Protocol commands would need to resolve references and fetch from external documents.

**3. Diff-aware updates**

Add `update_section(doc, section_id, new_content)` that replaces a section in-place while preserving byte offsets for unchanged sections. Useful for incremental updates to large documents.

**4. Structured metadata extraction**

Generalize the `tags` attribute to support structured metadata (e.g., `meta='{"confidence": 0.95, "impact": "high"}'`). Protocol commands could filter/sort sections by metadata.

**5. Compression-aware slicing**

For very large documents, compress sections on-disk and decompress on-demand during `get_slice()`. Would require a compressed index format (e.g., storing compressed byte ranges).

**6. Streaming writer API**

Current writers build the entire document in memory (`lines: list[str]`). For multi-GB outputs, a streaming API (`write_section(stream, section_id, content)`) would reduce memory footprint.

### Relationship to Other Subsystems

**Search subsystem (`02_search_subsystem.md`):**

LDMD output for search results groups findings by containing function, with each group as a collapsible section. The TLDR shows top-5 matches per group, the body shows remaining matches. This maps cleanly to LDMD's preview/body separation.

**Query subsystem (`03_query_subsystem.md`):**

Entity and pattern queries can produce hundreds of matches. LDMD format allows agents to retrieve only the entity type sections they care about (e.g., "show me function matches, skip class matches").

**Analysis commands (`04_analysis_commands.md`):**

Call analysis (`cmd_calls`) and impact analysis (`cmd_impact`) generate call graphs and taint paths. LDMD sections can encode graph nodes as separate retrievable units, with TLDR showing high-impact nodes and body showing full graph.

**Multi-step execution (`05_multi_step_execution.md`):**

Run plans with many steps can produce very long outputs. LDMD format allows each step result to be a separate section, with a top-level TLDR summarizing step outcomes and per-step bodies for drill-down.

**Neighborhood subsystem ([08_neighborhood_subsystem.md](08_neighborhood_subsystem.md)):**

Neighborhood outputs are the primary driver for LDMD. SNBs contain dozens of slices (definitions, references, imports, diagnostics, LSP evidence) that are too large to consume at once. LDMD's preview/body split is tuned for this use case.

---

## Summary

LDMD provides a lightweight, self-contained progressive disclosure protocol for long markdown documents. Its strict marker grammar, byte-offset indexing, and preview/body separation enable efficient random access and targeted retrieval—critical for LLM agents working within context window constraints.

Key innovations:

- **Attribute-based markers** are both human-readable (HTML comments) and machine-parseable
- **Stack-validated nesting** catches structural errors immediately with precise diagnostics
- **UTF-8-safe truncation** preserves character boundaries without performance penalty
- **Preview/body separation** aligns with agent context economics (quick scan, then drill down)
- **Zero-runtime dependencies** for parsing (stdlib only)

The subsystem integrates cleanly with CQ's output format system (`--format ldmd`) and provides a protocol command set (`index`, `get`, `search`, `neighbors`) for programmatic access. It serves as the foundation for efficient agent consumption of large analysis artifacts, particularly semantic neighborhood bundles.
