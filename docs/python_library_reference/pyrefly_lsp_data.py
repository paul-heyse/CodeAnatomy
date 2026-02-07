
You *are* already at the “direct” integration point.

Pyrefly exposes two public surfaces: **CLI** (`pyrefly check`, etc.) and a **standards-compliant LSP server** (`pyrefly lsp`). There isn’t a third, simpler “give me everything about the symbol at (line,col)” API — because in LSP land, “everything” is intentionally split into separate feature calls (hover, definition, references, signature help, etc.). Pyrefly’s own docs list these features explicitly as separate LSP capabilities. ([Pyrefly][1])

### Why the “one request for everything” feeling doesn’t exist in LSP

Two reasons:

1. **The protocol doesn’t define a “SymbolInfo super-request.”** It defines individual requests like `textDocument/hover`, `…/definition`, `…/references`, etc. Pyrefly follows that model. ([Pyrefly][2])
2. **You can’t JSON-RPC “batch” them into one message** to make it a single request on the wire — the LSP spec explicitly says JSON-RPC batch messages aren’t supported and clients/servers must not send them. ([Microsoft GitHub][3])

### What *is* efficient (and feels direct): pipelining over one warm server

Even though you can’t batch, you *can* do the next best thing: **send N requests back-to-back without waiting**, then read responses as they come (all over one stdio pipe, no network). That’s exactly how editors stay fast.

Below is a **minimal, synchronous, pipelined** “probe position” that asks for a practical maximal bundle (hover/type, definition, typeDefinition, signatureHelp, references) in one *function call* from cq, with one warmed LSP process.

```python
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any


class PyreflyLsp:
    def __init__(self, root: Path, *, cmd: list[str] | None = None) -> None:
        self.root = root.resolve()
        self.cmd = cmd or ["pyrefly", "lsp"]
        self._id = 0

        self.proc = subprocess.Popen(
            self.cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,  # unbuffered bytes I/O
        )
        assert self.proc.stdin and self.proc.stdout

        self._initialize()

    # ---------- LSP framing ----------
    def _send(self, msg: dict[str, Any]) -> None:
        data = json.dumps(msg, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        header = f"Content-Length: {len(data)}\r\n\r\n".encode("ascii")
        self.proc.stdin.write(header)
        self.proc.stdin.write(data)
        self.proc.stdin.flush()

    def _recv(self) -> dict[str, Any]:
        # Read headers until blank line
        content_length: int | None = None
        while True:
            line = self.proc.stdout.readline()
            if not line:
                raise RuntimeError("pyrefly LSP closed stdout")
            if line in (b"\r\n", b"\n"):
                break
            k, v = line.decode("ascii").split(":", 1)
            if k.strip().lower() == "content-length":
                content_length = int(v.strip())

        if content_length is None:
            raise RuntimeError("Missing Content-Length from LSP server")

        body = self.proc.stdout.read(content_length)
        return json.loads(body.decode("utf-8"))

    def request(self, method: str, params: Any) -> int:
        self._id += 1
        rid = self._id
        self._send({"jsonrpc": "2.0", "id": rid, "method": method, "params": params})
        return rid

    def notify(self, method: str, params: Any) -> None:
        self._send({"jsonrpc": "2.0", "method": method, "params": params})

    # ---------- lifecycle ----------
    def _initialize(self) -> None:
        init_id = self.request(
            "initialize",
            {
                "processId": os.getpid(),
                "rootUri": self.root.as_uri(),
                "capabilities": {},  # keep minimal; Pyrefly advertises what it supports
            },
        )

        # Wait for initialize response; ignore other messages
        while True:
            msg = self._recv()
            if msg.get("id") == init_id:
                break

        self.notify("initialized", {})

    def close(self) -> None:
        try:
            shut_id = self.request("shutdown", None)
            while True:
                msg = self._recv()
                if msg.get("id") == shut_id:
                    break
            self.notify("exit", None)
        finally:
            self.proc.terminate()
            self.proc.wait(timeout=2)

    # ---------- document management ----------
    def did_open(self, path: Path, *, version: int = 1) -> str:
        uri = path.resolve().as_uri()
        text = path.read_text(encoding="utf-8", errors="replace")
        self.notify(
            "textDocument/didOpen",
            {
                "textDocument": {
                    "uri": uri,
                    "languageId": "python",
                    "version": version,
                    "text": text,
                }
            },
        )
        return uri

    # ---------- “maximal practical” probe ----------
    def probe_position(self, uri: str, line0: int, col0: int) -> dict[str, Any]:
        td = {"uri": uri}
        pos = {"line": line0, "character": col0}
        tdp = {"textDocument": td, "position": pos}

        # PIPELINE: fire requests back-to-back (not JSON-RPC batch; this is allowed)
        reqs = {
            "hover": self.request("textDocument/hover", tdp),
            "definition": self.request("textDocument/definition", tdp),
            "typeDefinition": self.request("textDocument/typeDefinition", tdp),
            "signatureHelp": self.request("textDocument/signatureHelp", tdp),
            "references": self.request(
                "textDocument/references",
                {"textDocument": td, "position": pos, "context": {"includeDeclaration": True}},
            ),
        }

        # Collect responses (and ignore notifications) until all are satisfied
        pending = {rid: name for name, rid in reqs.items()}
        out: dict[str, Any] = {}

        while pending:
            msg = self._recv()

            # Notifications like publishDiagnostics have no "id" — ignore or capture separately.
            if "id" not in msg:
                continue

            rid = msg["id"]
            if rid not in pending:
                continue

            name = pending.pop(rid)
            if "error" in msg and msg["error"] is not None:
                out[name] = {"_error": msg["error"]}
            else:
                out[name] = msg.get("result")

        return out


if __name__ == "__main__":
    lsp = PyreflyLsp(root=Path("."))
    try:
        uri = lsp.did_open(Path("example.py"))
        bundle = lsp.probe_position(uri, line0=10, col0=5)
        # You said you don't need a JSON dump; print the parts you care about:
        print("hover:", bundle["hover"])
        print("definition:", bundle["definition"])
        print("typeDefinition:", bundle["typeDefinition"])
        print("signatureHelp:", bundle["signatureHelp"])
        print("references:", None if bundle["references"] is None else len(bundle["references"]))
    finally:
        lsp.close()
```

### If you truly want “one request”

The only real way is **non-standard**: add a **custom LSP method** to Pyrefly (or a tiny wrapper server) like `pyrefly/symbolBundle` that internally performs the multiple standard queries and returns one blob. That’s doable, but it’s a Pyrefly code change (or your own proxy), not something the standard protocol gives you “for free.” ([Pyrefly][2])

If you tell me your target “bundle schema” (what cq actually needs vs “everything”), I’ll trim this to the smallest high-value set and show how to keep a single Pyrefly process warm across multiple cq queries (so you pay startup once).

[1]: https://pyrefly.org/en/docs/?utm_source=chatgpt.com "Introduction"
[2]: https://pyrefly.org/en/docs/IDE-features/ "IDE Supported Features | Pyrefly"
[3]: https://microsoft.github.io/language-server-protocol/specifications/lsp/3.18/specification/?utm_source=chatgpt.com "Language Server Protocol Specification - 3.18"



Below is a **tool-builder deep dive** for **A) Type inference and value model** — organized around the **agent-semantic variables** you listed, with (1) what each variable *means*, (2) where it comes from in Pyrefly/LSP, (3) what it tells an agent about **runtime behavior / contracts**, and (4) how to **structure it into stable outputs** for cq.

---

## A0) One prerequisite that makes all of A work: span correctness

Everything in A is “**type facts at a position**”. If your spans are wrong, your facts attach to the wrong code.

### LSP positions are not “byte offsets”

LSP `Position.character` is defined in terms of a **position encoding** negotiated during initialization (default is UTF-16; newer LSP versions allow UTF-8/UTF-32). The server can pick an encoding and return it in `initialize` result capabilities. ([Microsoft GitHub][1])

**Action for cq:** store every range as:

* `lsp_range`: `{start:{line,character}, end:{line,character}}`
* plus your canonical `byte_span` by converting using the negotiated encoding (if you’re byte-span canonical internally).

---

## A1) Expression type + doc intent (Hover)

### Source surface

Pyrefly supports **Hover** and describes it as: “See type info and docstrings when hovering over code.” ([Pyrefly][2])
LSP specifies the hover response as `Hover | null`, with:

* `contents`: `MarkedString | MarkedString[] | MarkupContent`
* optional `range` (span used to visualize hover target) ([Microsoft GitHub][1])

### Variables

#### `expr.inferred_type`

**Definition:** the *best typechecker model* of “what value(s) can this expression evaluate to at this exact program point”, including any **flow/narrowing** already applied.

**Where it comes from:**
Primarily from `Hover.contents` (Pyrefly shows type info on hover) ([Pyrefly][2])
Secondarily (optional) from inlay hints (A3), which may surface the same type in a more structured place.

**Why it explains code behavior (“so what”):**

* It tells an agent what **operations are valid** on the expression (methods, operators).
* It reveals **implicit contracts** in unannotated code (Pyrefly “infers types in most locations” and uses **flow types** to refine types based on control flow). ([GitHub][3])
  Example consequence: after a narrowing check, hover can show the narrowed type, which is directly “what happens at runtime” if the check passes.

**Practical normalization rule (what you store):**

* Always store:

  * `expr.inferred_type.raw_hover` (full rendered hover contents)
  * `expr.inferred_type.type_string` (a best-effort extracted type line / code block)
  * optionally `expr.inferred_type.type_ast` if you parse the type expression

> Don’t overfit extraction: Hover `contents` can be Markdown, plaintext, or language+code blocks (`MarkedString` is explicitly “markdown or a code-block”). ([Microsoft GitHub][1])

#### `expr.docstring_summary`

**Definition:** the human semantic description of the symbol/expression: docstring, signature excerpt, or summary text.

**Where it comes from:**
Also `Hover.contents` (Pyrefly explicitly includes docstrings in hover). ([Pyrefly][2])

**So what:**

* Gives agents the **author intent** and invariants that aren’t present in types (preconditions, side effects, semantics).
* Combined with `expr.inferred_type`, this becomes “**what it is** + **how you should use it**”.

**Normalization rule:**

* Store:

  * `expr.docstring_summary.markup_kind` (`plaintext|markdown`)
  * `expr.docstring_summary.text` (possibly truncated)
  * `expr.docstring_summary.has_docs` boolean (to avoid misleading empty fields)

#### `expr.target_span`

**Definition:** the span in the document that the hover applies to (what should be highlighted / what your fact attaches to).

**Where it comes from:**
`Hover.range?` (optional). ([Microsoft GitHub][1])

**So what:**

* Without this, agents can’t confidently attach the inferred type to the correct token (especially in attribute chains / complex expressions).

**Normalization rule:**

* If hover includes `range`, store it.
* If hover does not include `range`, fall back to: “the token at the query position” (computed via tree-sitter/python AST), and store that as an inferred `target_span_source="fallback"`.

---

## A2) Call contract / overload selection (Signature Help)

### Source surface

Pyrefly supports **Signature help**. ([Pyrefly][2])
LSP defines the response as `SignatureHelp | null` with:

* `signatures: SignatureInformation[]`
* optional `activeSignature`
* optional `activeParameter` (or the per-signature `SignatureInformation.activeParameter`) ([Microsoft GitHub][1])

Pyrefly has explicitly improved signature help to include function documentation and keep labels single-line in releases. ([GitHub][4])

### Variables

#### `call.signatures[]`

**Definition:** the set of plausible callable contracts at this call site (overloads, union-callables, generic instantiations).

**Where it comes from:**
`SignatureHelp.signatures[]`, each `SignatureInformation` includes:

* `label` (what UI shows)
* optional `documentation`
* optional `parameters[]` with per-parameter `documentation` ([Microsoft GitHub][1])

**So what:**

* This is the agent’s authoritative “**how to call this**” contract *at the exact cursor position*.
* It’s how you prevent “valid Python but wrong meaning” edits: reordering args, swapping positional/keyword usage, changing a call signature, etc.

**Normalization rule (store in an agent-friendly schema):**

* `call.signatures[i].label`
* `call.signatures[i].doc` (if present)
* `call.signatures[i].params[j].label` + `doc`

**Important implementation detail:** `ParameterInformation.label` is a **union**:

* string, or
* `[startOffset, endOffset]` offsets into the signature label (UTF-16-based per spec) ([Microsoft GitHub][1])
  Normalize to both:
* `param.label_text`
* `param.label_span_in_signature` (optional), so you can highlight the param segment inside the signature label.

#### `call.active_signature_index`

**Definition:** which signature Pyrefly thinks is active/right now.

**Where it comes from:**
`SignatureHelp.activeSignature?` (defaults to 0 if omitted/out of range). ([Microsoft GitHub][1])

**So what:**

* This is your “**resolved overload**” — the most actionable piece for edits.
* If you’re generating code changes (adding kwargs, changing arg order), you want to target the active signature first.

#### `call.active_parameter_index`

**Definition:** which parameter is active given the cursor position (or `null` if no parameter matches, e.g. unknown keyword).

**Where it comes from:**
`SignatureHelp.activeParameter?` or `SignatureInformation.activeParameter?`. ([Microsoft GitHub][1])

**So what:**

* Enables “pointed help”: what argument are we currently filling, what type is expected, is the keyword recognized, etc.
* For automated edits: helps you decide where to inject / rename / reorder arguments.

#### `call.resolved_callee_span`

**Definition:** the span of the **callee expression** in the source call (e.g., `foo`, `obj.method`, `pkg.mod.func`), so you can link signature help to grounding (definition/type definition) later.

**Where it comes from:**
Not directly from SignatureHelp. You compute it by finding the **enclosing call expression** at the signatureHelp position (tree-sitter or Python AST) and extracting the callee node span.

**So what:**

* This is the bridge between “contract at callsite” and “symbol identity + navigation” (Section C).
* It’s crucial in Python because same spelling can refer to different callables (shadowing, imports, monkeypatching).

**Recommended algorithm (fast, deterministic):**

1. Use your existing CST/AST (tree-sitter/python or `ast`) to find the smallest `Call` node enclosing `(line,col)`.
2. Extract the callee node span:

   * `Call.func` for Python `ast`
   * `call.function` equivalent in tree-sitter
3. Store as `call.resolved_callee_span` and pass it to grounding (`textDocument/definition` / `typeDefinition`) in Section C.

---

## A3) Inline inferred facts (Inlay Hints)

### Source surface

Pyrefly supports **Inlay hints** and says they provide “Inline hints for types, parameter names, and return values”, and can be inserted as annotations. ([Pyrefly][2])

LSP defines `textDocument/inlayHint` returning `InlayHint[] | null`, each hint has:

* `position`
* `label: string | InlayHintLabelPart[]`
* optional `kind` (`Type=1`, `Parameter=2`)
* optional `textEdits` performed when accepting the hint
* optional `tooltip` ([Microsoft GitHub][1])

Spec explicitly says `textEdits` are the edits performed when accepting the inlay hint, expected to make the hint become part of the document. ([Microsoft GitHub][1])

### Variables

#### `inlay.type_hints[]`

**Definition:** inferred type facts that Pyrefly considers useful enough to inline (variable types, sometimes return types depending on server behavior).

**Where it comes from:**
`InlayHint.kind == Type (1)` plus `label` text. ([Microsoft GitHub][1])

**So what:**

* These are “cheap type visibility” facts: agents can understand the shape of values without reading the whole control flow.
* Strongly complements hover: hover is per-position pull; inlay hints are push hints that summarize the file.

**Normalization rule:**
Store each as:

* `pos` (line/col)
* `label_text` (concatenated label parts)
* `tooltip` (if present)
* `applies_to_span` (computed: the nearest token to the left/right via AST, because inlay hints are anchored by position but not always a range)

#### `inlay.param_name_hints[]`

**Definition:** parameter name hints at call sites (what argument maps to which parameter).

**Where it comes from:**
`InlayHint.kind == Parameter (2)` ([Microsoft GitHub][1])

**So what:**

* This is **behavioral**: it clarifies how positional args bind, which is a common source of silent bugs.
* For edits: it tells an agent when it should rewrite to keywords (stability) or keep positional (readability/perf style).

#### `inlay.return_hints[]`

**Definition:** return type hints for functions/methods where return isn’t annotated but can be inferred.

**Where it comes from:**
Still `InlayHint.kind == Type`, but you categorize “return hint” vs “var hint” using context:

* if the hint position lies inside a `FunctionDef` signature span or matches a “`-> T`” label pattern, classify as return.
* (This classification is **your** semantic layer; LSP only tells you “Type vs Parameter”.) ([Microsoft GitHub][1])

**So what:**

* Return types are the single biggest accelerant for safe refactors: they define what downstream code depends on.
* In Pyrefly, return inference is a first-class design goal (and configuration can disable/alter it). ([GitHub][3])

#### `inlay.materializable_edits[]`

**Definition:** the concrete edits that would insert the inferred hint into code as an annotation (turning “implicit contract” into “explicit contract”).

**Where it comes from:**
`InlayHint.textEdits?` (LSP) + Pyrefly’s “double-click to insert as annotation”. ([Microsoft GitHub][1])

**So what:**

* This is directly actionable for agents: **apply edit → run formatter → run typecheck**.
* It’s a micro-scale alternative to `pyrefly infer` (A5): annotate only the most valuable points.

**Normalization rule:**
Store each edit as:

* `range` + `newText` (LSP `TextEdit` semantics)
* `kind` + `label_text` + computed “what it annotates” span

---

## A4) Type inference policy knobs (interpretation context)

These are not “facts about code”, but they **explain why facts are missing or look permissive**.

#### `type_policy.infer_with_first_use`

**Definition:** whether Pyrefly infers types for unsolved type variables (especially empty containers like `[]`/`{}`) from first usage, vs defaulting to `Any`.

**Pyrefly behavior:**

* Option `infer-with-first-use` controls inference for “type variables not determined by a call or constructor… includes empty containers”. Default true.
* If set false, Pyrefly infers `Any` for unsolved type variables (behaves like Pyright). ([Pyrefly][5])

**So what:**

* If true, your tool can trust that hover/inlay types for empty containers are meaningful and will catch mixed-type uses early.
* If false, you’ll see `list[Any]`/`dict[Any,Any]` more often, and many “contract” facts become weak (agents should be more conservative about refactors).

#### `type_policy.untyped_def_behavior`

**Definition:** how Pyrefly treats functions lacking parameter/return annotations.

**Pyrefly behavior:**

* Default `"check-and-infer-return-type"`: checks bodies and infers return type.
* `"check-and-infer-return-any"`: checks body but treats return type as `Any`.
* `"skip-and-infer-return-any"`: skips checking body, return is `Any`, and *language server functionality like hover and finding definitions won’t be available there*. ([Pyrefly][5])

**So what:**

* This directly affects whether A1/A2/A3 data is present/precise inside untyped code.
* If you build agent workflows on hover/signatureHelp for refactoring inside untyped functions, `"skip-and-infer-return-any"` will make those workflows fail or degrade.

**Tooling recommendation:** always persist these knobs alongside your extracted type facts so agents can interpret “unknown” vs “not computed”.

---

## A5) “Make inferred types explicit” as a patch plan (`pyrefly infer`)

### Source surface

Pyrefly documents `pyrefly infer` as automatically adding type annotations; recommends:

* run on file or directory
* run in **small batches**
* manually review changes
* flags can toggle adding annotations for parameters, return types, containers
* new annotations often expose new type errors ([Pyrefly][6])

### Variables

#### `infer.annotation_scope`

**Definition:** what you ran inference on.

* `scope.kind = file|directory`
* `scope.path = …`
* plus (optional) include/exclude filters if you apply them outside Pyrefly.

**So what:**

* Agents need to understand “why did we annotate this set of files” and keep batch size reviewable (Pyrefly explicitly recommends small batches). ([Pyrefly][6])

#### `infer.annotation_patch`

**Definition:** the concrete patch representing the annotations Pyrefly would apply.

**So what:**

* This is the highest-actionability artifact in A: it’s a ready-to-apply plan that converts implicit contracts into explicit ones, enabling safer refactors and more stable future inference.

**Implementation pattern for cq (robust + deterministic):**

1. Copy the target file/dir to a temp workspace.
2. Run `pyrefly infer …` in the temp workspace.
3. Compute diff (`git diff --no-index` or a Python diff) between original and inferred.
4. Store:

   * unified diff text (human review)
   * plus a structured edit list (file → list of hunks/textEdits)

---

## The “so what” of Section A (what agents *gain*)

If you wire A correctly, your agents get four concrete capabilities:

1. **Type-grounded reasoning at any point** (Hover + Inlay): “what is this value *right here*?” including flow narrowing. ([Pyrefly][2])
2. **Callsite correctness** (SignatureHelp): “which overload/parameter is active, and what does it expect?” ([Microsoft GitHub][1])
3. **Actionable type-hardening edits** (Inlay textEdits + infer patch): “what annotation would make this contract explicit?” ([Microsoft GitHub][1])
4. **Interpretability + failure modes** (policy knobs): “did we *not compute* types here because config told us not to?” ([Pyrefly][5])

If you want, the next step is: I’ll turn A1–A5 into a **single normalized bundle schema** (your style: stable keys + categories + `details.kind/data`) plus a **minimal request plan** (“only call hover/signatureHelp/inlayHint on top-K spans; cache per file hash; include positionEncoding negotiation”) so agents implement it without accidental complexity.

[1]: https://microsoft.github.io/language-server-protocol/specifications/lsp/3.18/specification/ "Specification"
[2]: https://pyrefly.org/en/docs/IDE-features/ "IDE Supported Features | Pyrefly"
[3]: https://github.com/facebook/pyrefly "GitHub - facebook/pyrefly: A fast type checker and language server for Python"
[4]: https://github.com/facebook/pyrefly/releases "Releases · facebook/pyrefly · GitHub"
[5]: https://pyrefly.org/en/docs/configuration/ "Configuration | Pyrefly"
[6]: https://pyrefly.org/en/docs/autotype/ "Infer | Pyrefly"

Below is a **drop-in “Type inference bundle” contract** (A1–A5) in a **stable Finding.category + details.kind + details.data** style, plus a **minimal request plan** that stays fast (top-K, per-file caching, positionEncoding negotiation).

Everything is framed as “what an agent would do with it”, while the underlying LSP semantics are still correct and future-proof.

---

# 1) Normalized bundle schema (PyreflyTypeBundleV1)

### Top-level object

```json
{
  "bundle_kind": "pyrefly.type_bundle.v1",
  "meta": {
    "tool": "pyrefly",
    "server_name": "pyrefly",
    "server_version": "…",
    "lsp_position_encoding": "utf-16|utf-8",
    "workspace_root_uri": "file:///…",
    "config_fingerprint": "sha256:…",
    "created_at": "2026-02-07T…Z"
  },
  "document": {
    "uri": "file:///…/path/to/file.py",
    "path": "path/to/file.py",
    "language_id": "python",
    "version": 1,
    "content_sha256": "…"
  },
  "query": {
    "anchor": { "line": 12, "character": 8 },
    "anchor_kind": "token|call|search_match",
    "window": { "start_line": 10, "end_line": 18 }
  },
  "policy": {
    "infer_with_first_use": true,
    "untyped_def_behavior": "check-and-infer-return-type|check-and-infer-return-any|skip-and-infer-return-any",
    "policy_source": "pyproject.toml|pyrefly.toml|defaults"
  },
  "findings": [
    { "category": "…", "title": "…", "details": { "kind": "…", "data": { } } }
  ]
}
```

### Common substructures you’ll reuse

#### SpanRef

(Your canonical internal can still be bytes; this just ensures you keep LSP ranges too.)

```json
{
  "uri": "file:///…",
  "lsp_range": {
    "start": { "line": 12, "character": 8 },
    "end":   { "line": 12, "character": 20 }
  },
  "byte_span": { "start": 1234, "end": 1250, "encoding": "utf-8" }
}
```

**Note:** LSP character offsets depend on the negotiated `PositionEncodingKind` (`utf-16` default; `utf-8` possible since 3.17). The client announces `general.positionEncodings`, and the server replies with `capabilities.positionEncoding`. If omitted, it defaults to `utf-16`. ([Microsoft GitHub][1])

#### Markup

Normalized hover/docs payload.

```json
{ "kind": "markdown|plaintext", "value": "…" }
```

Hover is defined as `contents: MarkedString | MarkedString[] | MarkupContent` with optional `range`. ([Microsoft GitHub][1])

#### TextEdit (normalized)

```json
{ "range": { "start": {...}, "end": {...} }, "new_text": "…" }
```

(In LSP this is `TextEdit` and is used by inlay-hint materialization.)

---

# 2) Finding registry for A1–A5 (categories + details kinds + data keys)

## A1) Expression type + doc intent (Hover)

### Finding

* **category:** `type.expr`
* **details.kind:** `pyrefly.lsp.hover`

```json
{
  "category": "type.expr",
  "title": "Expression type + doc intent",
  "details": {
    "kind": "pyrefly.lsp.hover",
    "data": {
      "target_span": { "...SpanRef..." },
      "inferred_type": {
        "type_string": "list[int] | None",
        "confidence": "high|medium|low",
        "source": "hover"
      },
      "docstring_summary": {
        "markup": { "kind": "markdown", "value": "…" },
        "present": true
      },
      "raw_hover": {
        "contents": "… (optional raw)",
        "range": "… (optional raw)"
      }
    }
  }
}
```

**How to fill it**

* Call `textDocument/hover` at the anchor position.
* Use `Hover.range` as `target_span.lsp_range` when present. ([Microsoft GitHub][1])
* Parse `Hover.contents` into a normalized `Markup` and extract a best-effort `type_string`.

**Why this is agent-actionable**

* `inferred_type.type_string` is the “what values can flow here?” contract (what operations are valid).
* `docstring_summary` is “what is the author intent?” (preconditions/semantics not expressed in types).
* `target_span` makes the fact attachable to the correct token.

Pyrefly explicitly positions hover as “type info and docstrings”. ([Pyrefly][2])

---

## A2) Call contract / overload selection (Signature help)

### Finding

* **category:** `type.call`
* **details.kind:** `pyrefly.lsp.signature_help`

```json
{
  "category": "type.call",
  "title": "Call contract / overload selection",
  "details": {
    "kind": "pyrefly.lsp.signature_help",
    "data": {
      "call_span": { "...SpanRef..." },
      "resolved_callee_span": { "...SpanRef..." },
      "active_signature_index": 0,
      "active_parameter_index": 2,
      "signatures": [
        {
          "label": "f(x: int, y: str) -> bool",
          "documentation": { "kind": "markdown", "value": "…" },
          "parameters": [
            {
              "label_text": "x: int",
              "label_span_in_signature": [2, 8],
              "documentation": { "kind": "markdown", "value": "…" }
            }
          ],
          "active_parameter_index": 2
        }
      ],
      "raw_signature_help": { "...": "optional raw" }
    }
  }
}
```

**How to fill it**

* Call `textDocument/signatureHelp` at a position inside the call (commonly after `(` or at the current argument).
* `SignatureHelp.activeSignature` defaults to 0 if omitted/out of range. ([Microsoft GitHub][1])
* `SignatureHelp.activeParameter` can be `null` (named arg doesn’t match any declared parameter). ([Microsoft GitHub][1])
* Parameters may provide labels as a string **or** `[start,end]` offsets in the signature label (UTF-16 based). ([Microsoft GitHub][1])

**How to compute `resolved_callee_span`**

* Use your fast local parser (tree-sitter/python or `ast`) to find the enclosing `Call` node at the anchor.
* `resolved_callee_span` = span of the callee expression (`foo`, `obj.method`, etc.).
* `call_span` = span of the full call (or arglist) depending on your internal convention.

**Why this is agent-actionable**

* This is the “don’t silently break call semantics” bundle:

  * which overload was chosen (`active_signature_index`)
  * which parameter is being satisfied (`active_parameter_index`)
  * what each parameter means (`parameters[].documentation`)

Pyrefly supports “signature help” as “live function signatures… parameter hints.” ([Pyrefly][2])

---

## A3) Inline inferred facts (Inlay hints)

### Finding

* **category:** `type.inlay`
* **details.kind:** `pyrefly.lsp.inlay_hints`

```json
{
  "category": "type.inlay",
  "title": "Inline inferred facts (types, param names, returns)",
  "details": {
    "kind": "pyrefly.lsp.inlay_hints",
    "data": {
      "range_queried": { "start_line": 10, "end_line": 18 },
      "type_hints": [
        {
          "position": { "line": 12, "character": 14 },
          "label_text": ": list[int]",
          "tooltip": { "kind": "markdown", "value": "…" },
          "applies_to_span": { "...SpanRef..." },
          "materializable_edits": [
            { "range": { "start": {...}, "end": {...} }, "new_text": ": list[int]" }
          ]
        }
      ],
      "param_name_hints": [
        { "position": { "line": 20, "character": 9 }, "label_text": "timeout:", "applies_to_span": { "...SpanRef..." } }
      ],
      "return_hints": [
        { "position": { "line": 5, "character": 18 }, "label_text": "-> Result", "applies_to_span": { "...SpanRef..." } }
      ],
      "raw_inlay_hints": [ "...optional raw..." ]
    }
  }
}
```

**How to fill it**

* Call `textDocument/inlayHint` with a **small range window** (not whole file). The request explicitly takes a visible `range` and returns `InlayHint[] | null`. ([Microsoft GitHub][1])
* Each `InlayHint` includes:

  * `position`
  * `label` (string or parts)
  * optional `kind`
  * optional `textEdits` that materialize the hint into code ([Microsoft GitHub][1])
* Label parts may include a `location` used for hover/navigation (clickable) — useful if you ever want types to be “jumpable”. ([Microsoft GitHub][1])

**Classification (your semantic layer)**

* LSP only tells you `kind` = Type vs Parameter. ([Microsoft GitHub][1])
* You classify “return hints” vs “var hints” by context (is the hint near a function signature / return annotation zone vs after a variable name).

**Why this is agent-actionable**

* Inlay hints give agents “ambient type facts” without needing to query every token.
* `textEdits` are direct “type-hardening micro-patches” (turn implicit types into explicit annotations). ([Microsoft GitHub][1])
  Pyrefly’s docs also note inlay hints can be inserted as annotations. ([Pyrefly][2])

---

## A4) Type inference policy knobs (interpretation context)

### Finding

* **category:** `type.policy`
* **details.kind:** `pyrefly.config.snapshot`

```json
{
  "category": "type.policy",
  "title": "Type inference policy knobs (interpretation context)",
  "details": {
    "kind": "pyrefly.config.snapshot",
    "data": {
      "infer_with_first_use": true,
      "untyped_def_behavior": "check-and-infer-return-type",
      "policy_source": "pyproject.toml",
      "explainability_effects": {
        "infer_with_first_use": "Empty containers infer from first usage; otherwise Any-like",
        "untyped_def_behavior": "If skip-and-infer-return-any, hover/definition may be unavailable inside untyped bodies"
      }
    }
  }
}
```

**Where it comes from**

* Read from `[tool.pyrefly]` (or `pyrefly.toml`) once per workspace and cache it.
* `infer-with-first-use` is documented as controlling inference for unsolved type variables including empty containers. ([Pyrefly][3])
* `untyped-def-behavior` controls whether untyped bodies are checked and whether language-server functionality like hover/definition is available inside them when skipping. ([Pyrefly][3])

**Why this is agent-actionable**

* These are “why your type facts are strong/weak/missing” explanations that prevent agents from hallucinating certainty.

---

## A5) “Make inferred types explicit” as a patch plan (`pyrefly infer`)

### Finding

* **category:** `type.infer.patch`
* **details.kind:** `pyrefly.cli.infer_patch`

```json
{
  "category": "type.infer.patch",
  "title": "Make inferred types explicit (annotation patch plan)",
  "details": {
    "kind": "pyrefly.cli.infer_patch",
    "data": {
      "annotation_scope": { "kind": "file|directory", "path": "…" },
      "flags": { "params": true, "returns": true, "containers": true },
      "diff_unified": "…",
      "edits_structured": [
        { "uri": "file:///…", "text_edits": [ { "range": {…}, "new_text": "…" } ] }
      ],
      "notes": [
        "Run in small batches; review changes manually",
        "New annotations may expose new type errors"
      ]
    }
  }
}
```

**Where it comes from**

* Run `pyrefly infer path/to/file.py` (or directory). ([Pyrefly][4])
* Pyrefly recommends “small batches” and notes flags can toggle parameters/returns/containers. ([Pyrefly][4])

**Why this is agent-actionable**

* This is a concrete “type-hardening” edit plan that improves future refactors by making contracts explicit.

---

# 3) Minimal request plan (fast defaults, low complexity)

### 3.1 Session + positionEncoding negotiation (must-do)

1. Start one `pyrefly lsp` subprocess per cq invocation (or a warmed pool).
2. `initialize` with `general.positionEncodings = ["utf-8","utf-16"]` (prefer UTF-8 if you want easy byte mapping).
3. Read `initializeResult.capabilities.positionEncoding` and store it in `meta.lsp_position_encoding`.
   LSP 3.17+ defines this negotiation and defaulting behavior. ([Microsoft GitHub][1])

### 3.2 Candidate selection (top-K)

* Input candidates come from cq’s existing fast stages:

  * `/cq search`: the match span’s start position
  * `/cq calls`: call expression spans (callee + arglist)
  * `/cq entity`: identifier spans
* Default: `K = 15` positions total, `K_file = 8` per file.
* Deduplicate by `(uri, line, character)` and also by line (many queries cluster on same line).

### 3.3 Per-file query batching (no extra “query types”)

For each file in candidates:

1. `didOpen` once (cache by content hash).
2. For each selected anchor:

   * **Hover** at anchor → fills A1 (`type.expr`). (Hover returns `Hover | null` and includes `contents` union + optional `range`.) ([Microsoft GitHub][1])
   * If anchor is in a call context (local AST says inside Call):

     * **SignatureHelp** at a stable position (e.g. at `(` or current arg) → fills A2 (`type.call`). (Active indices semantics are defined in the spec.) ([Microsoft GitHub][1])
3. **InlayHints** once per *windowed range*, not per anchor:

   * Request `textDocument/inlayHint` for a small `Range` covering the union of selected lines ±2 lines (or the containing function header + current statement). The request is explicitly range-scoped. ([Microsoft GitHub][1])

### 3.4 Caching strategy (simple, reliable)

Key cache entries by:

* `(workspace_root, config_fingerprint, uri, content_sha256, lsp_position_encoding, method, position_or_range_hash)`

Cache:

* normalized finding payloads (not raw LSP) so downstream stays stable.

### 3.5 Policy snapshot (A4) — once per workspace

* Parse `[tool.pyrefly]` / `pyrefly.toml` once:

  * `infer-with-first-use` ([Pyrefly][3])
  * `untyped-def-behavior` ([Pyrefly][3])
* Attach to every bundle as `policy` and as a `type.policy` finding.

### 3.6 `pyrefly infer` (A5) — opt-in, not default

Because it’s heavier and produces large diffs:

* Only run when the agent is explicitly in “type harden” mode or when requested.
* Run at file granularity first.
* Store diff + structured edits; Pyrefly recommends small batches and manual review. ([Pyrefly][4])

---

If you want the next layer (still within A): I can provide a **single `normalize_type_bundle()` reference implementation** that:

* parses Hover.contents unions,
* normalizes SignatureHelp parameter labels (string vs `[start,end]`),
* normalizes InlayHint labels (string vs label parts),
* and computes `resolved_callee_span` from tree-sitter/python in ~O(1) per anchor.

[1]: https://microsoft.github.io/language-server-protocol/specifications/lsp/3.18/specification/ "Specification"
[2]: https://pyrefly.org/en/docs/IDE-features/ "IDE Supported Features | Pyrefly"
[3]: https://pyrefly.org/en/docs/configuration/ "Configuration | Pyrefly"
[4]: https://pyrefly.org/en/docs/autotype/ "Infer | Pyrefly"

Below is the deep dive for **B) Attribute inference and member surface**, organized exactly around the **agent-semantic variables** you listed, with explicit definitions (what shape it has), where it comes from (LSP/Pyrefly), and the “so what” (how it explains code behavior and supports safe edits).

Pyrefly explicitly supports **Completion**, **Go to definition**, **Go to type definition**, **Go to implementation**, and **Document highlights**. ([Pyrefly][1])

---

## B1) Member discovery — `members.completions[]` and `imports.auto_import_suggestions[]`

### Source surface

* **LSP method:** `textDocument/completion`
* **Pyrefly behavior:** “Autocomplete suggestions for project-specific symbols and imports.” ([Pyrefly][1])
* Pyrefly also advertises that autocomplete can “even import them automatically for you.” ([Pyrefly][2])

### What the completion response looks like (the “shapes” you must handle)

LSP defines the completion response as:
`CompletionItem[] | CompletionList | null`. If it’s a plain `CompletionItem[]`, it’s treated as a complete list (`isIncomplete=false`). ([Microsoft GitHub][3])

#### `CompletionList`

A structured list with:

* `isIncomplete: boolean` (means “further typing should recompute the list”)
* `items: CompletionItem[]`
* optional `itemDefaults` (defaults for item fields like commit characters or edit ranges). ([Microsoft GitHub][3])

**So what for agents:**
`isIncomplete` tells you whether you can treat the member surface as stable at this cursor state. If it’s incomplete, caching should be short-lived and downstream reasoning should be conservative (“likely members” not “definitely present”). ([Microsoft GitHub][3])

---

### `members.completions[]` — what it means

**Definition (agent semantic):**
A list of “next valid tokens/actions” at a position, usually:

* after `obj.` → **attribute/method surface** of the inferred type(s) of `obj`
* in expression position → available names (locals, imports, modules, etc.)

**Wire source:** `CompletionItem` objects inside the completion result.

#### `CompletionItem` — the key fields you should structure

At minimum, a completion item has:

* `label: string` (what is shown, and by default what is inserted) ([Microsoft GitHub][3])
* optional `kind` (Method/Function/Field/Property/Class/Module/etc.) ([Microsoft GitHub][3])
* optional `detail` (“type or symbol information”) ([Microsoft GitHub][3])
* optional `documentation` (string or `MarkupContent`) ([Microsoft GitHub][3])
* optional `labelDetails`:

  * `labelDetails.detail` should be used for function signatures or type annotations
  * `labelDetails.description` for fully qualified name / file path ([Microsoft GitHub][3])

Insertion/edits:

* `insertText?: string` (client may interpret it; spec recommends `textEdit` for precision) ([Microsoft GitHub][3])
* `textEdit?: TextEdit | InsertReplaceEdit` (precise insertion/replacement; must be single-line and include the cursor position) ([Microsoft GitHub][3])
* `insertTextFormat?: PlainText|Snippet` (snippet support exists; note doesn’t apply to `additionalTextEdits`) ([Microsoft GitHub][3])

Post-accept behavior:

* `additionalTextEdits?: TextEdit[]` (extra edits elsewhere; see auto-import below) ([Microsoft GitHub][3])
* `command?: Command` (runs after insertion; spec says extra modifications should be in `additionalTextEdits`) ([Microsoft GitHub][3])
* `data?: LSPAny` (opaque server data preserved for `completionItem/resolve`) ([Microsoft GitHub][3])

Lazy enrichment:
Servers may omit expensive fields (notably `documentation`) in the completion response and provide them later via `completionItem/resolve`. ([Microsoft GitHub][3])

**So what for code behavior / safe edits:**

* `kind` tells the agent *what it is* (property vs method vs module). That changes how you refactor: calling a property is a bug; assigning to a method is a bug; etc. ([Microsoft GitHub][3])
* `detail` / `labelDetails.detail` frequently carries the **type annotation or signature**. That’s immediate “what contract does this member expose?” without extra requests. ([Microsoft GitHub][3])
* `textEdit` and `insertTextFormat` tell the agent exactly what will be written if it “chooses” this member—critical when generating patches. ([Microsoft GitHub][3])
* `documentation` (often via `completionItem/resolve`) provides intent/semantics to avoid “type-correct but wrong meaning” edits. ([Microsoft GitHub][3])

---

### `imports.auto_import_suggestions[]` — what it means

**Definition (agent semantic):**
A subset of completion items that (a) insert an unqualified name and (b) also provide an **import-edit plan** that makes the code compile.

**Wire indicator (the reliable one):** `CompletionItem.additionalTextEdits`.

LSP explicitly defines `additionalTextEdits` as extra edits applied when selecting the completion, and gives the canonical example: *“adding an import statement at the top of the file if the completion item will insert an unqualified type.”* ([Microsoft GitHub][3])
Pyrefly’s blog also explicitly describes autocomplete as able to import automatically. ([Pyrefly][2])

**So what:**

* This is **action insight**: it’s a ready-to-apply, mechanically correct “dependency introduction” plan.
* You can use it to:

  * generate patches that add new symbols safely
  * explain to an agent “this name comes from module X” (often visible via the import edit or labelDetails.description)

**Practical structuring rule:**
Store auto-import suggestions as:

* `completion_ref` (the chosen completion’s label/kind)
* `import_edits[]` (the `additionalTextEdits` whose ranges are in the import region)
* optional `import_module_hint` (if you can infer it from the edit text or `labelDetails.description`)

---

## B2) Attribute/type navigation — definition/type definition/implementation targets

### Source surfaces (Pyrefly + LSP)

Pyrefly supports:

* Go to definition
* Go to type definition
* Go to implementation ([Pyrefly][1])

LSP defines each request as resolving a location for a symbol at a position:

* `textDocument/definition` ([Microsoft GitHub][3])
* `textDocument/typeDefinition` ([Microsoft GitHub][3])
* `textDocument/implementation` ([Microsoft GitHub][3])

Each supports returning `LocationLink[]` when the client indicates `linkSupport` for that request type. ([Microsoft GitHub][3])

### Variables

#### `member.definition_targets[]`

**Definition (agent semantic):**
Where the *behavior* of this member is defined (the code that runs when you call it / access it).

**Wire output:** one or more targets returned by `textDocument/definition` (normalize `Location` and `LocationLink` into a single internal `Target` record).

**So what:**

* This is your strongest “how the code works” anchor: you can jump to the actual implementation (Python source, not just a name).
* For edits:

  * if you’re changing call sites, you can confirm which function/method you’re truly calling (avoid shadowing/import alias mistakes)
  * if you’re modifying behavior, this tells you which file/class to patch

#### `member.type_definition_targets[]`

**Definition (agent semantic):**
Where the *type behind this member/expression* is defined (class/alias/protocol/stub defining the interface).

**Wire output:** targets returned by `textDocument/typeDefinition`. The spec describes it as resolving the type definition location of a symbol at a position. ([Microsoft GitHub][3])

**So what:**

* Explains the *member surface contract*:

  * which attributes should exist
  * which methods/protocols shape the object
* For edits:

  * guides safe refactors (changing a return type, changing an attribute’s type)
  * helps detect “I’m relying on a structural type/protocol” vs a concrete class

#### `member.implementation_targets[]`

**Definition (agent semantic):**
All concrete implementations relevant to a symbol (e.g., method overrides / interface implementers).

**Wire output:** targets returned by `textDocument/implementation`. LSP defines it as resolving the implementation location of a symbol at a position. ([Microsoft GitHub][3])

**So what:**

* This is how you reason about polymorphism and dispatch:

  * if you edit a base method signature, you must assess or update overrides
  * if you’re debugging behavior, this tells you “which concrete method might run”
* For edits:

  * prevents “fixed in base but still broken in override” failures
  * enables “patch all implementers” transformations

**Implementation note:**
All three should normalize to the same internal `Target[]` schema so agents don’t deal with LSP unions; you already did this pattern in Section A.

---

## B3) Read/write usage within a file — `local.occurrences[]`

### Source surface

* **LSP method:** `textDocument/documentHighlight`
* Pyrefly supports Document highlights: “Highlights all other instances of the symbol under your cursor.” ([Pyrefly][1])

LSP describes the request as resolving document highlights for a position, usually references scoped to this file; it’s allowed to be “more fuzzy” than project-wide references. ([Microsoft GitHub][3])
Response: `DocumentHighlight[] | null`, each highlight has:

* `range: Range`
* optional `kind` (default Text) ([Microsoft GitHub][3])
  Kinds:
* Text = 1
* Read = 2
* Write = 3 ([Microsoft GitHub][3])

### `local.occurrences[]`

**Definition (agent semantic):**
All same-symbol occurrences in the current file, optionally classified as read vs write.

**So what:**

* Gives agents a fast “mutation signal”:

  * if there are `Write` occurrences, the symbol is mutated in-file (higher refactor risk)
  * if only `Read`, you’re looking at a stable value source (often easier to rename/inline)
* Helps understand behavior like:

  * “this property is assigned in multiple branches”
  * “this attribute is only ever read (likely derived/immutable)”

**Practical rule for your tool:**
Store each occurrence as:

* `span`
* `access_kind = read|write|text|unknown` (unknown if kind omitted)

---

## “So what” conclusion for Section B (what agents gain)

With B wired in, an LLM agent can answer—fast and reliably:

1. **What members are available here, and what do they represent?**
   (`members.completions[]` with `kind/detail/docs`) ([Microsoft GitHub][3])

2. **Can I introduce this symbol safely (and how)?**
   (`imports.auto_import_suggestions[]` via `additionalTextEdits`) ([Microsoft GitHub][3])

3. **Where does this member’s behavior come from (definition) and what contract governs it (type definition)?**
   (`member.definition_targets[]`, `member.type_definition_targets[]`) ([Microsoft GitHub][3])

4. **If polymorphism is in play, what implementations do I need to consider?**
   (`member.implementation_targets[]`) ([Microsoft GitHub][3])

5. **Is this symbol mutated in this file (read vs write hotspots)?**
   (`local.occurrences[]` with `DocumentHighlightKind`) ([Microsoft GitHub][3])

If you want, I’ll do the follow-on artifact for B (like we did for A): a **normalized “AttributeSurfaceBundleV1” schema + minimal request plan** (top-K completion anchors, `completionItem/resolve` only for top-N items, navigation only for chosen members, and documentHighlight on demand).

[1]: https://pyrefly.org/en/docs/IDE-features/ "IDE Supported Features | Pyrefly"
[2]: https://pyrefly.org/blog/2025/09/15/ide-extension/ "Give your Python IDE a Glow-Up with Pyrefly | Pyrefly"
[3]: https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/ "Specification"

Below is a technical deep dive on **C) Symbol grounding and identity resolution** — organized around your **agent-semantic variables** and framed as “what an LLM agent would *do* with this.”

Everything in C is driven by four LSP requests:

* `textDocument/definition` → *what code is this name actually bound to?* ([Microsoft GitHub][1])
* `textDocument/declaration` → *where is this bound/declared (often at the import edge)?* ([Microsoft GitHub][1])
* `textDocument/typeDefinition` → *what type/interface is governing this expression?* ([Microsoft GitHub][1])
* `textDocument/implementation` → *what overrides/implementations exist?* ([Microsoft GitHub][1])

Pyrefly’s own behavior choices (.py vs .pyi, “stop at imports”, etc.) are documented on its “IDE Supported Features” page. ([Pyrefly][2])

---

## C0) The one thing agents need: a canonical “Target” record

LSP’s raw responses are intentionally flexible: each of the goto-* requests can return:

* `null`
* a single `Location`
* an array of `Location`
* an array of `LocationLink` (if the client declares `linkSupport`) ([Microsoft GitHub][1])

That flexibility is *not* something you want agents to handle. Normalize everything into one stable object:

```python
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Optional

@dataclass(frozen=True)
class Pos:  # LSP
    line: int
    character: int

@dataclass(frozen=True)
class Range:
    start: Pos
    end: Pos

@dataclass(frozen=True)
class Target:
    kind: str                    # "definition" | "declaration" | "type_definition" | "implementation"
    uri: str
    range: Range                 # enclosing span (e.g., function/class block)
    selection_range: Range       # identifier span (best-effort)
    origin_range: Optional[Range] = None   # only if provided (LocationLink)
    source_form: Optional[str] = None      # "py" | "pyi" | "typeshed" | "unknown"
    confidence: str = "high"               # "high|medium|low"
```

**So what:** once you have `Target[]`, every downstream agent action becomes deterministic:

* jump to code to inspect behavior
* build “rename/patch this symbol” edits
* map callsites/references to a stable identity
* reason about import aliasing / shadowing based on where the binding comes from

---

## C1) `symbol.definition_targets[]` + `symbol.preferred_source`

### Source surface

* **Pyrefly:** “Go to definition … If there is both a `.pyi` and `.py` file, we will jump to the `.py`.” ([Pyrefly][2])
* **LSP:** `textDocument/definition` resolves definition location; result is `Location | Location[] | LocationLink[] | null` and `LocationLink[]` depends on `textDocument.definition.linkSupport`. ([Microsoft GitHub][1])

### `symbol.definition_targets[]`

**Definition (agent semantic):**
The set of *canonical runtime behavior sources* that Pyrefly believes this reference resolves to — “this is the code that implements what you’re calling/using.”

**What you persist:** list of normalized `Target(kind="definition", …)`.

**When multiple targets happen (why it matters):**

* union types / overload-like callable unions
* ambiguous resolution (e.g., multiple possible imports on path)
* stubs + runtime sources (server chooses one; Pyrefly prefers `.py` here) ([Pyrefly][2])

**So what (code understanding / code edits):**

* **Disambiguation:** “this `foo()` call is *actually* `pkg.mod.foo` not the local `foo` you saw in the file.”
* **Safe edits:** patch the true definition, not a same-named shadow.
* **Refactor correctness:** renames, signature changes, and doc updates can be targeted to the right file/span.

### `symbol.preferred_source`

**Definition:**
A *policy annotation* describing Pyrefly’s selection preference when both runtime source and stub exist **for goto definition**.

**Pyrefly behavior:** prefers `.py` when both `.pyi` and `.py` exist for **Go to definition**. ([Pyrefly][2])

**So what:**

* Agents should treat **definition** as “behavior truth” (runtime code), not “interface truth.”
* When an agent is trying to understand *what executes*, `.py` is almost always the right anchor.

**Recommended persisted shape:**

```json
{
  "symbol": {
    "preferred_source": {
      "definition": "py",
      "rationale": "runtime behavior over stubs when both exist"
    }
  }
}
```

---

## C2) `symbol.declaration_targets[]` (“stop at imports”)

### Source surface

* **Pyrefly:** “Go to declaration … stop at imports.” ([Pyrefly][2])
* **LSP:** `textDocument/declaration` resolves declaration location; result is `Location | Location[] | LocationLink[] | null` (and `LocationLink[]` depends on `textDocument.declaration.linkSupport`). ([Microsoft GitHub][1])

### `symbol.declaration_targets[]`

**Definition (agent semantic):**
The *binding/edge* that introduces the symbol in the current context — typically the **import statement** or alias site, rather than the ultimate implementation.

**So what (the big one):** this is your **import-edge reasoning primitive**.

* If an agent wants to answer “why does `foo` mean *that* `foo` here?”, declaration targets show the *binding site* (often `from x import foo as bar`).
* For edits: declaration targets are the safest place to:

  * rewrite imports (`import x as y` → `from x import y`)
  * normalize aliasing
  * add/remove re-exports
  * fix cycles by changing binding edges instead of editing implementations

**Recommended additional derived fields (agent-facing):**

* `binding.kind`: `import|from_import|assignment|parameter|global|nonlocal` (you can derive this by inspecting the AST at the target range)
* `binding.alias`: if `as name` exists

That turns “jump target” into “action guidance.”

---

## C3) `symbol.type_definition_targets[]` (“type behind an expression”)

### Source surface

* **Pyrefly:** “Go to type definition … If the expression is a type, we will navigate to the `.pyi` file if both a `.py` and `.pyi` exist.” ([Pyrefly][2])
* **LSP:** `textDocument/typeDefinition` returns `Location | Location[] | LocationLink[] | null` and supports `linkSupport`. ([Microsoft GitHub][1])

### `symbol.type_definition_targets[]`

**Definition (agent semantic):**
The location(s) that define the **interface contract** governing the expression at the cursor — class/protocol/type alias/stub that determines member surface and type relations.

**Why `.pyi` matters here:**
Pyrefly explicitly prefers `.pyi` when the expression itself is a type and both `.py` and `.pyi` exist. ([Pyrefly][2])

**So what (code understanding / edits):**

* **Member surface truth:** If an agent wants to know “what attributes/methods should exist,” type definition is often the most authoritative place (especially with stubs).
* **Safe refactors:** If changing a return type or attribute type, agents should consult type-definition targets to understand downstream expectations.
* **Protocol/ABC reasoning:** type definition targets let agents see whether behavior is governed structurally (protocol) vs nominally (class).

**Practical policy you should persist:**

```json
{
  "symbol": {
    "preferred_source": {
      "type_definition": "pyi_when_available_for_types",
      "rationale": "interface contract/stubs over runtime implementation for types"
    }
  }
}
```

---

## C4) `symbol.implementations[]` (“override set / reimplementations”)

### Source surface

* **Pyrefly:** “Go to implementation … navigate to all reimplementations.” ([Pyrefly][2])
* **LSP:** `textDocument/implementation` returns `Location | Location[] | LocationLink[] | null`. ([Microsoft GitHub][1])

### `symbol.implementations[]`

**Definition (agent semantic):**
All concrete methods/functions that implement the selected symbol’s behavior across inheritance/override relationships (or other “implementation” notions in the language server).

**So what (the polymorphism killer feature):**

* **Behavior understanding:** “Which method actually runs?” depends on runtime type; the implementation set tells an agent where to look.
* **Safe signature edits:** If you change a base method signature, the agent can enumerate overrides that must be updated.
* **Correctness of refactors:** Many “looks safe” changes break in subclasses; `implementations[]` makes those visible up front.

---

## Minimal, agent-friendly computation plan for C (fast + deterministic)

For each anchor span (identifier, attribute, callee):

1. **Definition** (`textDocument/definition`) → `symbol.definition_targets[]`
2. **Declaration** (`textDocument/declaration`) → `symbol.declaration_targets[]`
3. **Type definition** (`textDocument/typeDefinition`) → `symbol.type_definition_targets[]`
4. **Implementation** (`textDocument/implementation`) only if:

   * anchor is method-like (your AST says it’s an attribute call), or
   * `definition_targets` land inside a class body

Then normalize all results into `Target[]` and attach `source_form`:

* if URI endswith `.pyi` → `"pyi"`
* if `.py` → `"py"`
* if path contains `typeshed` (common) → `"typeshed"`
* else `"unknown"`

---

## “So what” conclusion for Section C

With C wired in, an agent can reliably answer:

* **“What does this name actually refer to here?”** → `definition_targets[]` ([Microsoft GitHub][1])
* **“Where is that binding introduced (import/alias edge)?”** → `declaration_targets[]` ([Pyrefly][2])
* **“What interface/contract controls this value?”** → `type_definition_targets[]` ([Pyrefly][2])
* **“What overrides/implementations must I consider?”** → `implementations[]` ([Pyrefly][2])
* **“When stubs exist, should I look at runtime code or interface code?”** → `preferred_source` policies (Pyrefly: definition→`.py`, typeDefinition→`.pyi` for types). ([Pyrefly][2])

If you want, next I can produce the same follow-on artifact as A: a **`SymbolGroundingBundleV1` schema + minimal request plan** (top-K anchors, per-file caching, and a confidence scoring rubric for ambiguous/multi-target results).

[1]: https://microsoft.github.io/language-server-protocol/specifications/lsp/3.18/specification/ "Specification"
[2]: https://pyrefly.org/en/docs/IDE-features/ "IDE Supported Features | Pyrefly"

Below are the **two “follow-on artifacts”** in the same style as A: a **normalized bundle schema** (stable keys + `Finding.category` + `details.kind/data`) plus a **minimal request plan** designed to stay fast for cq (top-K, per-file caching, lazy resolve, on-demand highlights).

---

# 1) AttributeSurfaceBundleV1

## 1.1 Bundle purpose

**What agents do with it:**

* enumerate “what can I access next?” on a receiver (`obj.|`)
* distinguish **Method vs Property vs Field** quickly (safe calls vs safe assignments)
* capture **auto-import edit plans** for new symbols
* ground **existing** member access tokens (`obj.foo`) to definitions/types/implementations
* optionally identify **read vs write** occurrences in the current file (mutation signal)

Pyrefly explicitly supports Completion and Document Highlights and the navigation requests you’ll use here. ([pyrefly.org][1])

---

## 1.2 Normalized schema: `pyrefly.attribute_surface_bundle.v1`

### Top-level

```json
{
  "bundle_kind": "pyrefly.attribute_surface_bundle.v1",
  "meta": {
    "tool": "pyrefly",
    "server_version": "…",
    "lsp_position_encoding": "utf-16|utf-8",
    "workspace_root_uri": "file:///…",
    "created_at": "…"
  },
  "document": {
    "uri": "file:///…/path/to/file.py",
    "path": "path/to/file.py",
    "language_id": "python",
    "version": 1,
    "content_sha256": "…"
  },
  "query": {
    "anchors": [
      { "kind": "completion_anchor", "position": { "line": 12, "character": 9 } },
      { "kind": "member_token", "position": { "line": 40, "character": 17 } }
    ],
    "top_k": { "completion_anchors": 12, "resolve_items_per_anchor": 8 }
  },
  "findings": [
    {
      "category": "member.surface",
      "title": "Completion member surface",
      "details": { "kind": "pyrefly.lsp.completion.snapshot", "data": { } }
    }
  ]
}
```

### Common reusable shapes

**SpanRef**

```json
{
  "uri": "file:///…",
  "lsp_range": { "start": { "line": 1, "character": 0 }, "end": { "line": 1, "character": 10 } },
  "byte_span": { "start": 123, "end": 133, "encoding": "utf-8" }
}
```

**TextEdit**

```json
{ "range": { "start": { "line": 1, "character": 0 }, "end": { "line": 1, "character": 0 } }, "new_text": "import x\n" }
```

**Target**

```json
{
  "uri": "file:///…/defs.py",
  "range": { "start": { "line": 10, "character": 0 }, "end": { "line": 60, "character": 0 } },
  "selection_range": { "start": { "line": 12, "character": 4 }, "end": { "line": 12, "character": 12 } }
}
```

---

## 1.3 Findings + variable definitions (“what it means” + “so what”)

## B1) `members.completions[]` and `imports.auto_import_suggestions[]`

### Finding: `member.surface` / `pyrefly.lsp.completion.snapshot`

```json
{
  "category": "member.surface",
  "title": "Completion member surface",
  "details": {
    "kind": "pyrefly.lsp.completion.snapshot",
    "data": {
      "anchor": { "line": 12, "character": 9 },
      "completion_context": { "trigger_kind": "TriggerCharacter", "trigger_character": "." },
      "is_incomplete": false,
      "members": {
        "completions": [
          {
            "label": "append",
            "kind": "Method",
            "detail": "(self, x: T) -> None",
            "documentation": { "kind": "markdown", "value": "…" },

            "insert": {
              "mode": "textEdit|insertText",
              "text_edit": { "range": { "...": "..." }, "new_text": "append" },
              "insert_text_format": "PlainText|Snippet"
            },

            "auto_import": {
              "is_auto_import": false,
              "additional_text_edits": []
            },

            "resolve": {
              "eligible": true,
              "resolved": false,
              "data_token": { "...": "opaque" }
            }
          }
        ]
      }
    }
  }
}
```

### What the LSP actually returns (the “shapes” you must normalize)

`textDocument/completion` returns one of:

* `CompletionItem[]`
* `CompletionList`
* `null` ([Microsoft GitHub][2])

If you get a plain `CompletionItem[]`, LSP says treat it as complete (`isIncomplete: false`). ([Microsoft GitHub][2])

### `members.completions[]` — definition

**Meaning:** the server’s view of the *available member/name surface* at the anchor position.

**What you should persist per completion item (high-value subset):**

* `label`: display name / default inserted text ([Microsoft GitHub][2])
* `labelDetails` (if present): extra presentation info (e.g. fully qualified path) ([Microsoft GitHub][2])
* `kind`: Method/Property/Field/Class/etc. (drives safe call vs safe assignment) ([Microsoft GitHub][2])
* `detail`: human-readable type/symbol info (often signature/type) ([Microsoft GitHub][2])
* `documentation`: doc comment payload (may be missing until resolve) ([Microsoft GitHub][2])
* `textEdit` vs `insertText`: **prefer `textEdit`**; the spec warns `insertText` can be reinterpreted by clients and recommends `textEdit` for precision ([Microsoft GitHub][2])
* `insertTextFormat`: PlainText vs Snippet; note it does **not** apply to `additionalTextEdits` ([Microsoft GitHub][2])
* `additionalTextEdits`: extra edits for selection, with explicit note that this is where “add an import statement” belongs ([Microsoft GitHub][2])
* `data`: opaque token preserved for `completionItem/resolve` ([Microsoft GitHub][2])

### `imports.auto_import_suggestions[]` — definition

**Meaning:** completion candidates that include an *import edit plan* (or other dependency edits) so that selecting the item leaves the file in a valid state.

**How to detect reliably:**

* if `additionalTextEdits` is non-empty and those edits touch the import region, treat as auto-import.
  LSP explicitly calls out `additionalTextEdits` as the place for unrelated edits like “adding an import statement at the top of the file.” ([Microsoft GitHub][2])

### The “so what”

* Completion gives agents a **type-informed member surface** (“what can I call/access next”) without building your own attribute index. ([pyrefly.org][1])
* `kind` + `detail` is often enough to choose the right edit (“call method”, “don’t assign to method”, “property access ok”). ([Microsoft GitHub][2])
* `additionalTextEdits` is immediately actionable for safe code edits (auto-import injection). ([Microsoft GitHub][2])

---

### Finding: `member.surface.resolve` / `pyrefly.lsp.completion.resolve`

Use this only for top-N items.

```json
{
  "category": "member.surface.resolve",
  "title": "Resolved completion details",
  "details": {
    "kind": "pyrefly.lsp.completion.resolve",
    "data": {
      "anchor": { "line": 12, "character": 9 },
      "resolved_items": [
        {
          "label": "append",
          "resolved_detail": "(self, x: T) -> None",
          "resolved_documentation": { "kind": "markdown", "value": "…" }
        }
      ],
      "resolve_contract": {
        "may_fill": ["detail", "documentation"],
        "must_not_change": ["sortText", "filterText", "insertText", "textEdit"]
      }
    }
  }
}
```

**Why `completionItem/resolve` exists:** it’s explicitly for cases where computing full items (esp. documentation) is expensive; the server can delay that until selection. ([Microsoft GitHub][2])
Also: the spec states that properties needed for sorting/filtering/insertion (e.g. `sortText`, `filterText`, `insertText`, `textEdit`) must be provided in the initial completion response and must not change during resolve. ([Microsoft GitHub][2])

**So what:** this is your “cheap first pass, rich only when needed” lever.

---

## B2) `member.definition_targets[]`, `member.type_definition_targets[]`, `member.implementation_targets[]`

These are for **existing member tokens in code** (e.g. cursor on `foo` in `obj.foo`). They are not for “prospective” completion candidates.

### Finding: `member.navigation` / `pyrefly.lsp.member_navigation`

```json
{
  "category": "member.navigation",
  "title": "Member navigation targets (existing token)",
  "details": {
    "kind": "pyrefly.lsp.member_navigation",
    "data": {
      "member_span": { "...SpanRef..." },
      "definition_targets": [ { "...Target..." } ],
      "type_definition_targets": [ { "...Target..." } ],
      "implementation_targets": [ { "...Target..." } ]
    }
  }
}
```

**What these mean / so what:**

* `definition_targets`: “where the behavior lives” (runtime implementation). Pyrefly go-to-definition prefers `.py` over `.pyi` when both exist. ([pyrefly.org][1])
* `type_definition_targets`: “where the interface/contract lives” (often `.pyi` for types per Pyrefly). ([pyrefly.org][1])
* `implementation_targets`: “which overrides/reimplementations exist” (polymorphism footprint). ([pyrefly.org][1])

LSP specifies each of these goto-family requests returns `Location | Location[] | LocationLink[] | null`. ([Microsoft GitHub][2])

---

## B3) `local.occurrences[]` (Document highlights; mutation signal)

### Finding: `local.occurrences` / `pyrefly.lsp.document_highlight`

```json
{
  "category": "local.occurrences",
  "title": "Local read/write occurrences",
  "details": {
    "kind": "pyrefly.lsp.document_highlight",
    "data": {
      "symbol_span": { "...SpanRef..." },
      "occurrences": [
        { "span": { "...SpanRef..." }, "kind": "Read|Write|Text" }
      ],
      "notes": [
        "Read/Write usually indicates symbol matches; Text can indicate fuzzy/textual matches"
      ]
    }
  }
}
```

LSP explicitly separates `textDocument/documentHighlight` from `textDocument/references` because highlights are allowed to be more fuzzy; it also states symbol matches usually use `Read`/`Write` and fuzzy/text matches use `Text`. ([Microsoft GitHub][2])
`DocumentHighlightKind` enumerates `Text=1`, `Read=2`, `Write=3`. ([Microsoft GitHub][2])

**So what:** this is your low-cost “is it mutated here?” detector: if any `Write` exists, treat refactors (inline/rename/reorder) as higher risk.

---

## 1.4 Minimal request plan (fast defaults)

### Step 0: capabilities you should declare

* Enable `completion.contextSupport` if you want to send `CompletionParams.context`. LSP notes the context is only available if the client capability says so. ([Microsoft GitHub][2])
* Enable `linkSupport` for goto requests so you can receive `LocationLink[]` (gives `selectionRange` reliably). ([Microsoft GitHub][2])

### Step 1: choose top-K completion anchors

Default strategy (fast + deterministic):

* From your AST/tree-sitter scan in a window around cq matches, collect positions of:

  * `Attribute` access sites: `obj.|` or `obj.f|oo` (completion anchors)
  * receiver chains used in calls: `obj.method(|`
* Dedup by `(uri, line, character)`; cap to `K=12` per cq invocation.

### Step 2: call completion

For each completion anchor:

* `textDocument/completion` with `CompletionParams.context` set to `{triggerKind: TriggerCharacter, triggerCharacter: "."}` when applicable. ([Microsoft GitHub][2])
* Normalize the union response:

  * `CompletionList.items` + `CompletionList.isIncomplete` ([Microsoft GitHub][2])
  * or direct `CompletionItem[]` treated as complete ([Microsoft GitHub][2])

### Step 3: resolve only top-N items

Pick `N=8` items per anchor for `completionItem/resolve`, prioritized by:

* `kind in {Method, Function, Property, Field}` (actionable members) ([Microsoft GitHub][2])
* missing `documentation` or missing `detail`
  Then call `completionItem/resolve` for those items. ([Microsoft GitHub][2])

### Step 4: extract auto-import suggestions

Mark a completion item as an auto-import suggestion when:

* it has `additionalTextEdits` and those edits touch the import region.
  The spec explicitly frames `additionalTextEdits` as the mechanism for “adding an import statement at the top of the file.” ([Microsoft GitHub][2])

### Step 5: navigation only for chosen members (and existing tokens)

* For existing member tokens (`obj.foo` occurrences): run B2 navigation immediately (definition/typeDefinition/implementation). ([pyrefly.org][1])
* For completion candidates: don’t precompute goto-*; rely on resolve (docs/detail) until the agent actually inserts/chooses the member.

### Step 6: documentHighlight on demand

Only call `textDocument/documentHighlight` when the agent explicitly needs “is this mutated locally?” It’s allowed to be fuzzy, but Read/Write kinds are meaningful when present. ([Microsoft GitHub][2])

### Caching (simple key)

Cache per `(workspace_root, uri, content_sha256, method, position_or_range, completion_context)`; treat `CompletionList.isIncomplete=true` as short-lived cache (or no cache). ([Microsoft GitHub][2])

---

# 2) SymbolGroundingBundleV1

## 2.1 Bundle purpose

**What agents do with it:**

* eliminate ambiguity from **shadowing** and **import aliasing** (“which `foo` is this?”)
* create reliable hyperlinks for edits (rename/move/signature changes)
* separate “behavior source” (definition) vs “binding edge” (declaration) vs “interface contract” (type definition)
* understand polymorphic dispatch via “implementation set”

Pyrefly’s own preferences (.py vs .pyi, stop at imports, reimplementations) are explicitly documented. ([pyrefly.org][1])

---

## 2.2 Normalized schema: `pyrefly.symbol_grounding_bundle.v1`

```json
{
  "bundle_kind": "pyrefly.symbol_grounding_bundle.v1",
  "meta": {
    "tool": "pyrefly",
    "server_version": "…",
    "lsp_position_encoding": "utf-16|utf-8",
    "workspace_root_uri": "file:///…",
    "created_at": "…"
  },
  "document": { "uri": "file:///…/file.py", "content_sha256": "…" },
  "query": {
    "anchors": [
      { "kind": "identifier", "position": { "line": 12, "character": 8 } },
      { "kind": "attribute_name", "position": { "line": 40, "character": 18 } }
    ],
    "top_k": { "anchors": 20 }
  },
  "policy": {
    "preferred_source": {
      "definition": "py_over_pyi",
      "type_definition": "pyi_for_types_when_available",
      "declaration": "stop_at_import_edges"
    }
  },
  "findings": [
    {
      "category": "symbol.grounding",
      "title": "Symbol grounding (definition/declaration/type/implementation)",
      "details": {
        "kind": "pyrefly.lsp.symbol_grounding",
        "data": {
          "anchor": { "line": 12, "character": 8 },
          "definition_targets": [ { "...Target..." } ],
          "declaration_targets": [ { "...Target..." } ],
          "type_definition_targets": [ { "...Target..." } ],
          "implementations": [ { "...Target..." } ],
          "confidence": {
            "identity_score": 0.92,
            "identity_level": "high",
            "behavior_anchor_score": 0.85,
            "behavior_anchor_level": "high",
            "reasons": [ "single-definition-target", "definition-in-.py", "typeDefinition-in-.pyi" ]
          }
        }
      }
    }
  ]
}
```

### The policy block (what it means / so what)

This is not LSP; it’s **Pyrefly-specific semantics you want agents to internalize**:

* `definition`: if both `.pyi` and `.py` exist, Pyrefly go-to-definition jumps to `.py`. ([pyrefly.org][1])
* `type_definition`: if the expression is a type and both exist, Pyrefly goes to `.pyi`. ([pyrefly.org][1])
* `declaration`: “stop at imports” = binding-edge focus. ([pyrefly.org][1])
* `implementation`: “navigate to all reimplementations” = override set. ([pyrefly.org][1])

---

## 2.3 Minimal request plan (top-K, per-file caching)

### Step 1: choose top-K anchors

Anchor sources (cheap to compute from your existing cq stages):

* identifier spans from `/cq entity`
* attribute name spans from `/cq calls` / tree-sitter attribute nodes
* callee spans from call expressions (`foo(`, `obj.method(`)
* import name spans (important for declaration edges)

Cap: `K=20` anchors per invocation; dedup by `(uri,line,character)`.

### Step 2: per-file session discipline

For each file touched:

* `didOpen` once (version=1; full text)
* then pipeline requests for all anchors in that file.
  (LSP expects language features computed on synchronized state; keep your doc content consistent across requests.) ([Microsoft GitHub][2])

### Step 3: grounding calls per anchor (pipeline)

Always run:

* `textDocument/definition` ([Microsoft GitHub][2])
* `textDocument/declaration` ([Microsoft GitHub][2])
* `textDocument/typeDefinition` ([Microsoft GitHub][2])

Conditionally run `textDocument/implementation` if:

* the anchor is a method-ish site (attribute call), or
* the definition target lands in a class body. ([Microsoft GitHub][2])

**Normalize union results:** each of these returns `Location | Location[] | LocationLink[] | null`. ([Microsoft GitHub][2])
If you enable `linkSupport`, you can receive `LocationLink[]` (more precise selection spans). ([Microsoft GitHub][2])

### Step 4: caching

Cache by `(workspace_root, config_fingerprint, uri, content_sha256, method, position)`.

---

## 2.4 Confidence scoring rubric (ambiguous / multi-target)

LSP does **not** provide a confidence value; you’re building a *heuristic* confidence that is useful to agents. What you want is two separate scores:

### A) `identity_score`: “how confident are we that this anchor refers to *these* symbol(s)?”

**Base rules**

* `targets == []` → `0.0` (“unresolved”)
* `len(targets) == 1` → base `0.95`
* `len(targets) > 1` → base `0.70` minus `0.05*(n-1)` (cap at `0.40`)

**Additive boosts**

* `+0.05` if result came as `LocationLink[]` (means you have a precise `targetSelectionRange` vs only a coarse `range`) — only possible when `linkSupport` is enabled. ([Microsoft GitHub][2])
* `+0.05` if `definition_targets` and `declaration_targets` agree on the same `uri` (binding + behavior align)

**Penalties**

* `-0.10` if targets span multiple unrelated modules/roots (e.g., two different site-packages paths)
* `-0.15` if `declaration_targets` exists but `definition_targets` is empty (binding known but behavior unresolved)

### B) `behavior_anchor_score`: “how directly do the returned targets explain runtime behavior?”

This is *method-dependent* (definition vs typeDefinition are different kinds of truth):

For `definition_targets`:

* `.py` → `0.95`
* `.pyi` / typeshed → `0.60` (interface, not behavior)

For `type_definition_targets`:

* `.pyi` / typeshed → `0.95`
* `.py` → `0.75`

**Then** multiply by `identity_score` (because if identity is ambiguous, behavior explanation is also ambiguous).

### Output shape

```json
"confidence": {
  "identity_score": 0.62,
  "identity_level": "medium",
  "behavior_anchor_score": 0.41,
  "behavior_anchor_level": "low",
  "reasons": [
    "multiple-definition-targets (3)",
    "targets-span-multiple-modules",
    "typeDefinition-provides-interface-only"
  ]
}
```

**So what for agents**

* `identity_score` decides whether an automated refactor can proceed without human disambiguation.
* `behavior_anchor_score` tells whether “jumping to the target” will actually show executable behavior or only a contract.

---

If you want, the next “engineering-ready” step is I’ll write the **exact `details.kind` registry** for these two bundles (like you did for cq findings earlier) and a reference `normalize_*()` module that takes raw LSP responses and produces these normalized Finding objects with zero unions leaking through.

[1]: https://pyrefly.org/en/docs/IDE-features/ "IDE Supported Features | Pyrefly"
[2]: https://microsoft.github.io/language-server-protocol/specifications/lsp/3.18/specification/ "Specification"

Below are (1) an **exact `details.kind` registry** for the two bundles, and (2) a **reference `normalize_*` module** that turns raw LSP payloads into normalized `Finding` objects with **no union shapes leaking** (no `Location | Location[] | LocationLink[] | null`, no `CompletionItem[] | CompletionList | null`, etc.).

I’m keying the registry and normalizers directly off the LSP spec definitions (Completion, Resolve, Goto-family, DocumentHighlight, Location/LocationLink) and Pyrefly’s documented behaviors for `.py` vs `.pyi`, “stop at imports”, and “reimplementations”. ([Pyrefly][1])

---

## 1) `details.kind` registry

### Conventions

* `details.kind` namespace is **source-first**:

  * `pyrefly.lsp.*` = direct LSP call outputs (normalized)
  * `pyrefly.derive.*` = derived facts computed from normalized LSP outputs (heuristics, summaries, scores)
* Every `details.data` has **stable keys** and avoids unions:

  * lists are always lists (never `null`)
  * enums are mapped to strings
  * docs are normalized to `{kind,value}` (or `null`)

---

# 1A) AttributeSurfaceBundleV1 registry

### 1) Completion snapshot

**category:** `member.surface`
**details.kind:** `pyrefly.lsp.completion.snapshot`
**source:** `textDocument/completion` (`CompletionItem[] | CompletionList | null`) normalized to `{is_incomplete: bool, items: []}` ([Microsoft GitHub][2])

**details.data keys**

* `anchor`: `{line:int, character:int}`
* `completion_context`: `{trigger_kind:str, trigger_character:str|null}`
* `is_incomplete`: `bool` (from `CompletionList.isIncomplete`; if server returns `CompletionItem[]`, treat as complete) ([Microsoft GitHub][2])
* `items`: `CompletionItemNorm[]`
* `auto_import_suggestions`: `AutoImportSuggestion[]` (subset of items with `additional_text_edits` that look like imports; see note below)
* `notes`: `[str]`

**CompletionItemNorm keys (stable)**

* `label: str`
* `kind: {code:int|null, name:str|null}` (mapped using `CompletionItemKind`) ([Microsoft GitHub][2])
* `detail: str|null`
* `label_details: {detail:str|null, description:str|null}|null`
* `documentation: {kind:"markdown|plaintext", value:str}|null` (may be `null` until resolve) ([Microsoft GitHub][2])
* `insert`:

  * `mode: "textEdit"|"insertReplaceEdit"|"insertText"|"label"`
  * `new_text: str`
  * `text_edit: TextEditNorm|null`
  * `insert_replace: InsertReplaceEditNorm|null`
  * `insert_text_format: "PlainText"|"Snippet"|null`
* `additional_text_edits: TextEditNorm[]` (used for unrelated edits like imports) ([Microsoft GitHub][2])
* `data_token: object|null` (opaque; used to resolve) ([Microsoft GitHub][2])
* `resolve`: `{eligible: bool, resolved: bool}`

**Why this kind exists (“so what”)**

* It’s the **member surface** available *now*, including actionable edit plans for imports via `additionalTextEdits` (the spec explicitly calls out adding an import at the top of the file as the canonical use). ([Microsoft GitHub][2])

---

### 2) Completion resolve (top-N only)

**category:** `member.surface.resolve`
**details.kind:** `pyrefly.lsp.completion.resolve`
**source:** `completionItem/resolve` (returns a `CompletionItem`) ([Microsoft GitHub][2])

**details.data keys**

* `anchor`: `{line:int, character:int}`
* `resolved_items`: `ResolvedCompletionItem[]`
* `resolve_contract`:

  * `expected_lazy_fields`: `["detail","documentation"]` (default)
  * `must_not_change_fields`: `["sortText","filterText","insertText","textEdit"]` ([Microsoft GitHub][2])
* `notes`: `[str]`

**ResolvedCompletionItem keys**

* `label: str`
* `detail: str|null`
* `documentation: Markup|null`
* `label_details: {...}|null`

**Why this kind exists (“so what”)**

* LSP explicitly supports “cheap list first, rich details later”; resolve is the spec’d mechanism when documentation/detail is expensive. ([Microsoft GitHub][2])

---

### 3) Member navigation (existing token only)

**category:** `member.navigation`
**details.kind:** `pyrefly.lsp.member_navigation`
**source:** `textDocument/definition`, `textDocument/typeDefinition`, `textDocument/implementation` normalized to `Target[]` each; each method returns `Location | Location[] | LocationLink[] | null` ([Microsoft GitHub][2])

**details.data keys**

* `member_span`: `SpanRef`
* `definition_targets`: `Target[]`
* `type_definition_targets`: `Target[]`
* `implementation_targets`: `Target[]`
* `notes`: `[str]`

**Why this kind exists (“so what”)**

* It answers “what backs this member?” in three senses: runtime behavior, interface contract, and override set. Pyrefly documents its own `.py` vs `.pyi` preferences and “reimplementations.” ([Pyrefly][1])

---

### 4) Local occurrences (mutation signal)

**category:** `local.occurrences`
**details.kind:** `pyrefly.lsp.document_highlight`
**source:** `textDocument/documentHighlight` (`DocumentHighlight[] | null`) ([Microsoft GitHub][2])

**details.data keys**

* `symbol_span`: `SpanRef`
* `occurrences`: `Occurrence[]`
* `summary`: `{count:int, reads:int, writes:int, text:int}`
* `notes`: `[str]` (include “allowed to be fuzzy” note)

**Occurrence keys**

* `span: SpanRef`
* `kind: "Read"|"Write"|"Text"` (string-mapped)
* `confidence: "high"|"low"` (Read/Write = high; Text = low)

**Why this kind exists (“so what”)**

* LSP explicitly says document highlights are allowed to be fuzzier than references, and that symbol matches usually use Read/Write while fuzzy/textual matches use Text. ([Microsoft GitHub][2])

---

# 1B) SymbolGroundingBundleV1 registry

### 1) Symbol grounding (raw normalized targets)

**category:** `symbol.grounding`
**details.kind:** `pyrefly.lsp.symbol_grounding`
**source:** `textDocument/definition`, `textDocument/declaration`, `textDocument/typeDefinition`, `textDocument/implementation` ([Microsoft GitHub][2])

**details.data keys**

* `anchor`: `{line:int, character:int}`
* `definition_targets`: `Target[]`
* `declaration_targets`: `Target[]`
* `type_definition_targets`: `Target[]`
* `implementation_targets`: `Target[]` (optional; empty list if not computed)
* `preferred_source`:

  * `definition: "py_over_pyi"`
  * `type_definition: "pyi_for_types_when_available"`
  * `declaration: "stop_at_import_edges"`
* `notes`: `[str]`

**Why this kind exists (“so what”)**

* It is the canonical “identity anchor” layer: definition (behavior), declaration (binding edge), type definition (contract), implementation (override set). Pyrefly spells out these behaviors and preferences. ([Pyrefly][1])

---

### 2) Grounding confidence (derived rubric)

**category:** `symbol.grounding.confidence`
**details.kind:** `pyrefly.derive.symbol_grounding.confidence`
**source:** derived from normalized `Target[]` lists above (no LSP call)

**details.data keys**

* `identity_score: float` (0..1)
* `identity_level: "high"|"medium"|"low"`
* `behavior_anchor_score: float` (0..1)
* `behavior_anchor_level: "high"|"medium"|"low"`
* `reasons: str[]`
* `inputs`:

  * counts for each target list
  * `source_forms` summary (`py|pyi|typeshed|other`) derived from URIs

**Why this kind exists (“so what”)**

* LSP gives you locations; it does not give “confidence.” This rubric is how agents decide whether to proceed automatically or ask for disambiguation.

---

## 2) Reference `normalize_*` module

This module is written to be **pure-normalization**: you feed it raw JSON-decoded LSP results, and it returns normalized `Finding` dicts with stable shapes (no unions). It encodes the completion/resolve contract, completion kinds, goto unions, Location/LocationLink structure, and document highlight kinds per spec. ([Microsoft GitHub][2])

````python
# normalize_pyrefly_bundles.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, TypedDict, Literal, cast
import re


# ----------------------------
# Public Finding shape
# ----------------------------

class FindingDetails(TypedDict):
    kind: str
    data: Dict[str, Any]

class Finding(TypedDict):
    category: str
    title: str
    details: FindingDetails


def make_finding(*, category: str, title: str, kind: str, data: Dict[str, Any]) -> Finding:
    return {"category": category, "title": title, "details": {"kind": kind, "data": data}}


# ----------------------------
# LSP core shapes (normalized)
# ----------------------------

class Pos(TypedDict):
    line: int
    character: int

class Range(TypedDict):
    start: Pos
    end: Pos

class Markup(TypedDict):
    kind: Literal["markdown", "plaintext"]
    value: str

class TextEditNorm(TypedDict):
    range: Range
    new_text: str

class InsertReplaceEditNorm(TypedDict):
    new_text: str
    insert: Range
    replace: Range


class SpanRef(TypedDict, total=False):
    uri: str
    lsp_range: Range
    byte_span: Dict[str, Any]  # optional; your pipeline can fill this later


class Target(TypedDict, total=False):
    uri: str
    range: Range
    selection_range: Range
    origin_range: Range
    source_form: str  # "py"|"pyi"|"typeshed"|"other"
    confidence: str   # "high"|"medium"|"low"


# ----------------------------
# Spec-based enums (string mapped)
# CompletionItemKind per LSP 3.18
# ----------------------------

_COMPLETION_ITEM_KIND: Dict[int, str] = {
    1: "Text",
    2: "Method",
    3: "Function",
    4: "Constructor",
    5: "Field",
    6: "Variable",
    7: "Class",
    8: "Interface",
    9: "Module",
    10: "Property",
    11: "Unit",
    12: "Value",
    13: "Enum",
    14: "Keyword",
    15: "Snippet",
    16: "Color",
    17: "File",
    18: "Reference",
    19: "Folder",
    20: "EnumMember",
    21: "Constant",
    22: "Struct",
    23: "Event",
    24: "Operator",
    25: "TypeParameter",
}

_INSERT_TEXT_FORMAT: Dict[int, str] = {
    1: "PlainText",
    2: "Snippet",
}

_DOC_HL_KIND: Dict[int, str] = {
    1: "Text",
    2: "Read",
    3: "Write",
}


# ----------------------------
# Small, safe parsing helpers
# ----------------------------

def _is_dict(x: Any) -> bool:
    return isinstance(x, dict)

def _is_list(x: Any) -> bool:
    return isinstance(x, list)

def _get_int(d: Dict[str, Any], k: str) -> Optional[int]:
    v = d.get(k)
    return int(v) if isinstance(v, int) else None

def _get_str(d: Dict[str, Any], k: str) -> Optional[str]:
    v = d.get(k)
    return str(v) if isinstance(v, str) else None


def norm_pos(raw: Any) -> Pos:
    # Expect {"line": int, "character": int}
    if not _is_dict(raw):
        return {"line": 0, "character": 0}
    return {
        "line": int(raw.get("line", 0)),
        "character": int(raw.get("character", 0)),
    }

def norm_range(raw: Any) -> Range:
    if not _is_dict(raw):
        return {"start": {"line": 0, "character": 0}, "end": {"line": 0, "character": 0}}
    return {"start": norm_pos(raw.get("start")), "end": norm_pos(raw.get("end"))}


def norm_markup(raw: Any) -> Optional[Markup]:
    """
    Normalize:
      - MarkupContent: {"kind": "markdown|plaintext", "value": str}
      - string: treated as plaintext
      - MarkedString (legacy): {"language": str, "value": str} -> markdown fenced code block
    """
    if raw is None:
        return None
    if isinstance(raw, str):
        return {"kind": "plaintext", "value": raw}
    if _is_dict(raw):
        if "kind" in raw and "value" in raw and isinstance(raw["kind"], str) and isinstance(raw["value"], str):
            k = raw["kind"]
            if k not in ("markdown", "plaintext"):
                k = "plaintext"
            return {"kind": cast(Literal["markdown","plaintext"], k), "value": raw["value"]}
        if "language" in raw and "value" in raw and isinstance(raw["language"], str) and isinstance(raw["value"], str):
            lang = raw["language"]
            val = raw["value"]
            return {"kind": "markdown", "value": f"```{lang}\n{val}\n```"}
    if _is_list(raw):
        parts = [norm_markup(x) for x in raw]
        parts2 = [p for p in parts if p is not None]
        if not parts2:
            return None
        # Prefer markdown if any part is markdown.
        kind: Literal["markdown","plaintext"] = "markdown" if any(p["kind"] == "markdown" for p in parts2) else "plaintext"
        joined = "\n\n".join(p["value"] for p in parts2)
        return {"kind": kind, "value": joined}
    return None


def _derive_source_form(uri: str) -> str:
    u = uri.lower()
    if u.endswith(".pyi"):
        return "pyi"
    if u.endswith(".py"):
        return "py"
    # very common convention; keep it heuristic:
    if "typeshed" in u:
        return "typeshed"
    return "other"


# ----------------------------
# Location / LocationLink union normalization
# LSP defines Location and LocationLink shapes:
#   Location: {uri, range}
#   LocationLink: {targetUri, targetRange, targetSelectionRange, originSelectionRange?}
# ----------------------------

def normalize_location_union(raw: Any, *, confidence_default: str = "high") -> List[Target]:
    """
    Accepts any of:
      - null
      - Location
      - Location[]
      - LocationLink[]
      - (defensive) single LocationLink dict
    and returns List[Target] with:
      - uri, range, selection_range, origin_range?, source_form, confidence
    """
    if raw is None:
        return []

    items: List[Any]
    if _is_list(raw):
        items = raw
    else:
        items = [raw]

    out: List[Target] = []
    for it in items:
        if not _is_dict(it):
            continue

        # LocationLink
        if "targetUri" in it and "targetRange" in it and "targetSelectionRange" in it:
            uri = str(it.get("targetUri"))
            t: Target = {
                "uri": uri,
                "range": norm_range(it.get("targetRange")),
                "selection_range": norm_range(it.get("targetSelectionRange")),
                "source_form": _derive_source_form(uri),
                "confidence": confidence_default,
            }
            if "originSelectionRange" in it:
                t["origin_range"] = norm_range(it.get("originSelectionRange"))
            out.append(t)
            continue

        # Location
        if "uri" in it and "range" in it:
            uri = str(it.get("uri"))
            r = norm_range(it.get("range"))
            out.append({
                "uri": uri,
                "range": r,
                "selection_range": r,  # best available; Location doesn't split selection vs enclosing
                "source_form": _derive_source_form(uri),
                "confidence": confidence_default,
            })
            continue

    return out


# ----------------------------
# Completion normalization
# textDocument/completion returns: CompletionItem[] | CompletionList | null
# CompletionItem resolve can fill documentation/detail; other fields must not change.
# ----------------------------

class CompletionInsert(TypedDict, total=False):
    mode: Literal["textEdit","insertReplaceEdit","insertText","label"]
    new_text: str
    text_edit: TextEditNorm
    insert_replace: InsertReplaceEditNorm
    insert_text_format: Literal["PlainText","Snippet"]

class CompletionItemNorm(TypedDict, total=False):
    label: str
    kind: Dict[str, Any]  # {"code": int|None, "name": str|None}
    detail: Optional[str]
    label_details: Optional[Dict[str, Optional[str]]]
    documentation: Optional[Markup]
    insert: CompletionInsert
    additional_text_edits: List[TextEditNorm]
    data_token: Any
    resolve: Dict[str, Any]

class CompletionSnapshot(TypedDict):
    is_incomplete: bool
    items: List[CompletionItemNorm]
    auto_import_suggestions: List[Dict[str, Any]]


def _norm_text_edit(raw: Any) -> Optional[TextEditNorm]:
    if not _is_dict(raw):
        return None
    if "range" in raw and "newText" in raw:
        return {"range": norm_range(raw["range"]), "new_text": str(raw["newText"])}
    return None

def _norm_insert_replace_edit(raw: Any) -> Optional[InsertReplaceEditNorm]:
    if not _is_dict(raw):
        return None
    if "newText" in raw and "insert" in raw and "replace" in raw:
        return {
            "new_text": str(raw["newText"]),
            "insert": norm_range(raw["insert"]),
            "replace": norm_range(raw["replace"]),
        }
    return None

_IMPORT_RE = re.compile(r"^\s*(from\s+\S+\s+import\s+|import\s+\S+)", re.M)

def _looks_like_import_edits(edits: List[TextEditNorm]) -> bool:
    joined = "\n".join(e["new_text"] for e in edits)
    return bool(_IMPORT_RE.search(joined))


def normalize_completion_item(raw: Any) -> CompletionItemNorm:
    """
    Normalize a raw CompletionItem dict into a stable, union-free structure.
    """
    if not _is_dict(raw):
        return {
            "label": "",
            "kind": {"code": None, "name": None},
            "detail": None,
            "label_details": None,
            "documentation": None,
            "insert": {"mode": "label", "new_text": ""},
            "additional_text_edits": [],
            "data_token": None,
            "resolve": {"eligible": False, "resolved": False},
        }

    label = str(raw.get("label", ""))
    kind_code = _get_int(raw, "kind")
    kind_name = _COMPLETION_ITEM_KIND.get(kind_code) if kind_code is not None else None

    # docs can be string or MarkupContent; normalize to Markup.
    documentation = norm_markup(raw.get("documentation"))

    # labelDetails can exist; preserve but normalize to nullable strings
    ld = raw.get("labelDetails")
    label_details = None
    if _is_dict(ld):
        label_details = {"detail": _get_str(ld, "detail"), "description": _get_str(ld, "description")}

    detail = _get_str(raw, "detail")

    # additionalTextEdits: list of TextEdit (no unions)
    additional: List[TextEditNorm] = []
    ate = raw.get("additionalTextEdits")
    if _is_list(ate):
        for e in ate:
            ne = _norm_text_edit(e)
            if ne:
                additional.append(ne)

    # insert plan: prefer textEdit when present; otherwise insertText; otherwise label.
    insert: CompletionInsert = {"mode": "label", "new_text": label}
    te = raw.get("textEdit")
    if te is not None:
        te_norm = _norm_text_edit(te)
        ir_norm = _norm_insert_replace_edit(te)
        if te_norm:
            insert = {"mode": "textEdit", "new_text": te_norm["new_text"], "text_edit": te_norm}
        elif ir_norm:
            insert = {"mode": "insertReplaceEdit", "new_text": ir_norm["new_text"], "insert_replace": ir_norm}
    elif isinstance(raw.get("insertText"), str):
        insert = {"mode": "insertText", "new_text": str(raw["insertText"])}

    # insertTextFormat: 1=PlainText 2=Snippet
    itf = _get_int(raw, "insertTextFormat")
    if itf in _INSERT_TEXT_FORMAT:
        insert["insert_text_format"] = cast(Literal["PlainText","Snippet"], _INSERT_TEXT_FORMAT[itf])

    data_token = raw.get("data")

    # resolve eligibility: if server uses resolve, it will typically provide data token,
    # or the item lacks detail/documentation (cheap first pass).
    eligible = bool(data_token is not None) or (detail is None or documentation is None)

    # auto-import detection: best-effort from additionalTextEdits content.
    is_auto_import = bool(additional) and _looks_like_import_edits(additional)

    return {
        "label": label,
        "kind": {"code": kind_code, "name": kind_name},
        "detail": detail,
        "label_details": label_details,
        "documentation": documentation,
        "insert": insert,
        "additional_text_edits": additional,
        "data_token": data_token,
        "resolve": {"eligible": eligible, "resolved": False},
        # keep derived flag inside the item for convenience
        "auto_import": {"is_auto_import": is_auto_import},
    }


def normalize_completion_response(raw: Any) -> CompletionSnapshot:
    """
    Normalize textDocument/completion union:
      CompletionItem[] | CompletionList | null
    into:
      {is_incomplete: bool, items: CompletionItemNorm[], auto_import_suggestions: []}
    """
    is_incomplete = False
    raw_items: List[Any] = []

    if raw is None:
        raw_items = []
        is_incomplete = False
    elif _is_list(raw):
        raw_items = raw
        is_incomplete = False
    elif _is_dict(raw) and "items" in raw:
        # CompletionList
        is_incomplete = bool(raw.get("isIncomplete", False))
        items_val = raw.get("items")
        raw_items = items_val if _is_list(items_val) else []
    else:
        # unexpected shape
        raw_items = []
        is_incomplete = False

    items = [normalize_completion_item(it) for it in raw_items]
    auto_import_suggestions = [
        {
            "label": it["label"],
            "kind": it["kind"],
            "additional_text_edits": it.get("additional_text_edits", []),
        }
        for it in items
        if it.get("auto_import", {}).get("is_auto_import") is True
    ]

    return {"is_incomplete": is_incomplete, "items": items, "auto_import_suggestions": auto_import_suggestions}


def merge_completion_resolve(base_item: CompletionItemNorm, resolved_raw: Any) -> CompletionItemNorm:
    """
    Merge completionItem/resolve result into a normalized base item.
    We only accept filling in fields that are typically resolved lazily:
      - detail
      - documentation
      - labelDetails (sometimes)
    Other insertion/sorting fields are intentionally not touched.
    """
    if not _is_dict(resolved_raw):
        return base_item

    merged = dict(base_item)

    if isinstance(resolved_raw.get("detail"), str):
        merged["detail"] = str(resolved_raw["detail"])

    doc = norm_markup(resolved_raw.get("documentation"))
    if doc is not None:
        merged["documentation"] = doc

    ld = resolved_raw.get("labelDetails")
    if _is_dict(ld):
        merged["label_details"] = {"detail": _get_str(ld, "detail"), "description": _get_str(ld, "description")}

    # mark resolved
    r = dict(merged.get("resolve", {}))
    r["resolved"] = True
    merged["resolve"] = r

    return cast(CompletionItemNorm, merged)


# ----------------------------
# Document highlight normalization
# ----------------------------

class Occurrence(TypedDict):
    span: SpanRef
    kind: Literal["Read","Write","Text"]
    confidence: Literal["high","low"]


def normalize_document_highlights(raw: Any, *, uri: str) -> List[Occurrence]:
    """
    Normalize DocumentHighlight[] | null into a stable list.
    DocumentHighlight.kind defaults to Text when omitted.
    """
    if raw is None:
        return []
    if not _is_list(raw):
        return []

    out: List[Occurrence] = []
    for h in raw:
        if not _is_dict(h) or "range" not in h:
            continue
        k = _get_int(h, "kind") or 1
        kind = _DOC_HL_KIND.get(k, "Text")
        conf: Literal["high","low"] = "high" if kind in ("Read","Write") else "low"
        out.append({
            "span": {"uri": uri, "lsp_range": norm_range(h["range"])},
            "kind": cast(Literal["Read","Write","Text"], kind),
            "confidence": conf,
        })
    return out


# ----------------------------
# Finding builders: AttributeSurfaceBundleV1
# ----------------------------

def build_completion_snapshot_finding(
    *,
    anchor: Pos,
    raw_completion: Any,
    trigger_kind: str = "Unknown",
    trigger_character: Optional[str] = None,
) -> Finding:
    snap = normalize_completion_response(raw_completion)
    return make_finding(
        category="member.surface",
        title="Completion member surface",
        kind="pyrefly.lsp.completion.snapshot",
        data={
            "anchor": anchor,
            "completion_context": {"trigger_kind": trigger_kind, "trigger_character": trigger_character},
            "is_incomplete": snap["is_incomplete"],
            "items": snap["items"],
            "auto_import_suggestions": snap["auto_import_suggestions"],
            "notes": [
                "auto_import_suggestions derived from additionalTextEdits (heuristic: inserted text matches import/from)",
            ],
        },
    )


def build_completion_resolve_finding(
    *,
    anchor: Pos,
    base_items: List[CompletionItemNorm],
    resolved_items_raw: List[Any],
) -> Finding:
    # Build lookup by label (simple). If you want stronger pairing, use data_token.
    by_label: Dict[str, CompletionItemNorm] = {it.get("label", ""): it for it in base_items}

    resolved_norm: List[Dict[str, Any]] = []
    merged: List[CompletionItemNorm] = []
    for rr in resolved_items_raw:
        if not _is_dict(rr):
            continue
        lbl = str(rr.get("label", ""))
        base = by_label.get(lbl)
        if base is None:
            continue
        m = merge_completion_resolve(base, rr)
        merged.append(m)
        resolved_norm.append({
            "label": lbl,
            "detail": m.get("detail"),
            "documentation": m.get("documentation"),
            "label_details": m.get("label_details"),
        })

    return make_finding(
        category="member.surface.resolve",
        title="Resolved completion details",
        kind="pyrefly.lsp.completion.resolve",
        data={
            "anchor": anchor,
            "resolved_items": resolved_norm,
            "resolve_contract": {
                "expected_lazy_fields": ["detail", "documentation"],
                "must_not_change_fields": ["sortText", "filterText", "insertText", "textEdit"],
            },
            "notes": [
                "By default, resolve delays detail/documentation; other insertion/sorting fields must not change.",
            ],
        },
    )


def build_member_navigation_finding(
    *,
    member_span: SpanRef,
    raw_definition: Any,
    raw_type_definition: Any,
    raw_implementation: Any,
) -> Finding:
    return make_finding(
        category="member.navigation",
        title="Member navigation targets (existing token)",
        kind="pyrefly.lsp.member_navigation",
        data={
            "member_span": member_span,
            "definition_targets": normalize_location_union(raw_definition),
            "type_definition_targets": normalize_location_union(raw_type_definition),
            "implementation_targets": normalize_location_union(raw_implementation),
            "notes": [],
        },
    )


def build_document_highlight_finding(*, symbol_span: SpanRef, raw_highlights: Any) -> Finding:
    uri = symbol_span.get("uri", "")
    occ = normalize_document_highlights(raw_highlights, uri=uri)
    summary = {
        "count": len(occ),
        "reads": sum(1 for o in occ if o["kind"] == "Read"),
        "writes": sum(1 for o in occ if o["kind"] == "Write"),
        "text": sum(1 for o in occ if o["kind"] == "Text"),
    }
    return make_finding(
        category="local.occurrences",
        title="Local read/write occurrences",
        kind="pyrefly.lsp.document_highlight",
        data={
            "symbol_span": symbol_span,
            "occurrences": occ,
            "summary": summary,
            "notes": [
                "documentHighlight is allowed to be fuzzier than references; Text often indicates fuzzy matches.",
            ],
        },
    )


# ----------------------------
# Finding builders: SymbolGroundingBundleV1
# ----------------------------

def build_symbol_grounding_finding(
    *,
    anchor: Pos,
    raw_definition: Any,
    raw_declaration: Any,
    raw_type_definition: Any,
    raw_implementation: Any = None,
) -> Finding:
    return make_finding(
        category="symbol.grounding",
        title="Symbol grounding (definition/declaration/type/implementation)",
        kind="pyrefly.lsp.symbol_grounding",
        data={
            "anchor": anchor,
            "definition_targets": normalize_location_union(raw_definition),
            "declaration_targets": normalize_location_union(raw_declaration),
            "type_definition_targets": normalize_location_union(raw_type_definition),
            "implementation_targets": normalize_location_union(raw_implementation),
            "preferred_source": {
                "definition": "py_over_pyi",
                "type_definition": "pyi_for_types_when_available",
                "declaration": "stop_at_import_edges",
            },
            "notes": [],
        },
    )


def _level(score: float) -> str:
    if score >= 0.85:
        return "high"
    if score >= 0.60:
        return "medium"
    return "low"


def derive_grounding_confidence(
    *,
    definition_targets: List[Target],
    declaration_targets: List[Target],
    type_definition_targets: List[Target],
) -> Dict[str, Any]:
    """
    Heuristic rubric:
      identity_score: how confident the symbol mapping is
      behavior_anchor_score: how directly the definition explains runtime behavior
    """
    reasons: List[str] = []

    # Identity score: mostly target multiplicity + agreement signal.
    def_n = len(definition_targets)
    decl_n = len(declaration_targets)
    type_n = len(type_definition_targets)

    if def_n == 0 and decl_n == 0 and type_n == 0:
        identity = 0.0
        reasons.append("unresolved-all")
    else:
        if def_n == 1:
            identity = 0.95
            reasons.append("single-definition-target")
        elif def_n > 1:
            identity = max(0.40, 0.70 - 0.05 * (def_n - 1))
            reasons.append(f"multiple-definition-targets ({def_n})")
        else:
            identity = 0.60
            reasons.append("no-definition-target")

        # Agreement boost: any same-uri between declaration and definition.
        def_uris = {t["uri"] for t in definition_targets if "uri" in t}
        decl_uris = {t["uri"] for t in declaration_targets if "uri" in t}
        if def_uris and decl_uris and (def_uris & decl_uris):
            identity = min(1.0, identity + 0.05)
            reasons.append("definition-declaration-uri-overlap")

    # Behavior anchor score: definition in .py is strongest.
    beh = 0.0
    if definition_targets:
        # pick best target by source_form
        best = max(definition_targets, key=lambda t: 1 if t.get("source_form") == "py" else 0)
        sf = best.get("source_form", "other")
        if sf == "py":
            beh = 0.95
            reasons.append("definition-in-py")
        elif sf in ("pyi", "typeshed"):
            beh = 0.60
            reasons.append("definition-in-stub")
        else:
            beh = 0.75
            reasons.append("definition-in-other")
    else:
        reasons.append("no-behavior-anchor")

    # Combine with identity so ambiguity lowers effective confidence.
    beh *= identity

    return {
        "identity_score": round(identity, 3),
        "identity_level": _level(identity),
        "behavior_anchor_score": round(beh, 3),
        "behavior_anchor_level": _level(beh),
        "reasons": reasons,
        "inputs": {
            "definition_targets": def_n,
            "declaration_targets": decl_n,
            "type_definition_targets": type_n,
            "source_forms": {
                "definition": [t.get("source_form") for t in definition_targets],
                "declaration": [t.get("source_form") for t in declaration_targets],
                "type_definition": [t.get("source_form") for t in type_definition_targets],
            },
        },
    }


def build_grounding_confidence_finding(*, anchor: Pos, grounding_finding: Finding) -> Finding:
    d = grounding_finding["details"]["data"]
    conf = derive_grounding_confidence(
        definition_targets=cast(List[Target], d.get("definition_targets", [])),
        declaration_targets=cast(List[Target], d.get("declaration_targets", [])),
        type_definition_targets=cast(List[Target], d.get("type_definition_targets", [])),
    )
    return make_finding(
        category="symbol.grounding.confidence",
        title="Grounding confidence (derived)",
        kind="pyrefly.derive.symbol_grounding.confidence",
        data={"anchor": anchor, **conf},
    )
````

### Notes on correctness vs spec (why the module is “safe”)

* **Completion unions:** normalized from `CompletionItem[] | CompletionList | null` into `{is_incomplete, items}` (no unions). ([Microsoft GitHub][2])
* **Resolve contract:** the module only merges `detail` + `documentation` (the spec’s default lazy fields), and explicitly documents fields that must not change. ([Microsoft GitHub][2])
* **Auto-import detection:** uses a conservative heuristic based on `additionalTextEdits` text content; the spec’s canonical example is “add import statement at top of file” via `additionalTextEdits`. ([Microsoft GitHub][2])
* **Goto-family unions:** all normalized through `normalize_location_union()` from `Location | Location[] | LocationLink[] | null` to `Target[]`. ([Microsoft GitHub][2])
* **Document highlight semantics:** normalized kinds and assigns confidence consistent with spec guidance (Read/Write for symbol matches, Text for fuzzy). ([Microsoft GitHub][2])
* **Pyrefly semantics:** your bundle-level “preferred_source” policy strings match Pyrefly’s documented behavior for definition/type definition/declaration/implementation. ([Pyrefly][1])

---

If you want the next incremental artifact (high leverage): I can adapt the above module into a **msgspec Struct** model set (so serialization + validation is frictionless in your codebase) and add a tiny “golden fixtures” harness: feed recorded raw LSP JSON → assert normalized output is stable across Pyrefly upgrades.

[1]: https://pyrefly.org/en/docs/IDE-features/ "IDE Supported Features | Pyrefly"
[2]: https://microsoft.github.io/language-server-protocol/specifications/lsp/3.18/specification/ "Specification"

Below is the deep dive for **D) Usage, dependency footprint, and call graph**, written in the same “agent-semantic variable” style you’ve been using. I’ll be explicit about **what each variable means**, **what shape it has**, **where it comes from**, and **what it teaches an agent about code behavior / safe edits**.

---

## D0) Mental model: three complementary “dependency” lenses

1. **References** = *all usages* of a symbol across the project (not just calls). Pyrefly’s “Find references” is exactly this: “List all usages of a symbol across your codebase.” ([pyrefly.org][1])
2. **Call hierarchy** = *only call relationships* (incoming callers, outgoing callees) and does it via a **two-step** protocol. Pyrefly describes it as incoming/outgoing calls and “a full picture … within a call stack.” ([pyrefly.org][1])
3. **Implementations** = the *override set* (polymorphism): “On a method, navigate to all reimplementations.” ([pyrefly.org][1])

Your agent workflow becomes: **References → broad impact**, **Call hierarchy → behavioral call flow**, **Implementation set → polymorphic expansion**.

---

## D1) Cross-project usage sites

### Source surface

* **Pyrefly:** “Find references … List all usages of a symbol across your codebase.” ([pyrefly.org][1])
* **LSP method:** `textDocument/references`

  * It “resolve[s] project-wide references for the symbol denoted by the given text document position.” ([Microsoft GitHub][2])
  * Params include `ReferenceContext.includeDeclaration: boolean` (whether to include the symbol declaration in results). ([Microsoft GitHub][2])
  * Response is `Location[] | null`. ([Microsoft GitHub][2])

### “Location” and span semantics (what your `usage.reference_sites[]` actually stores)

LSP `Location` is:

```json
{ "uri": "file:///…", "range": { "start": {...}, "end": {...} } }
```

and `Range` is `{start: Position, end: Position}`. ([Microsoft GitHub][2])

### Variables

#### `usage.reference_sites[]`

**Definition (agent semantic):**
A normalized list of all the **places in code that depend on this symbol**.

**Shape:** list of normalized locations:

```json
[
  { "uri": "file:///…/a.py", "range": { "start": {"line":10,"character":2}, "end": {"line":10,"character":5} } },
  …
]
```

(You may wrap each in your `SpanRef` with byte spans, but the core is `uri+range`.) ([Microsoft GitHub][2])

**Key nuance (`includeDeclaration`)**
`includeDeclaration` controls whether the returned list includes the declaration site for the symbol. ([Microsoft GitHub][2])
Agentically: if you set it `true`, your “reference sites” will include the definition/declaration edge(s), which can distort “how many callsites/users exist” unless you subtract those.

**So what (code behavior / safe edits):**

* This is your **impact perimeter**: “what could break if I change this symbol’s interface or semantics.”
* It enables safe refactors like rename, signature change, type change by enumerating all consumers.

---

#### Derived stats: `usage.ref_count`, `usage.unique_files`, `usage.hotspots[]`

##### `usage.ref_count`

**Definition:** `len(usage.reference_sites)` minus optional “declaration sites” if you’re measuring *uses only*.
**So what:** magnitude-of-impact scalar that helps agents decide whether a change is “surgical” or “wide blast radius”.

##### `usage.unique_files`

**Definition:** count of unique `uri` values in `reference_sites`.
**So what:** tells you whether impact is localized or cross-cutting (a change hitting 30 files is qualitatively different from 30 uses in one file).

##### `usage.hotspots[]`

**Definition (agent semantic):** ranked clusters of usage density so an agent knows where to inspect first.

A practical, stable shape:

```json
[
  {
    "uri": "file:///…/hot.py",
    "ref_count": 47,
    "top_lines": [
      {"line": 120, "count": 6},
      {"line": 121, "count": 5}
    ],
    "contexts": ["callsite", "type_annotation", "attribute_access"]
  }
]
```

**How to compute (deterministic):**

* Group `reference_sites` by `uri` → file counts
* Within each file, group by `range.start.line` → line counts
* Optionally classify each occurrence via cheap AST context around the range:

  * if the range is callee token of a `Call` node → `callsite`
  * if inside `AnnAssign`/function signature → `type_annotation`
  * if on right side of `.` → `attribute_access`

**So what:**

* Hotspots are an **inspection priority queue**: “go read these 3 files first to understand how the symbol is used.”
* They support **risk triage**: lots of type-annotation contexts usually means interface changes ripple through typing; lots of callsites means runtime behavior changes ripple.

---

## D2) Call graph edges (incoming/outgoing)

### Source surface

* **Pyrefly:** Call Hierarchy = incoming + outgoing calls, “full picture … within a call stack.” ([pyrefly.org][1])
* **LSP call hierarchy is explicitly two-step:**

  1. prepare a `CallHierarchyItem` for a document position
  2. resolve incoming/outgoing for that item ([Microsoft GitHub][2])

### Core types you’re persisting

#### `CallHierarchyItem`

Returned by `textDocument/prepareCallHierarchy` as `CallHierarchyItem[] | null`. ([Microsoft GitHub][2])

A `CallHierarchyItem` includes (key fields):

* `name: string`
* `kind: SymbolKind` (function/method/class/etc.)
* `detail?: string` (often signature-like detail)
* `uri: DocumentUri`
* `range: Range` (enclosing symbol span)
* `selectionRange: Range` (identifier span)
* `data?: LSPAny` preserved across requests ([Microsoft GitHub][2])

#### Incoming / outgoing call records

* `callHierarchy/incomingCalls` returns `CallHierarchyIncomingCall[] | null` where each entry has:

  * `from: CallHierarchyItem` (the caller)
  * `fromRanges: Range[]` (callsite ranges in the caller) ([Microsoft GitHub][2])
* `callHierarchy/outgoingCalls` returns `CallHierarchyOutgoingCall[] | null` where each entry has:

  * `to: CallHierarchyItem` (the callee)
  * `fromRanges: Range[]` (callsite ranges in the *original caller*) ([Microsoft GitHub][2])

### Variables

#### `callgraph.incoming_edges[]`

**Definition (agent semantic):** all edges **caller → this function/method**, plus exact callsite spans.

**Shape (normalized):**

```json
[
  {
    "caller": { "name":"...", "uri":"file:///…", "range": {...}, "selectionRange": {...}, "detail":"..." },
    "callee": { "name":"TARGET", "uri":"file:///…", "range": {...}, "selectionRange": {...} },
    "callsite_ranges": [ { "start": {...}, "end": {...} } ]
  }
]
```

(The `callee` is the `CallHierarchyItem` you requested incoming calls for; each entry’s `fromRanges` are callsites in the caller.) ([Microsoft GitHub][2])

**So what:**

* This is **“who calls me?”** at the function/method level.
* It’s a high-signal impact view for changing behavior: if you alter semantics or exceptions, these are the upstream stacks to reason about.

---

#### `callgraph.outgoing_edges[]`

**Definition (agent semantic):** all edges **this function/method → callees**, plus callsite spans in this function.

**Shape (normalized):**

```json
[
  {
    "caller": { "name":"SOURCE", "uri":"file:///…", "range": {...} },
    "callee": { "name":"...", "uri":"file:///…", "range": {...}, "detail":"..." },
    "callsite_ranges": [ { "start": {...}, "end": {...} } ]
  }
]
```

(Each outgoing record has `to` and `fromRanges`.) ([Microsoft GitHub][2])

**So what:**

* This is **“what do I call?”** which is your best quick proxy for “what does this function do” without reading the entire body.
* For refactors: if you change this function’s signature or return type, its outgoing edges tell you which dependencies might constrain the change (e.g., downstream expects a specific type).

---

### Why call hierarchy ≠ references (important for agents)

* **References** includes non-call usages (type annotations, attribute accesses, imports, etc.). ([pyrefly.org][1])
* **Call hierarchy** is specifically call relationships and is explicitly structured around callsites (`fromRanges`). ([Microsoft GitHub][2])

Agent guidance: use references for *breadth*, call hierarchy for *behavioral flow*.

---

## D3) Where polymorphism matters

### Source surfaces

* **Pyrefly:** “Go to implementation … On a method, navigate to all reimplementations.” ([pyrefly.org][1])
* **LSP:** `textDocument/implementation` returns `Location | Location[] | LocationLink[] | null` (normalized to targets). ([Microsoft GitHub][2])
* Combine with **Call hierarchy** edges (D2) to understand dispatch. ([Microsoft GitHub][2])

### Variables

#### `polymorphism.override_set[]`

**Definition (agent semantic):** the set of concrete method implementations that could satisfy/override the base method symbol.

**How to compute (practical):**

* Put the cursor on the *base method* name (or a callsite resolved to the base method).
* Call `textDocument/implementation`.
* Normalize each result into a `Target {uri, range, selection_range}` list. ([Microsoft GitHub][2])

**So what:**

* This is the *minimum checklist* for safe interface changes:

  * renaming a method
  * changing parameter list
  * changing return type
  * altering exceptions or invariants

If the override set is non-empty, any change to the base method can break Liskov-style expectations in subclasses.

---

#### `polymorphism.call_edges_through_base[]`

**Definition (agent semantic):** call edges where the static call target is a base method, but runtime dispatch may land on one of several overrides.

**How to derive (the useful algorithm):**

1. Compute `override_set[]` for the base method (above). ([Microsoft GitHub][2])
2. Compute call hierarchy edges to the base method:

   * incoming callers of base method item = “who calls the base symbol” ([Microsoft GitHub][2])
3. For each callsite range, refine possible runtime targets:

   * If you also have type inference (Section A), use hover/inferred receiver type at the callsite to narrow which subclasses are plausible.
   * Intersect narrowed receiver types with `override_set[]`.
4. Emit edges like:

```json
[
  {
    "caller": { ...CallHierarchyItem... },
    "base_callee": { ...CallHierarchyItem... },
    "callsite_ranges": [ ... ],
    "possible_runtime_callees": [ {uri, range, selection_range} ],
    "confidence": "high|medium|low",
    "reasons": ["receiver_type_narrows_to_X", "override_set_size=3"]
  }
]
```

**So what:**

* This is how you convert “static call graph” into “likely runtime call graph” in OO code.
* It’s the difference between:

  * “this function calls `Base.f`”
  * and “at runtime it may call `ChildA.f` or `ChildB.f` depending on receiver type”

That’s directly actionable for edits:

* If you change base semantics, you now know which overrides must be audited for consistency.
* If you change an override, you know which call paths may now behave differently.

---

## “So what” conclusion for D (what agents learn about code behavior)

With D wired in, agents gain a concrete, checkable picture of behavior:

* **What breaks if I change X?** → `usage.reference_sites[]` + derived stats, with hotspots to prioritize review. ([pyrefly.org][1])
* **How does execution flow?** → `callgraph.incoming_edges[]` and `callgraph.outgoing_edges[]` with precise callsite ranges. ([Microsoft GitHub][2])
* **Where does dynamic dispatch complicate things?** → `polymorphism.override_set[]` + `call_edges_through_base[]` to reason about runtime targets. ([pyrefly.org][1])

If you want the next follow-on artifact (like we did for A/B/C): I can produce a **`UsageAndCallGraphBundleV1` schema + minimal request plan** (top-K anchors, depth-limited call hierarchy expansion, caching, and a confidence rubric for “runtime dispatch expansion”).

[1]: https://pyrefly.org/en/docs/IDE-features/ "IDE Supported Features | Pyrefly"
[2]: https://microsoft.github.io/language-server-protocol/specifications/lsp/3.18/specification/ "Specification"

Below is a **UsageAndCallGraphBundleV1** artifact (schema + `details.kind` registry) and a **minimal request plan** that stays fast (top-K anchors, depth-limited call hierarchy expansion, caching), plus a concrete **confidence rubric** for “runtime dispatch expansion”.

This bundle is built on Pyrefly’s supported LSP features: **Find references**, **Call Hierarchy**, and **Go to implementation**. ([Pyrefly][1])

---

# UsageAndCallGraphBundleV1

## 1) What this bundle is for

**Agent intent:** answer (a) “where is it used?” (breadth), (b) “what calls / is called?” (behavioral flow), and (c) “where does polymorphism change the runtime call target?” (dynamic dispatch).

Pyrefly explicitly describes:

* **Find references:** “List all usages of a symbol across your codebase.” ([Pyrefly][1])
* **Call Hierarchy:** incoming + outgoing calls; “full picture … within a call stack.” ([Pyrefly][1])
* **Go to implementation:** “On a method, navigate to all reimplementations.” ([Pyrefly][1])

---

## 2) Normalized schema

### 2.1 Top-level

```json
{
  "bundle_kind": "pyrefly.usage_callgraph_bundle.v1",
  "meta": {
    "tool": "pyrefly",
    "server_version": "...",
    "lsp_position_encoding": "utf-16|utf-8",
    "workspace_root_uri": "file:///...",
    "created_at": "2026-02-07T..."
  },
  "anchor": {
    "text_document": { "uri": "file:///repo/pkg/mod.py" },
    "position": { "line": 120, "character": 15 },
    "anchor_kind": "symbol|callee|method_name|class_name"
  },
  "policy": {
    "references": { "include_declaration": false },
    "call_hierarchy": {
      "direction": "both",
      "max_depth": 2,
      "max_nodes": 80,
      "max_edges": 250
    },
    "dispatch_expansion": {
      "max_overrides": 25,
      "use_receiver_type_narrowing": true
    }
  },
  "findings": [
    { "category": "...", "title": "...", "details": { "kind": "...", "data": {} } }
  ]
}
```

### 2.2 Common normalized primitives

**SpanRef**

```json
{ "uri": "file:///...", "lsp_range": { "start": {...}, "end": {...} }, "byte_span": { "start": 0, "end": 0 } }
```

**Target** (normalized goto targets, used for overrides)

```json
{ "uri": "file:///...", "range": {...}, "selection_range": {...}, "source_form": "py|pyi|typeshed|other" }
```

**CallItem** (normalized CallHierarchyItem)

```json
{
  "name": "foo",
  "kind": "Function|Method|...",
  "detail": "optional",
  "uri": "file:///...",
  "range": { "start": {...}, "end": {...} },
  "selection_range": { "start": {...}, "end": {...} },
  "data_token": { "opaque": "..." }
}
```

Why `selection_range` and `data_token` matter:

* LSP defines `selectionRange` as the range “selected and revealed when this symbol is picked” and requires it be contained in `range`. ([Microsoft GitHub][2])
* LSP defines `data` as “preserved between a call hierarchy prepare and incoming/outgoing calls requests.” ([Microsoft GitHub][2])

---

## 3) `details.kind` registry for this bundle

### D1) References

**category:** `usage.references`
**details.kind:** `pyrefly.lsp.references`

**Source:** `textDocument/references` returns `Location[] | null`. ([Microsoft GitHub][3])
Params include `ReferenceContext.includeDeclaration`. ([Microsoft GitHub][3])

**details.data**

```json
{
  "include_declaration": false,
  "reference_sites": [ { "uri": "file:///...", "range": {...} } ],
  "notes": []
}
```

---

### D1 derived stats

**category:** `usage.stats`
**details.kind:** `pyrefly.derive.usage.stats`

**details.data**

```json
{
  "ref_count": 123,
  "unique_files": 17,
  "hotspots": [
    { "uri": "file:///...", "ref_count": 44, "top_lines": [ { "line": 88, "count": 6 } ] }
  ]
}
```

---

### D2) Call hierarchy root (prepare)

**category:** `callgraph.root`
**details.kind:** `pyrefly.lsp.call_hierarchy.prepare`

**Source:** `textDocument/prepareCallHierarchy` returns `CallHierarchyItem[] | null`. ([Microsoft GitHub][2])

**details.data**

```json
{
  "root_items": [ { "...CallItem..." } ],
  "chosen_root": { "...CallItem..." }
}
```

---

### D2) Incoming calls (one hop)

**category:** `callgraph.edges.incoming`
**details.kind:** `pyrefly.lsp.call_hierarchy.incoming`

**Source:** `callHierarchy/incomingCalls` returns `CallHierarchyIncomingCall[] | null` where each has:

* `from: CallHierarchyItem`
* `fromRanges: Range[]` (callsite ranges relative to caller) ([Microsoft GitHub][2])

**details.data**

```json
{
  "for_item": { "...CallItem..." },
  "incoming_edges": [
    {
      "caller": { "...CallItem..." },
      "callee": { "...CallItem..." },
      "callsite_ranges": [ { "start": {...}, "end": {...} } ]
    }
  ]
}
```

---

### D2) Outgoing calls (one hop)

**category:** `callgraph.edges.outgoing`
**details.kind:** `pyrefly.lsp.call_hierarchy.outgoing`

**Source:** `callHierarchy/outgoingCalls` returns `CallHierarchyOutgoingCall[] | null` where each has:

* `to: CallHierarchyItem`
* `fromRanges: Range[]` (callsite ranges relative to the caller item you queried) ([Microsoft GitHub][2])

**details.data**

```json
{
  "for_item": { "...CallItem..." },
  "outgoing_edges": [
    {
      "caller": { "...CallItem..." },
      "callee": { "...CallItem..." },
      "callsite_ranges": [ { "start": {...}, "end": {...} } ]
    }
  ]
}
```

---

### Depth-limited expansion summary

**category:** `callgraph.expansion`
**details.kind:** `pyrefly.derive.callgraph.expansion`

**details.data**

```json
{
  "max_depth": 2,
  "direction": "both",
  "nodes_visited": 61,
  "edges_collected": 143,
  "frontier_cutoffs": {
    "hit_max_nodes": false,
    "hit_max_edges": false,
    "hit_max_depth": true
  }
}
```

---

### D3) Override set (implementations)

**category:** `polymorphism.override_set`
**details.kind:** `pyrefly.lsp.implementation.targets`

**Source:** Pyrefly “Go to implementation” provides reimplementations. ([Pyrefly][1])
(Under the hood you call `textDocument/implementation` and normalize to `Target[]`.)

**details.data**

```json
{
  "method_item": { "...CallItem..." },
  "override_targets": [ { "...Target..." } ]
}
```

---

### D3) Runtime dispatch expansion

**category:** `polymorphism.dispatch_expansion`
**details.kind:** `pyrefly.derive.dispatch.expanded_edges`

**details.data**

```json
{
  "base_edge": {
    "caller": { "...CallItem..." },
    "base_callee": { "...CallItem..." },
    "callsite_ranges": [ { "...": "..." } ]
  },
  "override_targets": [ { "...Target..." } ],
  "receiver_type_narrowing": {
    "enabled": true,
    "receiver_inferred_types": [ "ChildA|ChildB" ],
    "narrowed_override_targets": [ { "...Target..." } ]
  },
  "confidence": {
    "dispatch_score": 0.72,
    "level": "medium",
    "reasons": [ "override_set_size=3", "receiver_type_union<=3" ]
  }
}
```

---

# 4) Minimal request plan

## 4.1 Anchor selection (Top-K)

Pick anchors from your existing cq stages:

* `entity` results (identifier spans)
* `calls` results (callee span; method name span)
* any “definition targets” from C bundle (to seed call hierarchy from a known callable)

Defaults:

* `K=20` anchors per invocation
* dedupe by `(uri, line, character)`
* prioritize anchors that are **callables** (functions/methods) for call hierarchy, and anything for references.

## 4.2 References (breadth) — one call per anchor

Call `textDocument/references` with:

* `context.includeDeclaration = false` by default (agents typically want “uses”, not the declaration). The flag is explicitly part of `ReferenceContext`. ([Microsoft GitHub][3])
  Response is `Location[] | null`. ([Microsoft GitHub][3])

Normalization:

* always store `reference_sites` as a list (empty if null).

Derived stats:

* `ref_count = len(reference_sites)`
* `unique_files = count(distinct uri)`
* `hotspots`: top-N files by count + top lines by count

## 4.3 Call hierarchy (behavioral flow) — depth-limited BFS

### Step A: prepare root

`textDocument/prepareCallHierarchy` at the anchor position → `CallHierarchyItem[] | null`. ([Microsoft GitHub][2])
Pick `chosen_root`:

* if multiple items, prefer the one whose `selectionRange` contains the anchor token you expect (or just take index 0).

### Step B: expand incoming/outgoing

For each expanded node `item`:

* `callHierarchy/incomingCalls` → each record has `from` + `fromRanges` (callsite ranges). ([Microsoft GitHub][2])
* `callHierarchy/outgoingCalls` → each record has `to` + `fromRanges`. ([Microsoft GitHub][2])

**Important cache key:** `CallHierarchyItem.data` is preserved between prepare and incoming/outgoing calls. Use `data_token` (if present) + `uri+selection_range` as your stable node identity. ([Microsoft GitHub][2])

### Step C: depth limit + explosion controls

* BFS depth `D=2` default (incoming and outgoing)
* global caps: `max_nodes=80`, `max_edges=250`
* always keep a `visited` set of node IDs
* if you hit caps, stop and record cutoffs in `callgraph.expansion`

## 4.4 Caching

Cache per `(workspace_root, config_fingerprint, uri, content_sha, method, params_hash)`:

* `references(anchor)` results are stable for a given snapshot of code.
* `prepareCallHierarchy(anchor)` and subsequent calls are stable per snapshot.
* `incomingCalls(item)` / `outgoingCalls(item)` stable per snapshot.

If you support incremental doc updates later (`didChange`), bump `content_sha`.

---

# 5) Confidence rubric for “runtime dispatch expansion”

This rubric is **not** in LSP; it’s your derived signal to tell agents when “base edge expands to these overrides” is trustworthy.

## 5.1 Inputs

* `override_targets`: from `textDocument/implementation` (Pyrefly: “reimplementations”). ([Pyrefly][1])
* `callsite_ranges`: from call hierarchy (`fromRanges`). ([Microsoft GitHub][2])
* optional: `receiver_inferred_types` from your **TypeBundle** (hover/inlay) at the receiver expression near the callsite (Section A).

## 5.2 Scoring components (0..1)

1. **Override set size factor**

* 0 overrides → 0.0 (no dispatch expansion)
* 1 override → 0.95
* 2–5 → 0.80
* 6–20 → 0.55
* > 20 → 0.25

2. **Receiver narrowing factor** (if enabled)

* exact concrete class → 0.95
* union of ≤3 concretes → 0.75
* protocol/Any/Unknown → 0.35
* not available → 0.50 (neutral)

3. **Callsite determinism factor**

* `super().m(...)` or `Class.m(self, ...)` pattern detected via AST near the callsite → 0.90 (dispatch constrained)
* plain `obj.m(...)` → 0.70

## 5.3 Final dispatch score

`dispatch_score = override_size_factor * receiver_narrowing_factor * callsite_determinism_factor`

Levels:

* `>=0.80` high
* `0.55–0.79` medium
* `<0.55` low

**What the score means (agent “so what”):**

* **High:** safe to say “this call likely dispatches to these overrides”; agents can confidently audit/update those files.
* **Medium:** present expanded targets but advise agents to verify receiver type at hotspots (top callers).
* **Low:** treat as “possible” rather than “likely”; use it as a navigation aid, not a refactor precondition.

---

If you want the next follow-on (same as A/B/C): I can produce an **exact `details.kind` normalization module** for this bundle (like `normalize_pyrefly_bundles.py`), including:

* `normalize_references(Location[]|null)` using the spec’s `Location` shape ([Microsoft GitHub][4])
* `normalize_call_hierarchy_prepare(CallHierarchyItem[]|null)` (range/selectionRange/data) ([Microsoft GitHub][2])
* `normalize_incoming/outgoing(CallHierarchy*Call[]|null)` with `fromRanges` semantics ([Microsoft GitHub][2])
  …and a deterministic “depth-limited BFS” helper that emits the edge lists + expansion summary.

[1]: https://pyrefly.org/en/docs/IDE-features/ "IDE Supported Features | Pyrefly"
[2]: https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/ "Specification"
[3]: https://microsoft.github.io/language-server-protocol/specifications/specification-3-15/ "Specification"
[4]: https://microsoft.github.io/language-server-protocol/specifications/lsp/3.18/specification/?utm_source=chatgpt.com "Language Server Protocol Specification - 3.18"

Below is the deep dive for **E) Structural map and discovery**, organized around your **agent-semantic variables**, with explicit definitions (what the variable *is*), where it comes from (Pyrefly/LSP), and the “so what” (how it improves understanding and enables safer edits).

Pyrefly explicitly supports **Document symbols**, **Workspace symbols**, and **Semantic tokens**. ([Pyrefly][1])

---

## E1) File outline — `outline.document_tree`

### Source surface

* **Pyrefly surface:** “Document symbols — See an outline of all top-level items in the current file.” ([Pyrefly][1])
* **LSP method:** `textDocument/documentSymbol`
* **Response shape (important):**
  `DocumentSymbol[] | SymbolInformation[] | null`. ([Microsoft GitHub][2])
  The protocol is explicit:

  * `DocumentSymbol[]` = **hierarchy** of symbols in the document
  * `SymbolInformation[]` = **flat list**; *you must not infer hierarchy* from `location.range` or `containerName` ([Microsoft GitHub][2])
  * “Servers should whenever possible return `DocumentSymbol` since it is the richer data structure.” ([Microsoft GitHub][2])

### What you persist: `outline.document_tree`

**Definition (agent semantic):**
A hierarchical tree of symbols in the file, with stable spans that allow agents to jump, summarize, refactor, and reason about nesting (module → class → method → inner defs).

**Best practice:** request **hierarchical** symbols by advertising client capability:
`textDocument.documentSymbol.hierarchicalDocumentSymbolSupport=true`. ([Microsoft GitHub][2])

### The core object you want: `DocumentSymbol`

LSP defines `DocumentSymbol` as hierarchical and explicitly states it has **two ranges**: ([Microsoft GitHub][2])

* `range`: encloses the symbol definition (no leading/trailing whitespace, but includes comments). Used to decide whether cursor is “inside” symbol. ([Microsoft GitHub][2])
* `selectionRange`: the “most interesting” range (usually identifier). Must be contained by `range`. ([Microsoft GitHub][2])
* `children?`: nested `DocumentSymbol[]` (e.g., methods inside a class). ([Microsoft GitHub][2])
* plus: `name`, optional `detail` (often signature), `kind`, optional `tags`/`deprecated`. ([Microsoft GitHub][2])

### Why you *don’t* want `SymbolInformation[]` (except as fallback)

LSP defines `SymbolInformation.location.range` as a UI reveal range that can include modifiers and “doesn’t have to denote an AST node range” and **cannot be used to reconstruct hierarchy**. ([Microsoft GitHub][2])
So if Pyrefly ever returns `SymbolInformation[]` (client didn’t request hierarchy), your tool should:

* treat it as a flat list for “jump targets”
* **not** attempt tree reconstruction (it’ll be wrong and brittle). ([Microsoft GitHub][2])

### “So what” for agents (what this teaches about the codebase)

`outline.document_tree` enables agents to:

* **Build a mental map of the file without reading it**: top-level defs, class boundaries, method sets, nesting depth.
* **Scope refactors safely**: “edit only this method body”, “rename within this class”, “extract this inner function”.
* **Render explainable context**: stable spans for “module summary”, “class summary”, “method summary”, stitched together from `range` blocks.

A good agent-facing node format (normalized):

```json
{
  "name": "MyClass",
  "kind": "Class",
  "detail": null,
  "range": {...},
  "selection_range": {...},
  "children": [...]
}
```

---

## E2) Project-wide symbol index — `index.symbol_hits[]`

### Source surface

* **Pyrefly surface:** “Workspace symbols — Search globally for functions, classes, and variables.” ([Pyrefly][1])
* **LSP method:** `workspace/symbol`
* **Response shape:** `SymbolInformation[] | WorkspaceSymbol[] | null` ([Microsoft GitHub][3])

LSP also defines:

* request param `query: string` (clients may send empty string to request all symbols). ([Microsoft GitHub][3])
* server capability includes `resolveProvider?: boolean` and defines a follow-up request `workspaceSymbol/resolve` to fetch additional info. ([Microsoft GitHub][3])

### What you persist: `index.symbol_hits[]`

**Definition (agent semantic):**
A **flat** list of symbol “hits” returned by the language server for a query, used for global discovery and jumping across the repo.

### Preferred object: `WorkspaceSymbol`

LSP recommends using `WorkspaceSymbol` over `SymbolInformation` and defines a key improvement: its `location` can be either:

* full `Location` (uri + range), or
* `{ uri: DocumentUri }` **without a range**, depending on client `workspace.symbol.resolveSupport`. ([Microsoft GitHub][3])

It also supports:

* `containerName?` (UI qualifier only; not hierarchy)
* `data?` preserved for resolve request. ([Microsoft GitHub][3])

### The normalization you want

Normalize both result forms into one stable internal record:

```json
{
  "name": "pkg.mod.MyClass",
  "kind": "Class",
  "container_name": "pkg.mod",
  "location": { "uri": "file:///…", "range": {...} },   // range optional
  "needs_resolve": true,
  "data_token": {...}
}
```

If you only get `{uri}` without a range:

* immediately treat it as “discovery only”
* then either:

  * call `workspaceSymbol/resolve` (if server supports resolve), or
  * fall back to **go-to-definition** (Section C) once you have a position inside the file. ([Microsoft GitHub][3])

### “So what” for agents

`index.symbol_hits[]` is the **repo-scale navigation primitive**:

* “Find the class that implements X”
* “Where is function Y defined?”
* “List all symbols matching ‘Tokenizer’ across the project”

It prevents a huge class of agent failures where the model “guesses” where something lives. Instead, it can **retrieve** the symbol and jump to it.

Also: LSP supports partial/streaming results via `$/progress` for requests like `workspace/symbol` (useful if results are large). ([Microsoft GitHub][3])

---

## E3) Semantic classification overlay — `semantics.tokens[]` and `semantics.legend`

### Source surface

* **Pyrefly surface:** “Semantic tokens — Rich syntax highlighting based on token type and origin.” ([Pyrefly][1])
* **LSP methods:**

  * `textDocument/semanticTokens/full` → `SemanticTokens | null`
  * `textDocument/semanticTokens/range` → `SemanticTokens | null`
  * `textDocument/semanticTokens/full/delta` → `SemanticTokens | SemanticTokensDelta | null` ([Microsoft GitHub][2])

LSP frames semantic tokens as “additional color information… depends on language specific symbol information”, and notes the result is usually large, so it encodes tokens as numbers and supports deltas. ([Microsoft GitHub][2])

### `semantics.legend`

**Definition:** the mapping the server uses to interpret tokenType/modifier integers.

LSP defines `SemanticTokensLegend`:

* `tokenTypes: string[]`
* `tokenModifiers: string[]` ([Microsoft GitHub][2])

And the spec is explicit: token types are looked up by index; token modifiers are a bitset over `tokenModifiers`. ([Microsoft GitHub][2])

**So what:** without the legend, the `data` array is meaningless. Your tool must persist the legend (typically from initialize/server capability registration) alongside token data.

### `semantics.tokens[]`

**Definition (agent semantic):** a per-token classification overlay that tells you what *role* each span plays (class name vs method vs parameter, and modifiers like definition/declaration/readonly/async/etc.).

**How semantic tokens are encoded (critical)**
LSP defines the integer encoding: each token is **5 integers** in a flat `data: uinteger[]` array: ([Microsoft GitHub][2])

* `deltaLine` (relative line)
* `deltaStart` (relative start character)
* `length`
* `tokenType` (index into `legend.tokenTypes`)
* `tokenModifiers` (bitset over `legend.tokenModifiers`) ([Microsoft GitHub][2])

It also notes `deltaStart` and `length` must be encoded using the **same position encoding** agreed during initialize, and discusses multiline/overlapping token support via capabilities. ([Microsoft GitHub][2])

**Decode algorithm (reference)**

```python
def decode_semantic_tokens(data, legend):
    line = 0
    start = 0
    out = []
    for i in range(0, len(data), 5):
        dline, dstart, length, ttype_i, mods_bits = data[i:i+5]
        line += dline
        start = (start + dstart) if dline == 0 else dstart
        token_type = legend["tokenTypes"][ttype_i]
        mods = [legend["tokenModifiers"][b] for b in range(32) if (mods_bits >> b) & 1]
        out.append({"line": line, "start": start, "length": length, "token_type": token_type, "modifiers": mods})
    return out
```

Your persisted `semantics.tokens[]` should *not* store the raw 5-int tuples as the primary agent interface. Instead store normalized spans:

```json
{
  "span": { "uri": "...", "lsp_range": {...} },
  "token_type": "class|function|parameter|property|...",
  "modifiers": ["definition","readonly","async",...]
}
```

### “So what” for agents

Semantic tokens are a **semantic filter layer** that enables agent behaviors you can’t get reliably from syntax alone:

* **Definition vs reference labeling** (via modifiers like `definition` / `declaration`) makes “rename only definitions” and “extract all public API definitions” far safer. ([Microsoft GitHub][2])
* **Role-aware summarization**: “summarize only classes + top-level functions,” “ignore locals,” “list all decorators,” etc. (token types include things like `class`, `function`, `method`, `parameter`, `property`, and more, and the protocol explicitly frames tokens as language-aware, symbol-informed classification). ([Microsoft GitHub][2])
* **Disambiguation**: if the same string appears as a class name, variable, and module in different contexts, semantic tokens let your UI and your agent prompts reflect that.

The VS Code semantic highlighting guide describes semantic tokenization as additional token information based on the server’s project-aware symbol resolution, layered on top of grammar highlighting. ([Visual Studio Code][4])

---

## “So what” conclusion for E

With E implemented, your agents get a fast, structured “map layer”:

1. **File-scale map** (`outline.document_tree`): what constructs exist and where their boundaries are, with reliable nesting. ([Microsoft GitHub][2])
2. **Repo-scale discovery** (`index.symbol_hits[]`): find symbols globally without guessing paths, with optional resolve. ([Microsoft GitHub][3])
3. **Semantic overlay** (`semantics.tokens[]` + `legend`): role- and definition-aware filtering/summarization and better disambiguation than syntax-only parsing. ([Microsoft GitHub][2])

If you want the follow-on artifact (like A/B/C/D): I can produce a **StructuralDiscoveryBundleV1 schema + minimal request plan** (hierarchicalDocumentSymbolSupport always on, workspace symbol resolve only when needed, semantic tokens range-first + full/delta caching) and a `normalize_structural_discovery.py` module in the same style as your existing normalizers.

[1]: https://pyrefly.org/en/docs/IDE-features/?utm_source=chatgpt.com "IDE Supported Features"
[2]: https://microsoft.github.io/language-server-protocol/specifications/lsp/3.18/specification/ "Specification"
[3]: https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/ "Specification"
[4]: https://code.visualstudio.com/api/language-extensions/semantic-highlight-guide?utm_source=chatgpt.com "Semantic Highlight Guide | Visual Studio Code Extension API"

Below are the three artifacts you asked for:

1. **StructuralDiscoveryBundleV1 schema** (normalized, agent-semantic)
2. **Minimal request plan** (hierarchicalDocumentSymbolSupport always on, workspace symbol resolve only when needed, semantic tokens range-first + full/delta caching)
3. **`normalize_structural_discovery.py`** (same style as prior normalizers; converts raw LSP payloads into stable Finding objects with **no union shapes leaking**)

Pyrefly explicitly supports **Document symbols**, **Workspace symbols**, and **Semantic tokens**. ([Pyrefly][1])

---

## 1) StructuralDiscoveryBundleV1 schema

### 1.1 Top-level bundle

```json
{
  "bundle_kind": "pyrefly.structural_discovery_bundle.v1",
  "meta": {
    "tool": "pyrefly",
    "server_name": "pyrefly",
    "server_version": "…",
    "workspace_root_uri": "file:///…",
    "lsp_position_encoding": "utf-16|utf-8",
    "created_at": "2026-02-07T…Z",
    "config_fingerprint": "sha256:…"
  },
  "document": {
    "uri": "file:///…/path/to/file.py",
    "path": "path/to/file.py",
    "language_id": "python",
    "version": 1,
    "content_sha256": "…"
  },
  "query": {
    "kind": "file_outline|workspace_search|semantic_overlay",
    "document_symbol": { "enabled": true },
    "workspace_symbol": {
      "enabled": true,
      "query": "Tokenizer",
      "resolve_only_when_needed": true,
      "resolve_properties": ["location.range"]
    },
    "semantic_tokens": {
      "enabled": true,
      "mode": "range_first",
      "range": { "start": { "line": 0, "character": 0 }, "end": { "line": 200, "character": 0 } },
      "use_full_delta_cache": true
    }
  },
  "policy": {
    "document_symbols": {
      "hierarchicalDocumentSymbolSupport": true,
      "fallback_behavior": "accept_flat_SymbolInformation_but_do_not_infer_hierarchy"
    },
    "workspace_symbols": {
      "prefer_WorkspaceSymbol": true,
      "client_resolveSupport_properties": ["location.range"],
      "resolveProvider_required": true,
      "resolve_on": "selected|topN|missing_range"
    },
    "semantic_tokens": {
      "prefer_range": true,
      "cache_full_with_resultId": true,
      "apply_delta_when_available": true,
      "decode_using_legend": true
    }
  },
  "findings": [
    { "category": "...", "title": "...", "details": { "kind": "...", "data": {} } }
  ]
}
```

---

## 2) `details.kind` registry

This registry is the “learn once” set of `details.kind` strings your agents should emit.

### Document symbols (E1)

* **category:** `outline.document_tree`
  **details.kind:** `pyrefly.lsp.document_symbols`

  * Source: `textDocument/documentSymbol` returns `DocumentSymbol[] | SymbolInformation[] | null`. ([Microsoft GitHub][2])
  * Critical spec rule: if the server returns `SymbolInformation[]`, it’s a **flat list** and you must not infer hierarchy from `location.range` or `containerName`. ([Microsoft GitHub][2])
  * Client capability to request hierarchy: `textDocument.documentSymbol.hierarchicalDocumentSymbolSupport`. ([Microsoft GitHub][2])

### Workspace symbols (E2)

* **category:** `index.symbol_hits`
  **details.kind:** `pyrefly.lsp.workspace_symbols.search`

  * Source: `workspace/symbol` returns `SymbolInformation[] | WorkspaceSymbol[] | null`. ([Microsoft GitHub][2])
  * WorkspaceSymbol can return `location: Location | { uri }` depending on `workspace.symbol.resolveSupport`. ([Microsoft GitHub][2])

* **category:** `index.symbol_resolve`
  **details.kind:** `pyrefly.lsp.workspace_symbols.resolve`

  * Only when needed: `workspaceSymbol/resolve` resolves additional properties (typically `location.range`). ([Microsoft GitHub][2])
  * Gate with server capability: `workspaceSymbolProvider.resolveProvider?: boolean`. ([Microsoft GitHub][2])

### Semantic tokens (E3)

* **category:** `semantics.legend`
  **details.kind:** `pyrefly.lsp.semantic_tokens.legend`

  * Legend is required to decode numeric token types/modifiers into strings: `tokenTypes[]`, `tokenModifiers[]`. ([Microsoft GitHub][2])

* **category:** `semantics.tokens.range`
  **details.kind:** `pyrefly.lsp.semantic_tokens.range`

  * Source: `textDocument/semanticTokens/range` returns `SemanticTokens | null`. ([Microsoft GitHub][2])

* **category:** `semantics.tokens.full`
  **details.kind:** `pyrefly.lsp.semantic_tokens.full`

  * Source: `textDocument/semanticTokens/full` returns `SemanticTokens | null`, with optional `resultId` enabling delta updates. ([Microsoft GitHub][2])

* **category:** `semantics.tokens.delta`
  **details.kind:** `pyrefly.lsp.semantic_tokens.full.delta`

  * Source: `textDocument/semanticTokens/full/delta` returns `SemanticTokens | SemanticTokensDelta | null`, and `SemanticTokensDelta.edits[]` transform the previous `data` array. ([Microsoft GitHub][2])

* **category:** `semantics.tokens.decoded`
  **details.kind:** `pyrefly.derive.semantic_tokens.decoded`

  * Derived: decoded spans + tokenType + modifiers based on the 5-integer encoding (`deltaLine`, `deltaStart`, `length`, `tokenType`, `tokenModifiers`). ([Microsoft GitHub][2])

---

## 3) Minimal request plan

### 3.1 Initialization capabilities (always on)

**Document symbols**

* Set client capability:
  `textDocument.documentSymbol.hierarchicalDocumentSymbolSupport = true` so the server returns `DocumentSymbol[]` (hierarchy) whenever possible. ([Microsoft GitHub][2])

**Workspace symbols**

* If you want lazy range resolution:

  * Set `workspace.symbol.resolveSupport.properties = ["location.range"]` (client says it can resolve range lazily). ([Microsoft GitHub][2])

**Semantic tokens**

* Capture `semanticTokensProvider.legend` from the server’s capabilities and store it in cache; decoding depends on it. ([Microsoft GitHub][2])
* Respect position encoding negotiation for token `deltaStart` and `length`. The spec requires `deltaStart` and `length` use the negotiated encoding. ([Microsoft GitHub][2])

---

### 3.2 E1 Document symbols plan

* **Per file** (only for files you’re rendering / summarizing):

  1. `didOpen` (or ensure server has file content)
  2. `textDocument/documentSymbol`
* **Normalization rule**

  * If result is `DocumentSymbol[]`: build the tree directly
  * If result is `SymbolInformation[]`: store it as `outline.flat_symbols` and do **not** infer nesting. ([Microsoft GitHub][2])
* **Cache key**

  * `(workspace_root, config_fingerprint, uri, content_sha256, method=documentSymbol)`

---

### 3.3 E2 Workspace symbols plan (resolve only when needed)

* `workspace/symbol` with query string (clients may send empty string to request all symbols). ([Microsoft GitHub][2])
* Normalize each hit to:

  * `{name, kind, containerName, uri, range_or_null, data_token, needs_resolve}`
* Only call `workspaceSymbol/resolve` when:

  * hit has `range == null` and `data_token != null` and
  * (a) user/agent selects it, or (b) it’s in top-N results you want to render with spans.
* Gate resolve on:

  * server `workspaceSymbolProvider.resolveProvider` true ([Microsoft GitHub][2])
  * client `workspace.symbol.resolveSupport` configured ([Microsoft GitHub][2])

**Cache**

* Search results cache: `(workspace_root, config_fingerprint, query, time_bucket|content_epoch)`
* Resolve cache: `(workspace_root, config_fingerprint, hit_key(name+uri+data_token))`

---

### 3.4 E3 Semantic tokens plan (range-first + full/delta caching)

**Range-first (default)**

* For agent-facing summarization/filtering around a match, call:

  * `textDocument/semanticTokens/range` with a small range window. ([Microsoft GitHub][2])
* Decode using legend + 5-int encoding. ([Microsoft GitHub][2])

**Full + delta caching (when you need whole-file semantic overlays)**

1. First time per `(uri, content_sha)`:

   * call `textDocument/semanticTokens/full` and store:

     * `data[]` and `resultId` if present. ([Microsoft GitHub][2])
2. If you support incremental updates (you apply `didChange` and keep the session alive):

   * call `textDocument/semanticTokens/full/delta` with `previousResultId`
   * apply `SemanticTokensEdit {start, deleteCount, data?}` edits to prior `data[]`
   * update cached `resultId` ([Microsoft GitHub][2])

**Cache keys**

* Range tokens: `(uri, content_sha, range, method=semanticTokensRange)`
* Full tokens: `(uri, content_sha, method=semanticTokensFull)`
* Delta tokens: `(uri, previousResultId, method=semanticTokensDelta)` plus resulting `resultId`

---

## 4) `normalize_structural_discovery.py`

```python
# normalize_structural_discovery.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, TypedDict, Literal, cast
import re


# ----------------------------
# Finding shape (same pattern as your existing normalizers)
# ----------------------------

class FindingDetails(TypedDict):
    kind: str
    data: Dict[str, Any]

class Finding(TypedDict):
    category: str
    title: str
    details: FindingDetails

def make_finding(*, category: str, title: str, kind: str, data: Dict[str, Any]) -> Finding:
    return {"category": category, "title": title, "details": {"kind": kind, "data": data}}


# ----------------------------
# Core normalized shapes
# ----------------------------

class Pos(TypedDict):
    line: int
    character: int

class Range(TypedDict):
    start: Pos
    end: Pos

class SpanRef(TypedDict, total=False):
    uri: str
    lsp_range: Range
    byte_span: Dict[str, Any]  # optional: filled upstream if you convert to bytes

class Markup(TypedDict):
    kind: Literal["markdown", "plaintext"]
    value: str

class SymbolKindNorm(TypedDict, total=False):
    code: int
    name: str

class DocumentNode(TypedDict, total=False):
    name: str
    detail: Optional[str]
    kind: SymbolKindNorm
    tags: List[str]
    deprecated: bool
    range: Range
    selection_range: Range
    children: List["DocumentNode"]

class SymbolHit(TypedDict, total=False):
    name: str
    kind: SymbolKindNorm
    container_name: Optional[str]
    uri: str
    range: Optional[Range]              # stable shape: always present, may be null
    data_token: Any
    needs_resolve: bool


# ----------------------------
# Helpers
# ----------------------------

def _is_dict(x: Any) -> bool:
    return isinstance(x, dict)

def _is_list(x: Any) -> bool:
    return isinstance(x, list)

def norm_pos(raw: Any) -> Pos:
    if not _is_dict(raw):
        return {"line": 0, "character": 0}
    return {"line": int(raw.get("line", 0)), "character": int(raw.get("character", 0))}

def norm_range(raw: Any) -> Range:
    if not _is_dict(raw):
        return {"start": {"line": 0, "character": 0}, "end": {"line": 0, "character": 0}}
    return {"start": norm_pos(raw.get("start")), "end": norm_pos(raw.get("end"))}

def _get_int(d: Dict[str, Any], k: str) -> Optional[int]:
    v = d.get(k)
    return int(v) if isinstance(v, int) else None

def _get_str(d: Dict[str, Any], k: str) -> Optional[str]:
    v = d.get(k)
    return str(v) if isinstance(v, str) else None


# ----------------------------
# SymbolKind mapping (keep stable; unknown codes preserved)
# LSP SymbolKind uses numeric enums (1..26) in spec.
# ----------------------------

_SYMBOL_KIND: Dict[int, str] = {
    1: "File",
    2: "Module",
    3: "Namespace",
    4: "Package",
    5: "Class",
    6: "Method",
    7: "Property",
    8: "Field",
    9: "Constructor",
    10: "Enum",
    11: "Interface",
    12: "Function",
    13: "Variable",
    14: "Constant",
    15: "String",
    16: "Number",
    17: "Boolean",
    18: "Array",
    19: "Object",
    20: "Key",
    21: "Null",
    22: "EnumMember",
    23: "Struct",
    24: "Event",
    25: "Operator",
    26: "TypeParameter",
}

def norm_symbol_kind(code: Optional[int]) -> SymbolKindNorm:
    if code is None:
        return {"code": 0, "name": "Unknown"}
    return {"code": code, "name": _SYMBOL_KIND.get(code, "Unknown")}


# ----------------------------
# E1: Document symbols normalization
# LSP: result = DocumentSymbol[] | SymbolInformation[] | null
# If SymbolInformation[]: DO NOT infer hierarchy.
# ----------------------------

def _looks_like_document_symbol(obj: Any) -> bool:
    return _is_dict(obj) and "range" in obj and "selectionRange" in obj and "name" in obj

def _looks_like_symbol_information(obj: Any) -> bool:
    return _is_dict(obj) and "location" in obj and "name" in obj

def normalize_document_symbols(raw: Any, *, uri: str) -> Dict[str, Any]:
    """
    Returns stable shape:
      {
        "document_tree": DocumentNode[],
        "flat_symbols": SymbolHit[],
        "format": "hierarchical|flat|none"
      }
    """
    if raw is None:
        return {"document_tree": [], "flat_symbols": [], "format": "none"}

    if not _is_list(raw):
        return {"document_tree": [], "flat_symbols": [], "format": "none"}

    if len(raw) == 0:
        return {"document_tree": [], "flat_symbols": [], "format": "none"}

    first = raw[0]
    if _looks_like_document_symbol(first):
        # Hierarchical DocumentSymbol[]
        def convert(node: Dict[str, Any]) -> DocumentNode:
            kind = norm_symbol_kind(_get_int(node, "kind"))
            children_raw = node.get("children")
            children: List[DocumentNode] = []
            if _is_list(children_raw):
                children = [convert(c) for c in children_raw if _is_dict(c)]

            tags: List[str] = []
            if _is_list(node.get("tags")):
                # tags are numeric SymbolTag values in LSP; map "Deprecated" if present (1)
                for t in node.get("tags", []):
                    if t == 1:
                        tags.append("Deprecated")

            deprecated = bool(node.get("deprecated", False))  # legacy field still seen in practice

            return {
                "name": str(node.get("name", "")),
                "detail": _get_str(node, "detail"),
                "kind": kind,
                "tags": tags,
                "deprecated": deprecated,
                "range": norm_range(node.get("range")),
                "selection_range": norm_range(node.get("selectionRange")),
                "children": children,
            }

        tree = [convert(n) for n in raw if _is_dict(n)]
        return {"document_tree": tree, "flat_symbols": [], "format": "hierarchical"}

    # Flat SymbolInformation[]
    if _looks_like_symbol_information(first):
        flat: List[SymbolHit] = []
        for si in raw:
            if not _is_dict(si):
                continue
            loc = si.get("location")
            if not _is_dict(loc):
                continue
            flat.append({
                "name": str(si.get("name", "")),
                "kind": norm_symbol_kind(_get_int(si, "kind")),
                "container_name": _get_str(si, "containerName"),
                "uri": str(loc.get("uri", uri)),
                "range": norm_range(loc.get("range")) if "range" in loc else None,
                "data_token": None,
                "needs_resolve": False,
            })
        # IMPORTANT: spec says do not infer hierarchy from location/containerName.
        return {"document_tree": [], "flat_symbols": flat, "format": "flat"}

    return {"document_tree": [], "flat_symbols": [], "format": "none"}


def build_document_symbols_finding(*, uri: str, raw_document_symbols: Any) -> Finding:
    normed = normalize_document_symbols(raw_document_symbols, uri=uri)
    return make_finding(
        category="outline.document_tree",
        title="File outline (document symbols)",
        kind="pyrefly.lsp.document_symbols",
        data={
            "uri": uri,
            "format": normed["format"],
            "document_tree": normed["document_tree"],
            "flat_symbols": normed["flat_symbols"],
            "notes": [
                "If format=flat, do not infer hierarchy from containerName or ranges (LSP spec).",
            ],
        },
    )


# ----------------------------
# E2: Workspace symbols normalization
# LSP: result = SymbolInformation[] | WorkspaceSymbol[] | null
# WorkspaceSymbol.location may be Location OR {uri} (missing range) depending on resolveSupport.
# ----------------------------

def _looks_like_workspace_symbol(obj: Any) -> bool:
    return _is_dict(obj) and "location" in obj and "name" in obj and "kind" in obj and "data" in obj or "location" in obj

def normalize_workspace_symbols(raw: Any) -> List[SymbolHit]:
    """
    Normalizes both WorkspaceSymbol[] and SymbolInformation[] into a stable list:
      SymbolHit {name, kind, container_name, uri, range (nullable), data_token, needs_resolve}
    """
    if raw is None:
        return []
    if not _is_list(raw):
        return []

    hits: List[SymbolHit] = []
    for item in raw:
        if not _is_dict(item):
            continue

        # WorkspaceSymbol has "location" which can be Location or {uri}
        if "location" in item and "name" in item and "kind" in item:
            loc = item.get("location")
            uri = ""
            rng: Optional[Range] = None
            if _is_dict(loc):
                if "uri" in loc and "range" in loc:
                    uri = str(loc.get("uri", ""))
                    rng = norm_range(loc.get("range"))
                elif "uri" in loc:
                    uri = str(loc.get("uri", ""))  # no range
                    rng = None
            data_token = item.get("data")
            needs_resolve = (rng is None) and (data_token is not None)

            hits.append({
                "name": str(item.get("name", "")),
                "kind": norm_symbol_kind(_get_int(item, "kind")),
                "container_name": _get_str(item, "containerName"),
                "uri": uri,
                "range": rng,
                "data_token": data_token,
                "needs_resolve": needs_resolve,
            })
            continue

        # SymbolInformation fallback
        if "location" in item and "name" in item:
            loc = item.get("location")
            if not _is_dict(loc):
                continue
            hits.append({
                "name": str(item.get("name", "")),
                "kind": norm_symbol_kind(_get_int(item, "kind")),
                "container_name": _get_str(item, "containerName"),
                "uri": str(loc.get("uri", "")),
                "range": norm_range(loc.get("range")) if "range" in loc else None,
                "data_token": None,
                "needs_resolve": False,
            })

    return hits


def merge_workspace_symbol_resolve(base: SymbolHit, resolved_raw: Any) -> SymbolHit:
    """
    Merge workspaceSymbol/resolve result into a SymbolHit.
    Primary expected lazily resolved property: location.range (per spec guidance).
    """
    if not _is_dict(resolved_raw):
        return base

    merged: SymbolHit = dict(base)

    loc = resolved_raw.get("location")
    if _is_dict(loc):
        if "uri" in loc:
            merged["uri"] = str(loc.get("uri", merged.get("uri", "")))
        if "range" in loc:
            merged["range"] = norm_range(loc.get("range"))
            merged["needs_resolve"] = False

    # server may also include containerName/tags/etc; keep minimal stable surface
    if "containerName" in resolved_raw and isinstance(resolved_raw["containerName"], str):
        merged["container_name"] = str(resolved_raw["containerName"])

    return merged


def build_workspace_symbols_search_finding(*, query: str, raw_workspace_symbols: Any) -> Finding:
    hits = normalize_workspace_symbols(raw_workspace_symbols)
    return make_finding(
        category="index.symbol_hits",
        title="Workspace symbol search hits",
        kind="pyrefly.lsp.workspace_symbols.search",
        data={
            "query": query,
            "hits": hits,
            "summary": {
                "hit_count": len(hits),
                "needs_resolve_count": sum(1 for h in hits if h.get("needs_resolve") is True),
                "unique_files": len({h.get("uri","") for h in hits if h.get("uri")}),
            },
            "notes": [
                "If a hit has range=null and needs_resolve=true, call workspaceSymbol/resolve only when needed.",
            ],
        },
    )


def build_workspace_symbols_resolve_finding(*, resolved: List[SymbolHit]) -> Finding:
    return make_finding(
        category="index.symbol_resolve",
        title="Workspace symbol resolve results",
        kind="pyrefly.lsp.workspace_symbols.resolve",
        data={
            "resolved_hits": resolved,
            "summary": {
                "resolved_count": len(resolved),
                "still_missing_range": sum(1 for h in resolved if h.get("range") is None),
            },
        },
    )


# ----------------------------
# E3: Semantic tokens normalization + decoding
# LSP: SemanticTokens {resultId?, data: uinteger[]}
# Delta: SemanticTokensDelta {resultId?, edits: [{start, deleteCount, data?}]}
# Encoding: 5 ints per token: deltaLine, deltaStart, length, tokenType, tokenModifiers
# Legend: tokenTypes[], tokenModifiers[]
# ----------------------------

class SemanticLegend(TypedDict):
    tokenTypes: List[str]
    tokenModifiers: List[str]

class SemanticTokenDecoded(TypedDict, total=False):
    span: SpanRef
    token_type: str
    modifiers: List[str]

def normalize_semantic_tokens_full_or_range(raw: Any) -> Dict[str, Any]:
    """
    Returns stable shape:
      { "result_id": str|None, "data": int[], "kind": "full|range|none" }
    Caller sets kind in the Finding.
    """
    if not _is_dict(raw):
        return {"result_id": None, "data": [], "kind": "none"}
    data = raw.get("data")
    if not _is_list(data):
        return {"result_id": None, "data": [], "kind": "none"}
    rid = raw.get("resultId")
    return {"result_id": str(rid) if isinstance(rid, str) else None, "data": [int(x) for x in data], "kind": "ok"}

def normalize_semantic_tokens_delta(raw: Any) -> Dict[str, Any]:
    """
    Returns stable shape:
      { "result_id": str|None, "edits": [{start:int, delete_count:int, data:int[]}] }
    """
    if not _is_dict(raw):
        return {"result_id": None, "edits": []}
    rid = raw.get("resultId")
    edits_raw = raw.get("edits")
    edits: List[Dict[str, Any]] = []
    if _is_list(edits_raw):
        for e in edits_raw:
            if not _is_dict(e):
                continue
            start = int(e.get("start", 0))
            delete_count = int(e.get("deleteCount", 0))
            data_raw = e.get("data")
            data_ins: List[int] = [int(x) for x in data_raw] if _is_list(data_raw) else []
            edits.append({"start": start, "delete_count": delete_count, "data": data_ins})
    return {"result_id": str(rid) if isinstance(rid, str) else None, "edits": edits}

def apply_semantic_tokens_delta(prev_data: List[int], edits: List[Dict[str, Any]]) -> List[int]:
    """
    Applies SemanticTokensEdit operations to the prior integer array.
    Each edit: {start, deleteCount, data?}
    """
    data = list(prev_data)
    # Edits are offsets into the integer array. Apply in order; spec intends them to transform previous result.
    # To be safe, apply from last to first by start offset.
    for e in sorted(edits, key=lambda x: x["start"], reverse=True):
        s = int(e["start"])
        dc = int(e["delete_count"])
        ins = cast(List[int], e.get("data", []))
        data[s:s+dc] = ins
    return data

def decode_semantic_tokens(*, uri: str, data: List[int], legend: SemanticLegend) -> List[SemanticTokenDecoded]:
    """
    Decodes the 5-int relative format into spans:
      [deltaLine, deltaStart, length, tokenTypeIdx, tokenModifiersBitset] repeated.
    """
    out: List[SemanticTokenDecoded] = []
    if not data:
        return out

    token_types = legend.get("tokenTypes", [])
    token_mods = legend.get("tokenModifiers", [])

    line = 0
    start = 0
    for i in range(0, len(data) - 4, 5):
        delta_line = int(data[i])
        delta_start = int(data[i+1])
        length = int(data[i+2])
        ttype_i = int(data[i+3])
        mods_bits = int(data[i+4])

        line += delta_line
        start = (start + delta_start) if delta_line == 0 else delta_start

        token_type = token_types[ttype_i] if 0 <= ttype_i < len(token_types) else "unknown"
        modifiers: List[str] = []
        # Each set bit indexes into tokenModifiers.
        for b in range(len(token_mods)):
            if (mods_bits >> b) & 1:
                modifiers.append(token_mods[b])

        out.append({
            "span": {
                "uri": uri,
                "lsp_range": {
                    "start": {"line": line, "character": start},
                    "end": {"line": line, "character": start + length},
                },
            },
            "token_type": token_type,
            "modifiers": modifiers,
        })

    return out

_IMPORT_RE = re.compile(r"^\s*(from\s+\S+\s+import\s+|import\s+\S+)", re.M)

def build_semantic_legend_finding(*, legend: SemanticLegend) -> Finding:
    return make_finding(
        category="semantics.legend",
        title="Semantic tokens legend",
        kind="pyrefly.lsp.semantic_tokens.legend",
        data={"legend": legend},
    )

def build_semantic_tokens_range_finding(*, uri: str, range_queried: Range, raw_tokens: Any) -> Finding:
    normed = normalize_semantic_tokens_full_or_range(raw_tokens)
    return make_finding(
        category="semantics.tokens.range",
        title="Semantic tokens (range)",
        kind="pyrefly.lsp.semantic_tokens.range",
        data={"uri": uri, "range_queried": range_queried, "result_id": normed["result_id"], "data": normed["data"]},
    )

def build_semantic_tokens_full_finding(*, uri: str, raw_tokens: Any) -> Finding:
    normed = normalize_semantic_tokens_full_or_range(raw_tokens)
    return make_finding(
        category="semantics.tokens.full",
        title="Semantic tokens (full)",
        kind="pyrefly.lsp.semantic_tokens.full",
        data={"uri": uri, "result_id": normed["result_id"], "data": normed["data"]},
    )

def build_semantic_tokens_delta_finding(*, uri: str, previous_result_id: str, raw_delta: Any) -> Finding:
    d = normalize_semantic_tokens_delta(raw_delta)
    return make_finding(
        category="semantics.tokens.delta",
        title="Semantic tokens (full delta)",
        kind="pyrefly.lsp.semantic_tokens.full.delta",
        data={"uri": uri, "previous_result_id": previous_result_id, "result_id": d["result_id"], "edits": d["edits"]},
    )

def build_semantic_tokens_decoded_finding(*, uri: str, data: List[int], legend: SemanticLegend) -> Finding:
    decoded = decode_semantic_tokens(uri=uri, data=data, legend=legend)
    return make_finding(
        category="semantics.tokens.decoded",
        title="Semantic tokens (decoded)",
        kind="pyrefly.derive.semantic_tokens.decoded",
        data={"uri": uri, "token_count": len(decoded), "tokens": decoded},
    )
```

### Why this module matches your “no unions leaking” requirement

* `normalize_document_symbols()` always returns stable keys: `document_tree: []`, `flat_symbols: []`, `format: str` even though the LSP result is a union (`DocumentSymbol[] | SymbolInformation[] | null`). ([Microsoft GitHub][2])
* `normalize_workspace_symbols()` always yields `SymbolHit[]` where `range` is always present (nullable) even though the LSP result is a union (`SymbolInformation[] | WorkspaceSymbol[] | null`) and `WorkspaceSymbol.location` can be `{uri}` without a range. ([Microsoft GitHub][2])
* Semantic tokens are normalized into stable arrays and decoded into explicit spans using:

  * legend mapping (`tokenTypes`, `tokenModifiers`) ([Microsoft GitHub][2])
  * 5-int token encoding (`deltaLine`, `deltaStart`, `length`, `tokenType`, `tokenModifiers`) ([Microsoft GitHub][2])
  * full/delta mechanics via `resultId` and `SemanticTokensEdit` transforms ([Microsoft GitHub][2])

---


[1]: https://pyrefly.org/en/docs/IDE-features/ "IDE Supported Features | Pyrefly"
[2]: https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/ "Specification"

Below is the deep dive for **F) Safe transformations and edit plans**, framed exactly around your **agent-semantic variables** and “what an LLM agent would *do* with them”.

The key unifier: almost all IDE-side transformations surface as a **`WorkspaceEdit`** (plus optional file operations). A `WorkspaceEdit` can include text edits across many files *and* resource operations like file renames, and those operations must be executed in order. ([Microsoft GitHub][1])

---

## F0) The core “edit plan” primitive: `WorkspaceEdit`

### What it is (LSP semantics)

A `WorkspaceEdit` represents changes to many resources. It can be expressed either as:

* `changes: { [uri]: TextEdit[] }` (simple unversioned map), **or**
* `documentChanges: (TextDocumentEdit[] | (TextDocumentEdit | CreateFile | RenameFile | DeleteFile)[])` (supports versioned edits and file operations). ([Microsoft GitHub][1])

**Ordering matters:** if resource operations are present, clients execute them in the provided order; invalid sequences can cause failure. ([Microsoft GitHub][1])

### Why agents care (“so what”)

If you persist `WorkspaceEdit` as your canonical edit plan:

* You stop “guessing” edits (ripgrep/AST heuristics) and instead apply **server-computed, semantics-aware patches**.
* You can safely do project-wide renames, signature refactors, and file moves in one transaction.

---

## F1) Rename a symbol project-wide

### Pyrefly surface

Pyrefly: **“Rename — Safely rename symbols project-wide.”** ([Pyrefly][2])

### LSP mechanism

There are two related requests:

1. **Prepare rename** — check validity / determine the rename range
   `textDocument/prepareRename` returns one of:

* `Range`
* `{ range: Range, placeholder: string }`
* `{ defaultBehavior: boolean }` (client uses default identifier-selection rules)
* `null` (rename not valid at position)
  …and it can return an error “element can’t be renamed” (clients should show it). ([Microsoft GitHub][1])

2. **Rename** — compute the workspace change
   `textDocument/rename` takes `newName: string` (invalid name → error) and returns `WorkspaceEdit | null` (null ≈ empty edit). Errors also cover “nothing to rename”, “symbol not supported”, “code invalid”. ([Microsoft GitHub][1])
   The spec describes rename as computing a workspace change for a workspace-wide rename. ([Microsoft GitHub][1])

### Variables

#### `edit.rename_plan`

**Definition (agent semantic):** the **complete, mechanically correct patch plan** to rename the symbol everywhere (and only where semantically appropriate), represented as a normalized `WorkspaceEdit`.

**Shape you should store (normalized):**

* `text_edits_by_uri[]`: list of `{ uri, edits: [ {range, newText} ] }`
* `resource_ops[]`: any file ops embedded in `documentChanges` (rare for rename-symbol; more common for move/rename file)
* `summary`: `{files_touched, edits_count}`

**So what (code behavior understanding):**

* A rename plan is “semantic identity preservation”: you’re changing *spelling* without changing *binding*.
* It proves that downstream “this code still refers to the same callable/variable/class” because the server computed symbol-aware edits.

#### `edit.rename_conflicts?`

**Definition:** structured “cannot rename” / “shouldn’t rename” reasons, plus any preflight validity output.

**Where it comes from:**

* `prepareRename` error (human-readable reason) or `null` (not renamable here). ([Microsoft GitHub][1])
* `rename` request error, including invalid `newName`, “nothing at position”, “symbol doesn’t support renaming”, or “code invalid”. ([Microsoft GitHub][1])

**So what:**

* This is the **safety rail** that prevents agents from applying nonsense refactors (“rename whitespace”, “rename keyword”, “rename cannot be performed because code is invalid”).
* It also becomes a **decision point**: agents can switch strategy (e.g., rename a different token span, fix parse/type errors first, or fall back to textual rename only if explicitly allowed).

---

## F2) Move/rename file and update references

### Pyrefly surface

Pyrefly: **“Move/rename file — … do it’s best to rename all references to it.”** ([Pyrefly][2])

### LSP mechanisms (two places it can appear)

There are two relevant LSP pathways:

1. **File operations inside `WorkspaceEdit`**
   `WorkspaceEdit.documentChanges` can include `RenameFile` operations alongside text edits. `RenameFile` is:

````ts
{ kind:'rename', oldUri: DocumentUri, newUri: DocumentUri, options?: ... }
``` :contentReference[oaicite:10]{index=10}

2) **Pre-rename request hook (common in editors)**
`workspace/willRenameFiles` is sent by the client **before** files are renamed (user action or applying a workspace edit). It can return a `WorkspaceEdit` that is applied before the rename. The spec warns clients may drop results if too slow or consistently failing “to keep renames fast and reliable.” :contentReference[oaicite:11]{index=11}

### Variables

#### `edit.file_move_plan`
**Definition (agent semantic):** a two-part plan:
1) the **resource operation(s)**: file `oldUri → newUri`
2) the **reference rewrites**: import updates and other text edits across the repo

**What you should persist:**
- `file_ops[]`:
  - `{kind:'rename', oldUri, newUri, options?}`
- `workspace_edit_text_changes[]`:
  - per-uri text edits (imports, module paths, package references)
- `source`: `"workspace/willRenameFiles"` vs `"WorkspaceEdit.documentChanges"` (because behaviors differ in timing)

**So what (code behavior understanding):**
- File moves change Python’s import graph. A correct plan must rewrite import edges consistently; this is hard to do textually.
- The `file_move_plan` is your “import-graph preservation artifact”: after applying it, imports resolve to the same code (now at a new path).

**Operational note for your tool:**  
Because the spec explicitly says `willRenameFiles` results may be dropped for performance/reliability reasons, treat this path as **best-effort** and record when it was skipped/dropped. :contentReference[oaicite:12]{index=12}

---

## F3) Refactor operations (higher-level structured changes)

### Pyrefly surfaces (supported refactors)
Pyrefly’s IDE features page lists these refactors and describes their intent:
- Pull Member Up / Push Member Down  
- Convert to Package / Convert to Module  
- Move function to top level  
- Introduce Parameter (update definition + all call sites)  
- Inline Variable :contentReference[oaicite:13]{index=13}

### Transport: typically **Code Actions**
In LSP, refactors are usually surfaced through `textDocument/codeAction`, whose result is `(Command | CodeAction)[]`. A `CodeAction` must set `edit` and/or `command`; if both, edit is applied first. :contentReference[oaicite:14]{index=14}  
Clients can filter what they ask for using `CodeActionContext.only` (kinds not requested can be omitted by servers). :contentReference[oaicite:15]{index=15}  
A `CodeAction` can also be **disabled** with a human-readable `reason`, and its edit can be computed lazily via `codeAction/resolve` to avoid expensive computation in the initial request. :contentReference[oaicite:16]{index=16}

### Variables

#### `refactor.available_actions[]`
**Definition (agent semantic):** the menu of *semantically valid transformations* at a target span (selection), including whether they’re applicable now.

**Where it comes from:**
- `textDocument/codeAction` for a given `{textDocument, range, context}`; optionally provide `context.only` to request only refactors. :contentReference[oaicite:17]{index=17}

**What to persist per action:**
- `title` (human name)
- `kind` (used for filtering/grouping)
- `disabled.reason?` (if not currently applicable) :contentReference[oaicite:18]{index=18}
- `data?` (opaque token for resolve) :contentReference[oaicite:19]{index=19}
- `edit_present` (whether `edit` already present or needs resolve)
- `command_present` (whether execution requires command)

**So what:**
- This is the agent’s *capability surface*: “these are the safe, server-backed moves you can do here.”
- It prevents brittle, model-invented refactor steps when the server can provide a verified plan.

#### `refactor.edit_plan`
**Definition:** the concrete patch plan for a chosen refactor, in `WorkspaceEdit` form.

**Where it comes from:**
- `CodeAction.edit` directly, **or**
- `codeAction/resolve` to compute `edit` lazily (the spec explicitly calls this out as the main purpose of resolve). :contentReference[oaicite:20]{index=20}

**So what:**
- For refactors like **Introduce Parameter** and **Inline Variable**, the edit plan encodes the nontrivial “update all call sites / replace all uses” behavior that is easy to get wrong by hand. :contentReference[oaicite:21]{index=21}

#### `refactor.preconditions`
**Definition:** explicit reasons an action cannot be applied *now*.

**Where it comes from:**
- `CodeAction.disabled.reason` (human-readable). :contentReference[oaicite:22]{index=22}  
- If the action is a `Command`-only result, preconditions may also show up as errors when executing the command, but the spec-level “precondition surface” is the disabled reason.

**So what:**
- This is the missing ingredient for agent autonomy: “you can’t do this because X; fix X first.”
- It’s also how you avoid wasted cycles (agent keeps attempting invalid refactors).

---

## F4) Automated annotation patch (type-driven code edit): `pyrefly infer`

### Pyrefly surface
Pyrefly documents `pyrefly infer` as:
- run on a file or directory  
- recommended **small batches** for review/merge  
- flags can enable/disable annotating parameters, return types, containers  
- new annotations often expose new type errors :contentReference[oaicite:23]{index=23}

### Variable

#### `infer.annotation_patch`
**Definition (agent semantic):** a deterministic patch that converts inferred types into explicit annotations across the chosen scope.

**How to persist (practical):**
- `annotation_scope`: `{kind:file|directory, path}`
- `diff_unified`: textual diff
- `structured_edits`: per-file hunks or text-edits (your normalized internal representation)

**So what (code behavior understanding):**
- Annotations are *contracts*. This patch makes implicit contracts explicit, reducing ambiguity for future edits and for future inference.
- It also changes what refactors become safe: once return/param types are explicit, tools (and agents) can reason about changes with higher confidence.

---

## “So what” conclusion for F
Section F is where Pyrefly/LSP stops being “context” and becomes **action**:

- `edit.rename_plan` and `edit.file_move_plan` are *semantic preservation proofs* (bindings and imports stay correct after edits). :contentReference[oaicite:24]{index=24}  
- `refactor.available_actions[]` + `refactor.edit_plan` convert “high-level intent” (introduce parameter, inline variable, etc.) into a mechanically correct patch across all required sites. :contentReference[oaicite:25]{index=25}  
- `infer.annotation_patch` is the “type hardening” lever that upgrades future automation reliability. :contentReference[oaicite:26]{index=26}  

If you want the follow-on artifact like prior sections: I can produce a **SafeEditsBundleV1** schema + a `normalize_safe_edits.py` module (rename + willRenameFiles + codeAction/resolve + infer diff normalization), with the same “no unions leak” guarantees.
::contentReference[oaicite:27]{index=27}
````

[1]: https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/ "Specification"
[2]: https://pyrefly.org/en/docs/IDE-features/ "IDE Supported Features | Pyrefly"

Below are the two artifacts you asked for:

1. **SafeEditsBundleV1** (schema + `details.kind` registry)
2. **`normalize_safe_edits.py`** (rename + willRenameFiles + codeAction/resolve + infer diff normalization) with **no unions leaking** (every `… | … | null` is normalized to stable keys and empty lists instead of `null`).

I’m grounding the contract on the LSP spec’s exact semantics for **rename**, **prepareRename**, **WorkspaceEdit** (including file ops + ordering), **willRenameFiles**, and **codeAction/resolve**, plus Pyrefly’s feature set and `pyrefly infer` docs. ([Microsoft GitHub][1])

---

## 1) SafeEditsBundleV1

### 1.1 Bundle schema

```json
{
  "bundle_kind": "pyrefly.safe_edits_bundle.v1",
  "meta": {
    "tool": "pyrefly",
    "server_version": "…",
    "workspace_root_uri": "file:///…",
    "lsp_position_encoding": "utf-16|utf-8",
    "config_fingerprint": "sha256:…",
    "created_at": "2026-02-07T…Z"
  },
  "document": {
    "uri": "file:///…/path/to/file.py",
    "path": "path/to/file.py",
    "language_id": "python",
    "version": 1,
    "content_sha256": "…"
  },
  "request": {
    "mode": "rename_symbol|rename_files|code_actions|infer_annotations",
    "anchor_position": { "line": 10, "character": 8 },
    "selection_range": { "start": { "line": 10, "character": 8 }, "end": { "line": 10, "character": 12 } },
    "new_name": "newIdentifier",
    "rename_files": [ { "oldUri": "file:///…/a.py", "newUri": "file:///…/b.py" } ],
    "code_action_range": { "start": { "line": 10, "character": 0 }, "end": { "line": 12, "character": 0 } },
    "infer_scope": { "kind": "file|directory", "path": "…" }
  },
  "policy": {
    "workspace_edit": {
      "prefer_documentChanges": true,
      "preserve_operation_order": true
    },
    "code_action": {
      "resolve_edit_when_missing": true,
      "treat_disabled_as_precondition": true
    },
    "infer": {
      "capture_unified_diff": true,
      "small_batches": true
    }
  },
  "findings": [
    { "category": "...", "title": "...", "details": { "kind": "...", "data": { } } }
  ]
}
```

### 1.2 `details.kind` registry

Each kind below maps to a single normalized payload shape.

#### Rename

* **category:** `edit.rename.prepare`
  **details.kind:** `pyrefly.lsp.prepare_rename`
  **source:** `textDocument/prepareRename` returns `Range | {range, placeholder} | {defaultBehavior} | null` (or error). ([Microsoft GitHub][1])

* **category:** `edit.rename.plan`
  **details.kind:** `pyrefly.lsp.rename`
  **source:** `textDocument/rename` returns `WorkspaceEdit | null`; errors include “nothing at position”, “symbol doesn’t support renaming”, or “code is invalid”. ([Microsoft GitHub][1])

#### Move/rename file (import rewrites)

* **category:** `edit.fileops.will_rename_files`
  **details.kind:** `pyrefly.lsp.will_rename_files`
  **source:** `workspace/willRenameFiles` returns `WorkspaceEdit | null`, and clients may drop results to keep renames fast/reliable. ([Microsoft GitHub][1])

* **category:** `edit.workspace_edit`
  **details.kind:** `lsp.workspace_edit.normalized`
  **source:** `WorkspaceEdit` may contain text edits and (since 3.13) resource ops; if ops are present, clients must execute them **in order**. ([Microsoft GitHub][1])
  (This is a shared normalized representation used by rename, willRenameFiles, and code actions.)

#### Refactors / code actions

* **category:** `refactor.actions`
  **details.kind:** `pyrefly.lsp.code_actions`
  **source:** `textDocument/codeAction` returns `(Command | CodeAction)[] | null`; a `CodeAction` must set `edit` and/or `command`, and if both are supplied, `edit` is applied first. Disabled actions include a human-readable reason. ([Microsoft GitHub][1])

* **category:** `refactor.actions.resolve`
  **details.kind:** `pyrefly.lsp.code_action.resolve`
  **source:** `codeAction/resolve` returns a `CodeAction` and is commonly used to compute the `edit` lazily; servers shouldn’t alter existing attributes when resolving. ([Microsoft GitHub][1])

#### Type-driven edit plan (infer)

* **category:** `infer.annotation_patch`
  **details.kind:** `pyrefly.cli.infer.diff`
  **source:** `pyrefly infer` runs on file/dir, recommended in small batches; flags toggle parameters/returns/containers; changes should be reviewed. ([Pyrefly][2])

---

## 2) `normalize_safe_edits.py`

This module:

* Normalizes `WorkspaceEdit` (both `changes` and `documentChanges`, including `CreateFile` / `RenameFile` / `DeleteFile`) and preserves op order. ([Microsoft GitHub][1])
* Normalizes `prepareRename` union result into `{valid, range?, placeholder?, default_behavior?, error?}`. ([Microsoft GitHub][1])
* Normalizes rename plan (`WorkspaceEdit | null`) and willRenameFiles (`WorkspaceEdit | null`) into stable `Finding`s. ([Microsoft GitHub][1])
* Normalizes `textDocument/codeAction` union `(Command|CodeAction)[]|null` and supports resolve to fill missing `edit`. ([Microsoft GitHub][1])
* Normalizes a **unified diff** (for your “run infer in a temp dir + git diff” flow) into structured file/hunk objects. ([Pyrefly][2])

```python
# normalize_safe_edits.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, TypedDict, Literal, cast
import re


# ----------------------------
# Finding shape (consistent with your other normalizers)
# ----------------------------

class FindingDetails(TypedDict):
    kind: str
    data: Dict[str, Any]

class Finding(TypedDict):
    category: str
    title: str
    details: FindingDetails

def make_finding(*, category: str, title: str, kind: str, data: Dict[str, Any]) -> Finding:
    return {"category": category, "title": title, "details": {"kind": kind, "data": data}}


# ----------------------------
# LSP core shapes (normalized)
# ----------------------------

class Pos(TypedDict):
    line: int
    character: int

class Range(TypedDict):
    start: Pos
    end: Pos

class TextEditNorm(TypedDict, total=False):
    range: Range
    new_text: str
    annotation_id: str  # AnnotatedTextEdit support (optional)

class TextDocEditsNorm(TypedDict, total=False):
    uri: str
    version: Optional[int]
    edits: List[TextEditNorm]

class FileOpNorm(TypedDict, total=False):
    kind: Literal["create", "rename", "delete"]
    uri: str
    old_uri: str
    new_uri: str
    options: Dict[str, Any]
    annotation_id: str

class WorkspaceEditNorm(TypedDict, total=False):
    # Stable shape: always present keys with lists, never unions.
    text_document_edits: List[TextDocEditsNorm]
    resource_operations: List[FileOpNorm]
    uses_document_changes: bool
    notes: List[str]


# ----------------------------
# Helpers
# ----------------------------

def _is_dict(x: Any) -> bool:
    return isinstance(x, dict)

def _is_list(x: Any) -> bool:
    return isinstance(x, list)

def norm_pos(raw: Any) -> Pos:
    if not _is_dict(raw):
        return {"line": 0, "character": 0}
    return {"line": int(raw.get("line", 0)), "character": int(raw.get("character", 0))}

def norm_range(raw: Any) -> Range:
    if not _is_dict(raw):
        return {"start": {"line": 0, "character": 0}, "end": {"line": 0, "character": 0}}
    return {"start": norm_pos(raw.get("start")), "end": norm_pos(raw.get("end"))}

def _get_int(d: Dict[str, Any], k: str) -> Optional[int]:
    v = d.get(k)
    return int(v) if isinstance(v, int) else None

def _get_str(d: Dict[str, Any], k: str) -> Optional[str]:
    v = d.get(k)
    return str(v) if isinstance(v, str) else None


# ----------------------------
# WorkspaceEdit normalization
# - WorkspaceEdit can contain changes OR documentChanges.
# - documentChanges can mix TextDocumentEdit + CreateFile/RenameFile/DeleteFile.
# - If resource ops present, order must be preserved for application.
# ----------------------------

def _norm_text_edit(raw: Any) -> Optional[TextEditNorm]:
    if not _is_dict(raw):
        return None
    # TextEdit: {range, newText}
    if "range" in raw and "newText" in raw:
        out: TextEditNorm = {"range": norm_range(raw["range"]), "new_text": str(raw["newText"])}
        # AnnotatedTextEdit: same fields + annotationId
        if "annotationId" in raw and isinstance(raw["annotationId"], str):
            out["annotation_id"] = raw["annotationId"]
        return out
    return None

def _norm_text_document_edit(raw: Any) -> Optional[TextDocEditsNorm]:
    if not _is_dict(raw):
        return None
    if "textDocument" not in raw or "edits" not in raw:
        return None
    td = raw.get("textDocument")
    edits_raw = raw.get("edits")
    if not _is_dict(td) or not _is_list(edits_raw):
        return None
    uri = str(td.get("uri", ""))
    version = _get_int(td, "version")
    edits: List[TextEditNorm] = []
    for e in edits_raw:
        ne = _norm_text_edit(e)
        if ne:
            edits.append(ne)
    return {"uri": uri, "version": version, "edits": edits}

def _norm_create_file(raw: Dict[str, Any]) -> FileOpNorm:
    return {
        "kind": "create",
        "uri": str(raw.get("uri", "")),
        "options": cast(Dict[str, Any], raw.get("options") or {}),
        "annotation_id": str(raw.get("annotationId")) if isinstance(raw.get("annotationId"), str) else "",
    }

def _norm_rename_file(raw: Dict[str, Any]) -> FileOpNorm:
    return {
        "kind": "rename",
        "old_uri": str(raw.get("oldUri", "")),
        "new_uri": str(raw.get("newUri", "")),
        "options": cast(Dict[str, Any], raw.get("options") or {}),
        "annotation_id": str(raw.get("annotationId")) if isinstance(raw.get("annotationId"), str) else "",
    }

def _norm_delete_file(raw: Dict[str, Any]) -> FileOpNorm:
    return {
        "kind": "delete",
        "uri": str(raw.get("uri", "")),
        "options": cast(Dict[str, Any], raw.get("options") or {}),
        "annotation_id": str(raw.get("annotationId")) if isinstance(raw.get("annotationId"), str) else "",
    }

def normalize_workspace_edit(raw: Any) -> WorkspaceEditNorm:
    """
    Normalize LSP WorkspaceEdit into:
      - text_document_edits: [{uri, version?, edits:[{range,new_text,annotation_id?}]}]
      - resource_operations: [{kind, ...}]
    No unions leak: empty lists instead of null; booleans always present.
    """
    out: WorkspaceEditNorm = {
        "text_document_edits": [],
        "resource_operations": [],
        "uses_document_changes": False,
        "notes": [],
    }

    if raw is None:
        return out
    if not _is_dict(raw):
        out["notes"].append("unexpected_workspace_edit_shape")
        return out

    # Simple "changes" form: { [uri]: TextEdit[] }
    changes = raw.get("changes")
    if _is_dict(changes):
        for uri, edits_raw in changes.items():
            if not isinstance(uri, str) or not _is_list(edits_raw):
                continue
            edits: List[TextEditNorm] = []
            for e in edits_raw:
                ne = _norm_text_edit(e)
                if ne:
                    edits.append(ne)
            out["text_document_edits"].append({"uri": uri, "version": None, "edits": edits})

    # Rich form: documentChanges
    dc = raw.get("documentChanges")
    if _is_list(dc):
        out["uses_document_changes"] = True
        for item in dc:
            if not _is_dict(item):
                continue

            # Resource operations are identified by "kind"
            kind = item.get("kind")
            if kind == "create":
                out["resource_operations"].append(_norm_create_file(item))
                continue
            if kind == "rename":
                out["resource_operations"].append(_norm_rename_file(item))
                continue
            if kind == "delete":
                out["resource_operations"].append(_norm_delete_file(item))
                continue

            # Otherwise TextDocumentEdit
            tde = _norm_text_document_edit(item)
            if tde:
                out["text_document_edits"].append(tde)

        if out["resource_operations"]:
            out["notes"].append("resource_operations_present_preserve_order")

    return out


def build_workspace_edit_finding(*, title: str, raw_workspace_edit: Any) -> Finding:
    we = normalize_workspace_edit(raw_workspace_edit)
    # Simple summaries
    files_touched = {t["uri"] for t in we["text_document_edits"] if t.get("uri")}
    edits_count = sum(len(t.get("edits", [])) for t in we["text_document_edits"])
    return make_finding(
        category="edit.workspace_edit",
        title=title,
        kind="lsp.workspace_edit.normalized",
        data={
            "workspace_edit": we,
            "summary": {
                "files_touched": len(files_touched),
                "text_edits": edits_count,
                "resource_ops": len(we["resource_operations"]),
            },
        },
    )


# ----------------------------
# PrepareRename normalization
# result = Range | {range, placeholder} | {defaultBehavior} | null
# plus possible error message.
# ----------------------------

class PrepareRenameNorm(TypedDict, total=False):
    valid: bool
    range: Optional[Range]
    placeholder: Optional[str]
    default_behavior: bool
    error: Optional[str]

def normalize_prepare_rename_result(result: Any, *, error: Optional[str] = None) -> PrepareRenameNorm:
    out: PrepareRenameNorm = {
        "valid": False,
        "range": None,
        "placeholder": None,
        "default_behavior": False,
        "error": error,
    }

    if result is None:
        return out

    if _is_dict(result):
        # {range, placeholder}
        if "range" in result and _is_dict(result["range"]):
            out["valid"] = True
            out["range"] = norm_range(result["range"])
            if isinstance(result.get("placeholder"), str):
                out["placeholder"] = str(result["placeholder"])
            return out
        # {defaultBehavior}
        if "defaultBehavior" in result:
            out["valid"] = True
            out["default_behavior"] = bool(result.get("defaultBehavior"))
            return out

    # Range object
    if _is_dict(result) and "start" in result and "end" in result:
        out["valid"] = True
        out["range"] = norm_range(result)
        return out

    return out


def build_prepare_rename_finding(
    *,
    anchor: Pos,
    result: Any,
    error: Optional[str] = None,
) -> Finding:
    pr = normalize_prepare_rename_result(result, error=error)
    return make_finding(
        category="edit.rename.prepare",
        title="Rename preflight (prepareRename)",
        kind="pyrefly.lsp.prepare_rename",
        data={"anchor": anchor, "prepare": pr},
    )


# ----------------------------
# Rename normalization
# textDocument/rename => WorkspaceEdit | null (+ errors outside result)
# ----------------------------

def build_rename_plan_finding(*, anchor: Pos, new_name: str, raw_workspace_edit: Any, error: Optional[str] = None) -> Finding:
    we = normalize_workspace_edit(raw_workspace_edit)
    return make_finding(
        category="edit.rename.plan",
        title="Rename plan (workspace-wide)",
        kind="pyrefly.lsp.rename",
        data={
            "anchor": anchor,
            "new_name": new_name,
            "rename_plan": we,
            "error": error,
        },
    )


# ----------------------------
# willRenameFiles normalization
# workspace/willRenameFiles => WorkspaceEdit | null
# params: RenameFilesParams {files:[{oldUri,newUri}]}
# ----------------------------

class FileRenameNorm(TypedDict):
    old_uri: str
    new_uri: str

def normalize_rename_files_params(params: Any) -> List[FileRenameNorm]:
    if not _is_dict(params):
        return []
    files = params.get("files")
    if not _is_list(files):
        return []
    out: List[FileRenameNorm] = []
    for f in files:
        if not _is_dict(f):
            continue
        oldu = f.get("oldUri")
        newu = f.get("newUri")
        if isinstance(oldu, str) and isinstance(newu, str):
            out.append({"old_uri": oldu, "new_uri": newu})
    return out

def build_will_rename_files_finding(*, params: Any, raw_workspace_edit: Any, error: Optional[str] = None) -> Finding:
    renames = normalize_rename_files_params(params)
    we = normalize_workspace_edit(raw_workspace_edit)
    return make_finding(
        category="edit.fileops.will_rename_files",
        title="File rename/move plan (willRenameFiles)",
        kind="pyrefly.lsp.will_rename_files",
        data={
            "files": renames,
            "edit_plan": we,
            "error": error,
            "notes": ["Clients may drop willRenameFiles results if too slow; treat as best-effort."],
        },
    )


# ----------------------------
# Code actions + resolve normalization
# textDocument/codeAction => (Command | CodeAction)[] | null
# codeAction/resolve => CodeAction
# ----------------------------

class CommandNorm(TypedDict, total=False):
    title: str
    command: str
    arguments: List[Any]

class CodeActionNorm(TypedDict, total=False):
    title: str
    kind: Optional[str]
    is_preferred: bool
    disabled_reason: Optional[str]
    edit: WorkspaceEditNorm
    command: Optional[CommandNorm]
    data_token: Any
    resolved: bool

class ActionCandidate(TypedDict, total=False):
    action_type: Literal["command", "code_action"]
    command: CommandNorm
    code_action: CodeActionNorm


def _norm_command(raw: Any) -> Optional[CommandNorm]:
    if not _is_dict(raw):
        return None
    # Command requires "title" and "command"
    if isinstance(raw.get("title"), str) and isinstance(raw.get("command"), str):
        args = raw.get("arguments")
        return {
            "title": str(raw["title"]),
            "command": str(raw["command"]),
            "arguments": list(args) if _is_list(args) else [],
        }
    return None


def _norm_code_action(raw: Any) -> Optional[CodeActionNorm]:
    if not _is_dict(raw) or not isinstance(raw.get("title"), str):
        return None

    disabled_reason = None
    dis = raw.get("disabled")
    if _is_dict(dis) and isinstance(dis.get("reason"), str):
        disabled_reason = str(dis["reason"])

    cmd = _norm_command(raw.get("command")) if raw.get("command") is not None else None
    edit = normalize_workspace_edit(raw.get("edit"))

    return {
        "title": str(raw["title"]),
        "kind": str(raw["kind"]) if isinstance(raw.get("kind"), str) else None,
        "is_preferred": bool(raw.get("isPreferred", False)),
        "disabled_reason": disabled_reason,
        "edit": edit,
        "command": cmd,
        "data_token": raw.get("data"),
        "resolved": False,
    }


def normalize_code_actions_result(raw: Any) -> List[ActionCandidate]:
    """
    Normalize (Command | CodeAction)[] | null into a stable list.
    Disambiguation rule:
      - If 'command' is a string => Command
      - Else => CodeAction
    """
    if raw is None:
        return []
    if not _is_list(raw):
        return []

    out: List[ActionCandidate] = []
    for item in raw:
        if not _is_dict(item):
            continue

        # Command has command: string
        if isinstance(item.get("command"), str) and isinstance(item.get("title"), str):
            cmd = _norm_command(item)
            if cmd:
                out.append({"action_type": "command", "command": cmd})
            continue

        ca = _norm_code_action(item)
        if ca:
            out.append({"action_type": "code_action", "code_action": ca})

    return out


def merge_code_action_resolve(base: CodeActionNorm, resolved_raw: Any) -> CodeActionNorm:
    """
    Merge codeAction/resolve into a CodeActionNorm.
    Per spec, resolve is commonly used to compute edit lazily, and servers shouldn't
    alter existing attributes during resolve. We'll only fill missing edit/command,
    and record a note if suspicious changes would occur.
    """
    merged: CodeActionNorm = dict(base)
    if not _is_dict(resolved_raw):
        return merged

    # Only fill edit if base edit is empty (no changes) and resolved provides edits.
    base_we = merged.get("edit") or normalize_workspace_edit(None)
    resolved_we = normalize_workspace_edit(resolved_raw.get("edit"))
    if (not base_we.get("text_document_edits") and not base_we.get("resource_operations")) and (
        resolved_we.get("text_document_edits") or resolved_we.get("resource_operations")
    ):
        merged["edit"] = resolved_we

    # Fill command if missing
    if merged.get("command") is None and resolved_raw.get("command") is not None:
        cmd = _norm_command(resolved_raw.get("command"))
        if cmd:
            merged["command"] = cmd

    merged["resolved"] = True
    return merged


def build_code_actions_finding(*, range_queried: Range, raw_actions: Any) -> Finding:
    actions = normalize_code_actions_result(raw_actions)
    summary = {
        "count": len(actions),
        "code_actions": sum(1 for a in actions if a.get("action_type") == "code_action"),
        "commands": sum(1 for a in actions if a.get("action_type") == "command"),
        "disabled": sum(
            1 for a in actions
            if a.get("action_type") == "code_action" and (a.get("code_action") or {}).get("disabled_reason")
        ),
        "with_edit": sum(
            1 for a in actions
            if a.get("action_type") == "code_action"
            and (
                (a.get("code_action") or {}).get("edit", {}).get("text_document_edits")
                or (a.get("code_action") or {}).get("edit", {}).get("resource_operations")
            )
        ),
    }
    return make_finding(
        category="refactor.actions",
        title="Available refactor/code actions",
        kind="pyrefly.lsp.code_actions",
        data={"range_queried": range_queried, "actions": actions, "summary": summary},
    )


def build_code_action_resolve_finding(*, resolved_actions: List[CodeActionNorm]) -> Finding:
    return make_finding(
        category="refactor.actions.resolve",
        title="Resolved code action details",
        kind="pyrefly.lsp.code_action.resolve",
        data={
            "resolved": resolved_actions,
            "summary": {
                "resolved_count": len(resolved_actions),
                "with_edit": sum(
                    1 for a in resolved_actions
                    if a.get("edit", {}).get("text_document_edits") or a.get("edit", {}).get("resource_operations")
                ),
            },
            "resolve_contract": {
                "typical_lazy_field": "edit",
                "note": "Resolve is commonly used to compute edit lazily; servers shouldn't mutate existing attributes."
            },
        },
    )


# ----------------------------
# Infer diff normalization (unified diff -> structured)
# You get this by running pyrefly infer in a temp dir then `git diff --no-index`.
# ----------------------------

class DiffHunk(TypedDict, total=False):
    old_start: int
    old_count: int
    new_start: int
    new_count: int
    header: str
    lines: List[str]

class FileDiff(TypedDict, total=False):
    old_path: str
    new_path: str
    hunks: List[DiffHunk]

_DIFF_HEADER_RE = re.compile(r"^diff --git a/(.+) b/(.+)$")
_HUNK_RE = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@(.*)$")

def normalize_unified_diff(diff_text: str) -> Dict[str, Any]:
    files: List[FileDiff] = []
    cur: Optional[FileDiff] = None
    cur_hunk: Optional[DiffHunk] = None

    insertions = 0
    deletions = 0

    for line in diff_text.splitlines():
        m = _DIFF_HEADER_RE.match(line)
        if m:
            # close previous
            if cur is not None:
                if cur_hunk is not None:
                    cur.setdefault("hunks", []).append(cur_hunk)
                files.append(cur)
            cur = {"old_path": m.group(1), "new_path": m.group(2), "hunks": []}
            cur_hunk = None
            continue

        if cur is None:
            continue

        hm = _HUNK_RE.match(line)
        if hm:
            if cur_hunk is not None:
                cur["hunks"].append(cur_hunk)
            cur_hunk = {
                "old_start": int(hm.group(1)),
                "old_count": int(hm.group(2) or "1"),
                "new_start": int(hm.group(3)),
                "new_count": int(hm.group(4) or "1"),
                "header": (hm.group(5) or "").strip(),
                "lines": [],
            }
            continue

        if cur_hunk is not None:
            cur_hunk["lines"].append(line)
            if line.startswith("+") and not line.startswith("+++"):
                insertions += 1
            elif line.startswith("-") and not line.startswith("---"):
                deletions += 1

    # flush last
    if cur is not None:
        if cur_hunk is not None:
            cur.setdefault("hunks", []).append(cur_hunk)
        files.append(cur)

    return {
        "file_diffs": files,
        "summary": {
            "files_changed": len(files),
            "insertions": insertions,
            "deletions": deletions,
        },
    }


def build_infer_diff_finding(*, scope: Dict[str, Any], diff_text: str) -> Finding:
    normed = normalize_unified_diff(diff_text)
    return make_finding(
        category="infer.annotation_patch",
        title="Pyrefly infer annotation patch (diff)",
        kind="pyrefly.cli.infer.diff",
        data={
            "scope": scope,
            "diff_unified": diff_text,
            "diff_structured": normed,
            "notes": [
                "Pyrefly recommends running infer in small batches and manually reviewing changes.",
            ],
        },
    )
```

---

### Why this satisfies “no unions leak”

Every LSP union mentioned in the spec is normalized into a stable object:

* `prepareRename` union → `PrepareRenameNorm` with booleans + optional fields (never raw union) ([Microsoft GitHub][1])
* `rename` / `willRenameFiles` → `WorkspaceEditNorm` with `text_document_edits: []` + `resource_operations: []` even when `null` ([Microsoft GitHub][1])
* `WorkspaceEdit` may include file ops and must preserve order; module keeps a distinct ordered `resource_operations` list and flags order sensitivity ([Microsoft GitHub][1])
* `codeAction` union `(Command|CodeAction)[]|null` → list of `ActionCandidate` records; resolve merges only the lazy `edit` field ([Microsoft GitHub][1])

---

If you want, I can also provide a tiny **“apply plan” helper** (order-preserving application of `resource_operations` then `text_document_edits`) that matches the WorkspaceEdit ordering rules in the spec. ([Microsoft GitHub][1])

[1]: https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/ "Specification"
[2]: https://pyrefly.org/en/docs/autotype/ "Infer | Pyrefly"

## G) Project/environment metadata that makes outputs interpretable

Everything your agents get from Pyrefly (hover/types, definitions, call graphs, etc.) is downstream of two “hidden” decisions:

1. **How Pyrefly resolves imports** (what file a module name maps to).
2. **What environment/config Pyrefly actually used** (project root, search paths, interpreter/site-packages, per-file overrides).

If you don’t persist these, agents will see “mysterious drift” (same query, different answers) and won’t be able to explain *why*.

---

# G1) Import resolution context

### `imports.resolution_order`

**Definition (agent semantic):** the ordered list of import “groups” Pyrefly searches for **absolute imports**, plus what happens when all fail.

**Pyrefly’s absolute-import order is explicitly:**

1. search path
2. `typeshed`
3. fallback search path
4. site package path
5. import error ([Pyrefly][1])

**What to persist (practical shape):**

```json
imports.resolution_order = [
  {"group":"search_path", "source":"cli+config+heuristics"},
  {"group":"typeshed", "source":"bundled stdlib stubs"},
  {"group":"fallback_search_path", "source":"heuristic directory walk"},
  {"group":"site_package_path", "source":"config + interpreter query"},
  {"group":"error", "source":"MissingImport/UntypedImport/etc"}
]
```

**So what (why agents care):**

* This order explains why Pyrefly might resolve a module to **project code** vs **stdlib stubs** vs **third-party site-packages**.
* It’s the root cause behind “why does `definition` jump there?” and “why does hover show this type?” because those features depend on which module file was selected.

---

### `imports.relative_resolution_rules`

**Definition:** rules for resolving **relative imports** (leading dots) as a directory-walk in the package hierarchy.

Pyrefly states: relative imports are resolved relative to the importing file; one dot = current directory, more dots walk upward. ([Pyrefly][1])

**What to persist:**

```json
imports.relative_resolution_rules = {
  "base": "importing_file_directory",
  "dot_semantics": {".":"current_dir", "..":"parent_dir", "...":"grandparent_dir"},
  "walk_up": true
}
```

**So what:**

* Relative imports are *path-sensitive*. If your tool is analyzing a file outside the project root Pyrefly chose, the same `from ..x import y` can resolve differently (or fail). Capturing this makes “why import failed” explainable.

---

### Extra import semantics that matter for “explainability” (worth persisting even though you didn’t list them)

#### Stub-vs-source preference inside each group

Within each searched group, Pyrefly describes a **two-pass** matching process (first stubs, then sources) and it prefers returning `.pyi` files or “regular packages” (`__init__.py`/`__init__.pyi`) when found. ([Pyrefly][1])

**So what:**

* This directly changes the “surface” agents see:

  * Stubs can expose methods/types that aren’t obvious in runtime code.
  * Or stubs can be *stricter* than runtime behavior.
* If you’re using Pyrefly outputs to drive refactors, knowing “this came from stub pass vs source pass” helps avoid edits that satisfy stubs but break runtime.

#### `-stubs` packages and stub precedence

Pyrefly defines:

* stub files = `.pyi`
* stub packages = `<name>-stubs` that override `.py`/`.pyi` in the non-stubs package, and Pyrefly loads typing by searching for `-stubs` first, then `.pyi`, then `.py`. ([Pyrefly][1])

**So what:**

* Explains why type info may come from a separate distribution (`pandas-stubs`) rather than `pandas` itself.
* Agents need this to interpret “type contract vs runtime behavior” and to avoid editing the wrong artifact.

#### Bundled third-party stubs behavior

Pyrefly documents special rules for bundled third-party stubs, including a decision tree that depends on whether a config file is present, whether the package is installed, and whether user-installed stubs exist. ([Pyrefly][1])

**So what:**

* Two machines with different “installed stubs” state can get different typing surfaces for the same import. Persisting this context is essential for reproducibility.

#### Editable installs and import hooks (big source of “why can’t it find my code?”)

Pyrefly warns that editable installs using **import hooks** are incompatible with static tools like Pyrefly (because resolving requires executing code), and recommends path-based `.pth` files; otherwise Pyrefly may not locate/analyze sources, leading to incomplete analysis. ([Pyrefly][1])

**So what:**

* If an agent sees “missing source / incomplete types” for an editable dependency, this is often the reason.
* Persist a boolean like `imports.editable_install_hook_risk=true` whenever you detect an editable install in site-packages and missing sources.

---

# G2) “What config did Pyrefly actually use here?”

This is the **reproducibility snapshot**. Without it, you can’t explain why two runs differ.

### The recommended way to capture it: `pyrefly dump-config`

Pyrefly explicitly provides a `dump-config` command that dumps the import-related config options used **for each file it is checking**, and says to use it by replacing `check` with `dump-config` in your CLI invocation. ([Pyrefly][1])
The configuration docs also note you can run `pyrefly dump-config [<file>...]` to see what Pyrefly finds for interpreter + site-package-path and other config-debugging features. ([Pyrefly][2])

**So what:** `dump-config` is the “ground truth” of Pyrefly’s resolved view after applying:

* CLI flags precedence
* config finding
* search-path heuristics
* interpreter auto-configuration
* sub-config overrides

---

## `env.python_platform`, `env.python_version`, `env.site_package_path`

**Definition:** the environment values Pyrefly uses for typechecking and resolution.

Pyrefly’s environment auto-configuration (unless `skip-interpreter-query`) queries an interpreter and uses:

* `python-platform` from `sys.platform`
* `python-version` from `sys.version_info[:3]`
* `site-package-path` from `site.getsitepackages() + [site.getusersitepackages()]` ([Pyrefly][2])

It also appends interpreter-derived site-package paths onto configured site-package-path. ([Pyrefly][2])

**So what:**

* `python_platform` / `python_version` can change conditional typing branches and thus member surfaces and diagnostics.
* `site_package_path` controls which third-party packages exist and which stubs are visible; it’s a primary cause of “works on my machine” differences.

---

## `env.search_path` (including heuristics)

**Definition:** the ordered list of project roots Pyrefly searches for *project* imports (highest precedence in import order).

Pyrefly defines `search-path` as roots used for imports and says it has the **highest precedence** (before `typeshed` and site-package-path). ([Pyrefly][2])
Pyrefly also documents search path heuristics (when `disable-search-path-heuristics` is not set):

1. add the import root to the end of search-path
2. if no config is found, add each directory from the file up to `/` as fallback search path ([Pyrefly][2])

Import-resolution doc also explains the import root choices (`src/`, parent with `__init__`, or config dir). ([Pyrefly][1])

**So what:**

* This is *the* explanation for “why does `import a.b.c` resolve to `/x/y/z/a/b/c` vs somewhere else?”
* It determines whether a module is treated as “project code” (rich typing) vs “third-party / missing” (Any-like behavior).

---

## `env.config_file_used`

**Definition:** which configuration file (if any) anchored Pyrefly’s project root and options for this file.

Pyrefly’s config-finding rules:

1. if `--config/-c` is provided, use that file’s directory as project root
2. else upward search for `pyrefly.toml`, `pyproject.toml`, `setup.py`, `mypy.ini`, `pyrightconfig.json` and use the directory it’s found in
3. if none found, it still attempts import resolution by walking up the tree looking for a matching import root ([Pyrefly][2])
   Only `pyrefly.toml` and `pyproject.toml` are parsed for config options; other files are used as “root markers” to aid import resolution. ([Pyrefly][2])

**So what:**

* Agents can’t trust a path-based interpretation without knowing which directory Pyrefly considered “the project”.
* In monorepos, this is a common source of drift: different roots → different imports → different types.

---

## `env.subconfig_applied?`

**Definition:** which `[[sub-config]]` entries matched this file and which overrides they contributed.

Pyrefly Sub-Configs:

* override config values for matched file globs (`matches`), can have multiple matching entries, and option selection uses first non-null value in order of appearance. ([Pyrefly][2])
* allowed overrides are limited (errors, replace-imports-with-any, untyped-def-behavior, ignore-errors-in-generated-code). ([Pyrefly][2])

**So what:**

* Two files in the same repo can legitimately have different “coverage” settings (e.g., untyped-def-behavior differs), which changes what LSP features are available and how deep inference goes.
* Agents need the applied SubConfig chain to explain “why hover works here but not there” or “why imports are Any in tests”.

---

# G3) Config knobs that change explainability coverage

These don’t change *what the code does at runtime*, but they change **what Pyrefly can prove and surface**, which directly impacts your agent tool’s richness.

## `coverage.untyped_def_behavior`

**Definition:** policy for functions without parameter/return annotations.

Pyrefly options:

* `check-and-infer-return-type` (default): checks bodies, infers return type
* `check-and-infer-return-any`: checks body, but return type treated as `Any`
* `skip-and-infer-return-any`: skips checking body, return `Any`, and language server features like hover/definition won’t be available there ([Pyrefly][2])

It also notes that inferring return types may require loading/analyzing more modules (esp. site-package paths), impacting performance and “modules analyzed” output. ([Pyrefly][2])

**So what:**

* This is the single biggest knob for “how much semantic context will agents get inside untyped code?”
* If set to skip, your tool should treat missing hover/definition/call graph edges inside those bodies as “policy-induced”, not “tool failure”.

---

## `coverage.project_includes/excludes`

**Definition:** the file selection boundary for what Pyrefly typechecks.

* `project-includes` are glob patterns describing which files to type check; importantly, Pyrefly says this does **not** specify import resolution priority—`search-path` does. ([Pyrefly][2])
* `project-excludes` filter those files; Pyrefly appends defaults unless `disable-project-excludes-heuristics` is set, and defaults include common dirs and anything in site-package-path (even from interpreter) unless it would exclude search-path items. ([Pyrefly][2])

**So what:**

* Defines the “universe” in which references/call graphs are meaningful (if a file is excluded, references might still exist at runtime but won’t appear in Pyrefly’s world).
* Explains why agents might see incomplete dependency graphs (“because those files weren’t in the check set”).

---

# Practical “so what” conclusion for your tool design

If you persist G1–G3 alongside every enriched response, agents can:

* **Explain resolution choices** (“this symbol came from search-path vs typeshed vs site-packages”) and diagnose missing imports using the exact order Pyrefly follows. ([Pyrefly][1])
* **Make results reproducible** by keying caches and golden tests on `(config_file_used, search_path, site_package_path, python_platform/version, subconfigs applied)`. ([Pyrefly][2])
* **Interpret missing context correctly** (e.g., hover missing because `untyped-def-behavior=skip…`, or calls missing because files excluded). ([Pyrefly][2])
* **Debug import issues quickly** by running `pyrefly dump-config` (CLI ground truth). ([Pyrefly][1])

If you want the follow-on artifact (like prior sections): I can produce a **`PyreflyEnvSnapshotV1` schema + a `normalize_dump_config.py` parser** (tolerant of human text output) that fills exactly the fields you listed (`resolution_order`, `relative_rules`, `search_path`, interpreter-derived values, config root, sub-config matches, includes/excludes, and coverage knobs).

[1]: https://pyrefly.org/en/docs/import-resolution/ "Import Resolution | Pyrefly"
[2]: https://pyrefly.org/en/docs/configuration/ "Configuration | Pyrefly"

## H) Diagnostics and correctness signals

Diagnostics are your **safety proof layer**: they tell an agent whether a planned edit changed program meaning (or at least changed the type-checker’s model of meaning), and *where* the risk concentrates.

---

# H1) Live diagnostics in editor

### Source surface

Pyrefly publishes diagnostics to editors via LSP’s **`textDocument/publishDiagnostics`** notification, which carries `uri`, optional `version`, and `diagnostics: Diagnostic[]`. ([Microsoft][1])
Editors don’t “pull” diagnostics by default; the server pushes them. ([Visual Studio Code][2])

### Persist: `diagnostics.items[]`

Think of `diagnostics.items[]` as the normalized form of LSP `Diagnostic[]` (plus the file `uri` and doc version).

LSP’s `Diagnostic` object is defined as:

* `range: Range` (where the message applies)
* `severity?: DiagnosticSeverity` (recommended always provided; if omitted clients treat as Error)
* `code?: integer|string` (error code shown in UI)
* `codeDescription?: { href: URI }` (link)
* `source?: string` (e.g. “pyrefly”)
* `message: string`
* `tags?: DiagnosticTag[]` (`Unnecessary`, `Deprecated`)
* `relatedInformation?: DiagnosticRelatedInformation[]` (other spans + messages)
* `data?: any` (preserved between publishDiagnostics and codeAction) ([Microsoft][1])

#### Your agent-semantic variable definitions

### `diagnostics.items[].span`

**Definition:** the *exact* code region the diagnostic attaches to.
**Source:** `Diagnostic.range`. ([Microsoft][1])
**So what:** lets agents localize edits: “fix here” is a deterministic span, and you can correlate diagnostics to your own byte-span canonical internal keying.

### `diagnostics.items[].severity`

**Definition:** how “bad” the issue is (error vs warning vs info/hint).
**Source:** LSP `DiagnosticSeverity` values: Error=1, Warning=2, Information=3, Hint=4. ([Microsoft][1])
**Pyrefly semantic overlay:** Pyrefly categorizes severity as `ignore/info/warn/error`, where:

* `ignore`: not emitted, no nonzero exit
* `info`: blue in IDE, no nonzero exit
* `warn`: yellow in IDE, causes nonzero exit
* `error`: red in IDE, causes nonzero exit ([pyrefly.org][3])
  **So what:** your tool can treat diagnostics as a **gate**:
* *edit safety gate*: no `error` after applying a refactor plan
* *risk budget*: allow warnings in a migration phase, but block new errors (often via baseline)

### `diagnostics.items[].code_kind`

**Definition:** a stable classifier for the diagnostic, used for suppressions, baselines, trend tracking.
**Source:** LSP `Diagnostic.code` (int|string). ([Microsoft][1])
**Pyrefly semantic overlay:** Pyrefly “error kinds” are slugs like `bad-return`, and every error has exactly one kind; they’re specifically designed to be used in suppression comments (e.g. `# pyrefly: ignore[bad-return]`). ([pyrefly.org][3])
**So what:** **code_kind is your join key** across:

* IDE diagnostics (LSP `code`)
* CLI summary stats (“error types”)
* suppressions (`ignore[slug]`)
* baseline matching (code + column)

### `diagnostics.items[].message`

**Definition:** human-readable explanation of the issue.
**Source:** LSP `Diagnostic.message`. ([Microsoft][1])
**So what:** agents use this to decide *which* edit plan to apply (e.g., signature mismatch → callsite update; bad assignment → annotate or cast; missing import → fix search path or add stubs).

### Strongly recommended extras (high explainability ROI)

Even though you didn’t list them, these are the “diagnostic explainability” multipliers:

* `diagnostics.items[].source` — who emitted it (`pyrefly` vs other). ([Microsoft][1])
* `diagnostics.items[].tags[]` — `Unnecessary` / `Deprecated` signals (good for cleanup waves). ([Microsoft][1])
* `diagnostics.items[].related[]` — other spans that “caused” the error (e.g., name collisions). ([Microsoft][1])
* `diagnostics.items[].code_href` — `codeDescription.href` link to docs. ([Microsoft][1])
* `diagnostics.items[].data` — preserved between diagnostics and code actions. This is the bridge for “quick fix”/refactor workflows. ([Microsoft][1])

### Coverage note (why “no diagnostics” isn’t always “no problems”)

Pyrefly’s IDE can be configured to show type errors only when files are covered by config (`default`), always (`force-on`), never (`force-off`), or only missing-import/source errors. ([pyrefly.org][4])
**So what:** persist a `diagnostics.coverage_mode` snapshot (IDE setting + whether config covered the file) so agents don’t misinterpret silence as correctness.

---

# H2) CLI typecheck + summarization + baselining

## Persist: `diagnostics.summary`

### `pyrefly check --summarize-errors`

Pyrefly documents that `pyrefly check --summarize-errors` runs the type checker and provides **a list of type errors plus a summary of error types**. ([pyrefly.org][5])

**Definition (agent semantic):** a roll-up of your `diagnostics.items[]` suitable for CI gating and regressions.

**Recommended structure:**

* `summary.total_errors / total_warnings / total_info` (and any “ignored” count if you model config-level ignores)
* `summary.by_code_kind[slug] = count` (this is what “error types” should mean in a refactor/migration context) ([pyrefly.org][5])
* `summary.by_file[uri] = {errors, warnings, top_kinds[]}`
* `summary.new_since_baseline = …` (if baseline in effect)

**So what:** agents can say “this refactor reduced `bad-return` by 12 and introduced 0 new `missing-import`” — that’s concrete progress tracking.

---

## Persist: `diagnostics.baseline`

### Baseline file mode

Pyrefly supports a **baseline file** (experimental) where “errors matching the baseline will be ignored and only new errors will be reported.” ([pyrefly.org][6])

Commands:

* Generate/update: `pyrefly check --baseline="<path>" --update-baseline` ([pyrefly.org][6])
* Use baseline: `pyrefly check --baseline="<path>"` or set `baseline = "baseline.json"` in config. ([pyrefly.org][6])

Matching rule (very important for your data model):

> Errors are matched by **file**, **error code**, and **column number**. ([pyrefly.org][6])

Other important semantics:

* Baseline is **project-level** and can’t be overridden in sub-config. ([pyrefly.org][6])
* Errors suppressed by baseline are **still shown in the IDE**. ([pyrefly.org][6])

**Definition (agent semantic):** baseline is a CI/CLI gating mechanism for “no regressions” during migration; it’s not “hide errors everywhere.”

**Recommended persisted shape:**

* `baseline.path`
* `baseline.match_key = (uri, code_kind, column)` (column = `range.start.character` under LSP; store both LSP and byte conversions if you’re byte-canonical) ([pyrefly.org][6])
* `baseline.updated_at`, `baseline.file_hash`
* `baseline.is_active` (whether CLI invoked with baseline or config baseline)

**So what:** baseline enables safe incremental adoption: agents can apply refactors and prove “no *new* errors introduced,” even if the total error count is nonzero.

---

## Persist: `diagnostics.suppressions_applied`

### Suppression comments + suppress workflow

Pyrefly supports:

* inline suppression: `# pyrefly: ignore` (also same-line) ([pyrefly.org][6])
* kind-targeted suppression: `# pyrefly: ignore[bad-return]` ([pyrefly.org][6])
* `type: ignore` is respected ([pyrefly.org][6])
* file-level ignore: `# pyrefly: ignore-errors` ([pyrefly.org][6])

Automation:

* `pyrefly check --suppress-errors` will automatically suppress all type errors (adds ignores). ([pyrefly.org][5])
* Upgrade workflow includes `pyrefly check --remove-unused-ignores` to clean suppressions after formatting / fixes. ([pyrefly.org][5])

**Definition (agent semantic):** a record of “what suppressions did we introduce/remove” so that large code edits remain reviewable and reversible.

**Recommended persisted shape:**

* `suppressions_applied.mode = suppress-errors|remove-unused-ignores|manual`
* `suppressions_applied.files_changed[]`
* `suppressions_applied.added[]` (per file: line, kind slug if present)
* `suppressions_applied.removed[]`
* `suppressions_applied.patch_unified` (diff)
* `suppressions_applied.reason` (e.g., “typecheck migration baseline step”)

**So what:** suppressions are a *workflow tool*, not an end state. Agents need provenance to avoid turning “temporary migration scaffolding” into permanent debt.

---

# The “so what” takeaway for H

If you persist H correctly, your agents can do three things reliably:

1. **Prove edits are safe**
   Run before/after diagnostics and assert no new `error`-severity issues, with severity semantics consistent across CLI/IDE. ([pyrefly.org][3])

2. **Localize risk precisely**
   Each diagnostic has an exact span and optionally related spans; that becomes an edit queue. ([Microsoft][1])

3. **Run migration workflows without losing rigor**
   Baseline files (file+code+column matching) and suppression automation let you adopt typechecking incrementally while still preventing regressions. ([pyrefly.org][6])

[1]: https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/ "Specification"
[2]: https://code.visualstudio.com/api/language-extensions/programmatic-language-features "Programmatic Language Features | Visual Studio Code Extension
API"
[3]: https://pyrefly.org/en/docs/error-kinds/ "Pyrefly Error Kinds | Pyrefly"
[4]: https://pyrefly.org/en/docs/IDE/ "IDE Installation | Pyrefly"
[5]: https://pyrefly.org/en/docs/installation/ "Installation | Pyrefly"
[6]: https://pyrefly.org/en/docs/error-suppressions/ "Pyrefly Error Suppressions | Pyrefly"
