Below is a **section map** for **Rust LSP outputs**, organized by **semantic category + downstream use case**. I’m treating “Rust LSP” as the practical reality: **`rust-analyzer` speaking LSP 3.17**, plus a set of **rust-analyzer LSP extensions** (mostly under `experimental/*`). ([Rust Analyzer][1])

---

## 0) Session + workspace “environment” outputs

These aren’t “per symbol” results; they define *what universe the server thinks exists*.

* **Initialize / server capabilities**: negotiated capability matrix (what features/requests are supported, incl. semantic tokens, inlay hints, etc.). ([Microsoft GitHub][2])
* **Workspace loading status / health**: “is the project loaded?”, “is cargo metadata ready?”, “is analysis partially available?” (often surfaced via rust-analyzer extensions / status). ([Android Git Repositories][3])
* **Workspace-wide refresh notifications** (e.g., inlay-hint refresh) to prompt clients to re-request derived data. ([GitHub][4])

---

## 1) Diagnostics & correctness signals

Primary “is it correct?” channel—also the best substrate for CI-style baselines in editors.

* **Publish diagnostics**: compiler/analysis errors, warnings, notes, fix-its where available (standard LSP diagnostics). ([Microsoft GitHub][2])
* **Related information**: cross-spans that explain an error (e.g., trait mismatch origin). ([Microsoft GitHub][2])
* **Rust-analyzer-specific correctness overlays**: additional lints, macro-expansion-related diagnostics, “unresolved proc-macro” style issues (varies by configuration). ([Rust Analyzer][1])

---

## 2) Symbol identity & navigation outputs

This is the “code intelligence core” for building a symbol graph: *what is this thing, where is it defined, where used*.

* **Go to definition / declaration** (`textDocument/definition`, `textDocument/declaration`)
* **Type definition** (`textDocument/typeDefinition`)
* **Implementation targets** (`textDocument/implementation`)
* **References** (`textDocument/references`)
* **Document symbols** (`textDocument/documentSymbol`)
* **Workspace symbols** (`workspace/symbol`)
* **Call hierarchy** (`textDocument/prepareCallHierarchy`, incoming/outgoing calls)
* **Type hierarchy** (`textDocument/prepareTypeHierarchy`, supertypes/subtypes where supported)

All of these are standard LSP surfaces, and rust-analyzer explicitly advertises go-to-definition / find-references / refactors as first-class features. ([Microsoft GitHub][2])

---

## 3) Hover, signature, and “explain what this is” outputs

These are the “human/agent comprehension” channels.

* **Hover** (`textDocument/hover`): type, docs, trait bounds, inferred info. ([Rust Analyzer][1])
* **Signature help** (`textDocument/signatureHelp`): active parameter, overloads, docs. ([Microsoft GitHub][2])
* **Documentation resolution**: completion/hover may include deferred resolution steps (client requests more detail later). ([Microsoft GitHub][2])

---

## 4) Completion & “possible next actions” outputs

The “what can I type here?” plane.

* **Completion items** (`textDocument/completion`): symbols, snippets, trait methods, imports, module paths. ([Microsoft GitHub][2])
* **Completion item resolve** (`completionItem/resolve`): late-bound docs/types/additional edits. ([Microsoft GitHub][2])

---

## 5) Rename, edits, and refactoring outputs

Where you get *structured edits* you can apply (often the most valuable machine-actionable output besides diagnostics).

* **Rename** (`textDocument/rename`) + **prepareRename**: location legality + edit set. ([Microsoft GitHub][2])
* **Code actions** (`textDocument/codeAction`): quick fixes, assists, refactors; returns **WorkspaceEdit** + optional commands. ([Microsoft GitHub][2])
* **Code action resolve** (`codeAction/resolve`) where supported. ([Microsoft GitHub][2])
* **Execute command** (`workspace/executeCommand`): many rust-analyzer “assists” and project actions ride on commands, including custom ones. ([Microsoft GitHub][2])

---

## 6) Formatting & style outputs

“Make it look right” outputs—usually deterministic and patch-based.

* **Format document** (`textDocument/formatting`)
* **Format range** (`textDocument/rangeFormatting`)
* **On-type formatting** (`textDocument/onTypeFormatting`)

Rust-analyzer advertises integrated formatting (typically via rustfmt integration). ([Rust Analyzer][1])

---

## 7) Semantic highlighting & token classification outputs

This is the “high-fidelity token classification” plane; also useful for agents wanting stable token categories.

* **Semantic tokens** (`textDocument/semanticTokens/*`): token spans + token types/modifiers; supports full/delta/range depending on client/server negotiation. ([Microsoft GitHub][2])

---

## 8) Inlay hints & inline annotations outputs

One of the most “semantic density per byte” channels: types, parameter names, lifetime hints, etc.

* **Inlay hints** (`textDocument/inlayHint`) + **refresh** (`workspace/inlayHint/refresh`). ([GitHub][4])
* Rust-analyzer exposes extensive inlay hints configuration and behavior in its docs/manual ecosystem. ([Rust Analyzer][5])

---

## 9) Folding, selection, and structural editing outputs

Useful for editors and also for “structural context extraction”.

* **Folding ranges** (`textDocument/foldingRange`)
* **Selection range** (`textDocument/selectionRange`)

(These are standard LSP outputs that provide structure without full AST export.) ([Microsoft GitHub][2])

---

## 10) Rust-analyzer-specific “deep Rust” extensions (high leverage)

These are the outputs you *won’t* see in generic LSP servers, and they tend to be the most “Rust-semantic”.

Typical extension families (names vary; rust-analyzer documents extensions under its LSP extensions doc):

* **Macro expansion / expansion views** (e.g., expand macro / show expansion)
* **Runnable discovery** (tests/binaries/bench targets you can run from a cursor location)
* **Cargo-related actions** (reload workspace, run checks, etc.)
* **Server status / diagnostics beyond LSP**

rust-analyzer explicitly documents that these live in LSP **extensions** and are enabled via `experimental` capabilities. ([Android Git Repositories][3])

---

## 11) Configuration & “knob surfaces” as outputs

Not exactly “results,” but critical for reproducibility of outputs.

* rust-analyzer is configured via LSP configuration messages; editor-specific UIs map to these settings. ([Rust Analyzer][5])

---

If you want the next step to mirror what you did for pyrefly: we can take **each section above** and expand it into (a) **exact request/response shapes**, (b) rust-analyzer **extension method names + payload schemas**, and (c) **what to persist** (stable IDs, spans, symbol handles, edit sets) so an agent can build a durable “Rust semantic evidence layer.”

[1]: https://rust-analyzer.github.io/manual.html?utm_source=chatgpt.com "Introduction - rust-analyzer"
[2]: https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/?utm_source=chatgpt.com "Language Server Protocol Specification - 3.17"
[3]: https://android.googlesource.com/toolchain/rustc/%2B/HEAD/src/tools/rust-analyzer/docs/dev/lsp-extensions.md?utm_source=chatgpt.com "LSP Extensions"
[4]: https://github.com/rust-lang/rust-analyzer/issues/13369?utm_source=chatgpt.com "Support for workspace/inlayHint/refresh to ensure inlay ..."
[5]: https://rust-analyzer.github.io/book/configuration.html?utm_source=chatgpt.com "Configuration"

## 0) Session + workspace “environment” outputs (Rust LSP / rust-analyzer)

### 0.1 Initialize handshake + negotiated capability matrix

#### 0.1.1 `initialize` request (client → server)

**Method:** `initialize`
**Payload type:** `InitializeParams` (extends `WorkDoneProgressParams`) ([Microsoft GitHub][1])

Key fields you should treat as *protocol-level contract inputs*:

* `processId: integer | null` (server may exit if parent process is dead) ([Microsoft GitHub][1])
* `clientInfo?: { name: string; version?: string }` ([Microsoft GitHub][1])
* `capabilities: ClientCapabilities` (the whole negotiation surface) ([Microsoft GitHub][1])
* `initializationOptions?: LSPAny` (vendor/config blob; rust-analyzer uses this heavily in real clients) ([Microsoft GitHub][1])
* `workspaceFolders?: WorkspaceFolder[] | null` (initial workspace roots) ([Microsoft GitHub][1])

**Hard protocol ordering constraints (watch outs):**

* `initialize` **may only be sent once**. ([Microsoft GitHub][1])
* If server receives any request/notification before `initialize`, it should error requests with `code: -32002` and drop notifications (except `exit`). ([Microsoft GitHub][1])
* Server **must not** send arbitrary requests/notifications before replying with `InitializeResult` (only a narrow allowlist during initialize). ([Microsoft GitHub][1])

#### 0.1.2 `InitializeResult` response (server → client)

**Response shape:** `{ capabilities: ServerCapabilities, serverInfo?: { name, version? } }` ([Microsoft GitHub][1])

What to persist (agent-side):

* `serverInfo.name/version` (pin server flavor; rust-analyzer vs other) ([Microsoft GitHub][1])
* `capabilities` full blob (feature gating + request routing) ([Microsoft GitHub][1])
* `capabilities.experimental` (critical: rust-analyzer extensions are usually negotiated here) ([Microsoft GitHub][1])
* `capabilities.positionEncoding` (see below) ([Microsoft GitHub][1])

#### 0.1.3 Position encoding (UTF-16 vs UTF-8) is a *schema-level hazard*

* Since LSP 3.17, client can advertise supported encodings via `general.positionEncodings`. ([Microsoft GitHub][1])
* Server picks `ServerCapabilities.positionEncoding`; if omitted, defaults to `utf-16`. ([Microsoft GitHub][1])

**Value:** if you’re persisting spans/locations as evidence for an agent, you must know whether “character offsets” are UTF-16 code units vs another encoding. Misinterpreting this silently corrupts range-based joins.

**Watch outs:**

* “Defaults” happen on both sides: if client doesn’t offer encodings, server can only return `utf-16`; if server omits, assume `utf-16`. ([Microsoft GitHub][1])
* If you also maintain byte spans (recommended), treat LSP positions as view-layer + convert carefully.

#### 0.1.4 `initialized` notification (client → server)

**Method:** `initialized`
Sent after client receives `InitializeResult` but before it sends anything else. Server may use it to dynamically register capabilities. ([Microsoft GitHub][1])

**Value:** “end of handshake” marker; safe point to start higher-cost requests.

---

### 0.2 Workspace loading status / health (“what universe is ready?”)

Rust-analyzer has **explicit, machine-readable** workspace health via an extension.

#### 0.2.1 `experimental/serverStatus` (server → client) — persistent health channel

**Capability opt-in (client):** `experimental: { serverStatusNotification: boolean }` ([Android Git Repositories][2])
**Method:** `experimental/serverStatus`
**Type:** notification
**Payload:**

```ts
interface ServerStatusParams {
  health: "ok" | "warning" | "error";
  quiescent: boolean;
  message?: string;
}
```

([Android Git Repositories][2])

**Semantics (treat as authoritative “workspace ready-ish” signal):**

* `health`

  * `"ok"` = “completely functional”
  * `"warning"` = partially functional; “some results might be wrong … missing dependencies”
  * `"error"` = not functional; “most results will be incomplete or wrong”
* `quiescent: boolean` = whether background work is still pending that could change status (downloads, indexing, etc.) ([Android Git Repositories][2])

**Value for an LLM programming agent:**

* Hard gate expensive or correctness-sensitive queries:

  * If `health != ok`, mark downstream semantic facts as **low confidence**.
  * If `quiescent == true`, your cached “environment facts” are likely stable; if `false`, expect churn.
* Use `message` as a user-facing / agent-facing root-cause string (e.g., “missing toolchain”, “cargo metadata pending”).

**Watch outs:**

* Spec explicitly says clients may ignore; don’t assume it exists. Only use if you negotiated it. ([Android Git Repositories][2])
* Clients are “discouraged but allowed” to use `health` to decide whether to send requests; practical pattern: *send cheap requests, delay expensive ones*. ([Android Git Repositories][2])

#### 0.2.2 `rust-analyzer/reloadWorkspace` (client → server) — force cargo metadata re-eval

**Method:** `rust-analyzer/reloadWorkspace`
**Request:** `null` → **Response:** `null`
**Meaning:** re-executes `cargo metadata` to reload project info. ([Android Git Repositories][2])

**Value:**

* Deterministic “I changed Cargo.toml / workspace structure” lever.
* Useful after programmatic edits (agent adds dependency, modifies features, changes workspace members).

**Watch outs:**

* This is **not** standard LSP; must be supported by server + client needs to call it explicitly. ([Android Git Repositories][2])
* Reloading can invalidate many derived results; treat prior symbol locations/diagnostics as stale until status becomes quiescent again.

#### 0.2.3 `rust-analyzer/analyzerStatus` (client → server) — debug-grade snapshot

**Method:** `rust-analyzer/analyzerStatus`
**Request payload:** `{ textDocument?: TextDocumentIdentifier }`
**Response:** `string` (“internal status message, mostly for debugging purposes”) ([Android Git Repositories][2])

**Value:**

* Human/agent troubleshooting dump when serverStatus says `warning/error` but you need more detail.
* Can be logged into run artifacts (help reproduce “why did the agent get incomplete results?”).

**Watch outs:**

* Output is free-form text; don’t build strict parsers.
* Expect churn across rust-analyzer versions.

---

### 0.3 Workspace-wide refresh notifications (“re-request derived overlays”)

These are **server → client requests** that tell the client: “throw away cached overlays and ask me again”.

#### 0.3.1 Inlay hints refresh

**Method:** `workspace/inlayHint/refresh`
**Params:** none → **Result:** void
**Client capability gate:** `workspace.inlayHint.refreshSupport?: boolean` ([Microsoft GitHub][1])

**Value:**

* Makes inlay hints “eventually consistent” after workspace load or config changes.
* Avoids clients polling `textDocument/inlayHint`.

**Watch outs:**

* Global + potentially expensive: forces refresh of all displayed inlay hints; spec says “use with absolute care”. ([Microsoft GitHub][1])
* Real-world symptom: early `textDocument/inlayHint` can error during initialization; refresh after load is the clean fix pattern. ([GitHub][3])

#### 0.3.2 Semantic tokens refresh

**Method:** `workspace/semanticTokens/refresh`
**Params:** none → **Result:** void
**Client capability gate:** `workspace.semanticTokens.refreshSupport?: boolean` ([Microsoft GitHub][1])

**Value:** refresh semantic highlighting after project-wide changes (cfg flags, features, dependency graph changes).

**Watch outs:** same “global refresh / use with care” guidance applies. ([Microsoft GitHub][1])

#### 0.3.3 Code lens refresh (often coupled to workspace changes)

**Method:** `workspace/codeLens/refresh`
**Client capability gate:** `workspace.codeLens.refreshSupport?: boolean` ([Microsoft GitHub][1])

**Value:** re-materialize code lenses after config/workspace changes.

**Watch outs:** global refresh; client may delay recomputation for non-visible editors. ([Microsoft GitHub][1])

---

### 0.4 Minimal “agent-ready” persistence contract for this section

Persist these environment records as a stable header for any subsequent rust-analyzer-derived facts:

* `env.serverInfo` (name/version) ([Microsoft GitHub][1])
* `env.capabilities.server` + `env.capabilities.experimental` ([Microsoft GitHub][1])
* `env.positionEncoding` + `env.client.positionEncodings` ([Microsoft GitHub][1])
* `env.serverStatus` stream (latest `{health, quiescent, message}`) ([Android Git Repositories][2])
* `env.workspaceRefreshEvents` (timestamps of refresh requests received: inlayHint/semanticTokens/codeLens) ([Microsoft GitHub][1])
* `env.workspaceReloadInvocations` (when you called `rust-analyzer/reloadWorkspace`) ([Android Git Repositories][2])

If you want the next deep dive: the natural follow-on is **how to turn `serverStatus + quiescent` into a deterministic gating policy** for high-cost requests (references, rename, codeAction, macro expansion) without flaking during initialization.

[1]: https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/ "Specification"
[2]: https://android.googlesource.com/toolchain/rustc/%2B/HEAD/src/tools/rust-analyzer/docs/dev/lsp-extensions.md "LSP Extensions"
[3]: https://github.com/rust-lang/rust-analyzer/issues/13369 "Support for workspace/inlayHint/refresh to ensure inlay hints are correctly shown in editors · Issue #13369 · rust-lang/rust-analyzer · GitHub"

## 1) Diagnostics & correctness signals (Rust LSP / rust-analyzer)

### 1.1 Push diagnostics (standard LSP): `textDocument/publishDiagnostics`

#### 1.1.1 Method + payload

**Method:** `textDocument/publishDiagnostics` (server → client, notification) ([Microsoft GitHub][1])
**Params:** `PublishDiagnosticsParams`

* `uri: DocumentUri` (which document the diagnostics apply to)
* `version?: integer` (optional; ties diagnostics to a specific document version)
* `diagnostics: Diagnostic[]` ([Microsoft GitHub][1])

**Value**

* This is the canonical “as-you-edit” correctness channel every editor/LSP client understands.
* If `version` is present, you can treat diagnostics as *versioned evidence* and avoid mixing stale vs current reports (critical for agent pipelines that snapshot diagnostics). ([Microsoft GitHub][1])

**Watch outs**

* **Staleness:** If you don’t respect `version` (or it’s absent), you can easily persist diagnostics that correspond to an earlier buffer state. LSP has explicit `versionSupport` capability for a reason. ([Microsoft GitHub][1])
* **Clearing semantics:** servers “replace the full set” by sending a new list; clients must also handle clearing when list becomes empty. Some real-world rust-analyzer regressions have caused “diagnostics not clearing until restart” style behavior—treat client presentation as potentially buggy and prefer server-side trace logs for ground truth. ([GitHub][2])

#### 1.1.2 Client capability gates you should record

`textDocument.publishDiagnostics: PublishDiagnosticsClientCapabilities` includes: ([Microsoft GitHub][1])

* `relatedInformation?: boolean` (client will accept cross-span “why” edges) ([Microsoft GitHub][1])
* `tagSupport` (diagnostic tags such as “unnecessary”, “deprecated”) ([Microsoft GitHub][1])
* `versionSupport?: boolean` ([Microsoft GitHub][1])
* `codeDescriptionSupport?: boolean` (linkable doc for a diagnostic) ([Microsoft GitHub][1])
* `dataSupport?: boolean` (**preserve Diagnostic.data** between publishDiagnostics and codeAction) ([Microsoft GitHub][1])

**Value**

* For an LLM agent, `dataSupport` is the linchpin for “diagnostic → fix action” workflows: you can carry opaque server metadata forward into `textDocument/codeAction` calls. ([Microsoft GitHub][1])

---

### 1.2 Diagnostic object schema (what you actually persist)

#### 1.2.1 Core fields

`Diagnostic` (LSP) is “only valid in the scope of a resource” and includes at minimum: ([Microsoft GitHub][1])

* `range: Range` (span) ([Microsoft GitHub][1])
* (plus common fields you should treat as first-class evidence:)

  * `severity?: DiagnosticSeverity`
  * `code?: integer | string`
  * `source?: string`
  * `message: string`
  * `tags?: DiagnosticTag[]` (if supported)
  * `codeDescription?: { href: URI }` (if supported)

*(The spec defines these under the `Diagnostic` interface; persist them even if optional so you can join/aggregate consistently.)* ([Microsoft GitHub][1])

**Value**

* `source` is how you separate “rust-analyzer native” vs “cargo check / clippy / flycheck” vs other producers (client UI often mixes them).
* `code` becomes your stable baseline key (where available) for suppressions/waivers.

#### 1.2.2 `relatedInformation` = cross-span causal links

`Diagnostic.relatedInformation?: DiagnosticRelatedInformation[]` is explicitly intended to mark *multiple locations* relevant to the message (e.g., collisions, origins). ([Microsoft GitHub][1])

**Value**

* This is your “error explanation graph”:

  * primary diagnostic span = “symptom”
  * related spans = “causes / competing definitions / trait mismatch origin / where bound was introduced”
* For agent prompting, relatedInformation is often more valuable than the single-line message.

**Watch outs**

* Only send/expect it if client advertised `textDocument.publishDiagnostics.relatedInformation`. ([Microsoft GitHub][1])
* Related spans can land in other files; your persistence layer must treat them as separate `(uri, range)` anchors.

#### 1.2.3 `data` passthrough for “diagnostic → codeAction” bridging

`Diagnostic.data?: LSPAny` is **preserved** between `publishDiagnostics` and `textDocument/codeAction` when clients support it. ([Microsoft GitHub][1])

**Value**

* Enables high-fidelity quick-fix selection without re-deriving context:

  * store opaque `data`
  * when asking for code actions, pass the same diagnostic object (or preserve `data`)
  * server can return targeted fixes

**Watch outs**

* Don’t serialize/interpret `data` beyond “opaque blob”. Treat it as server-private.

---

### 1.3 Pull diagnostics (LSP 3.17): `textDocument/diagnostic`, `workspace/diagnostic`, refresh

Even if your current client stack is “push diagnostics”, LSP 3.17 introduced **pull** to give clients control over *when* and *which docs* are computed. ([Microsoft GitHub][1])

#### 1.3.1 `textDocument/diagnostic` (client → server)

**Method:** `textDocument/diagnostic` ([Microsoft GitHub][1])
**Value**

* Deterministic “compute now for this doc version” control (good for agent workflows that want to re-check after edits without waiting for background push).

**Watch outs**

* Only issued if server registers support; many client setups won’t use pull.

#### 1.3.2 `workspace/diagnostic` (client → server)

**Method:** `workspace/diagnostic` ([Microsoft GitHub][1])
Includes `previousResultIds` so servers can return “unchanged” reports for documents. ([Microsoft GitHub][1])

**Value**

* Whole-workspace correctness snapshot (good for “CI-like baseline” capture in-editor).

**Watch outs**

* LSP defines precedence rules if you mix document+workspace pulls (newer doc version wins; document pull wins over workspace pull). ([Microsoft GitHub][1])

#### 1.3.3 Global diagnostic refresh

**Method:** `workspace/diagnostic/refresh` (server → client request) ([Microsoft GitHub][1])
**Value:** server tells client “re-pull everything; project-wide change detected.”
**Watch out:** global + expensive; spec says “use with absolute care.” ([Microsoft GitHub][1])

---

### 1.4 rust-analyzer correctness overlays (configuration-driven producers)

Rust-analyzer diagnostics are typically a **composition** of:

* **native rust-analyzer diagnostics** (fast, incremental)
* **cargo-check-based diagnostics** (“check on save” / flycheck-style external process)
* optional **clippy** as check command (more lints, often slower/noisier)

#### 1.4.1 Native diagnostics toggles

Config keys: ([Rust Analyzer][3])

* `rust-analyzer.diagnostics.enable` (default `true`): “Show native rust-analyzer diagnostics.” ([Rust Analyzer][3])
* `rust-analyzer.diagnostics.experimental.enable` (default `false`): “experimental diagnostics” with potentially more false positives. ([Rust Analyzer][3])
* `rust-analyzer.diagnostics.disabled: string[]`: disable specific rust-analyzer diagnostic kinds. ([Rust Analyzer][3])
* `rust-analyzer.diagnostics.styleLints.enable` (default `false`): “additional style lints.” ([Rust Analyzer][3])

**Value**

* Lets you define a “signal policy” for agents:

  * baseline only on native diagnostics for fast, stable feedback
  * optionally include experimental/style lints for deeper cleanup passes

**Watch outs**

* Experimental diagnostics can be meaningfully noisier; if you persist them, tag them with `source=native-experimental` to avoid polluting “must-fix” queues. ([Rust Analyzer][3])

#### 1.4.2 Check-on-save (cargo diagnostics) toggles

Config keys: ([Rust Analyzer][3])

* `rust-analyzer.checkOnSave` (default `true`): “Run the check command for diagnostics on save.” ([Rust Analyzer][3])
* `rust-analyzer.check.command` (default `"check"`): cargo subcommand used. ([Rust Analyzer][3])
* `rust-analyzer.check.overrideCommand`: full override (must emit JSON; include `--message-format=json` or similar). ([Rust Analyzer][3])

**Value**

* This is where “compiler-grade” truth tends to come from (trait solving, borrow checking, etc.)—but it’s gated by saving and process execution.

**Watch outs**

* Concurrency/latency: external `cargo check` can block other cargo builds or slow iteration (common complaint pattern). ([GitHub][4])
* Output format hazards: rust-analyzer’s flycheck pipeline has had issues around how rustc/cargo emit JSON diagnostics to stdout vs stderr; if your agent depends on these diagnostics, prefer “known-good” command configurations and log capture. ([GitHub][5])

#### 1.4.3 Clippy as diagnostic source (more lints, more footguns)

You can point check at clippy via:

* `rust-analyzer.check.command: "clippy"` (common setup) ([The Rust Programming Language Forum][6])

**Value**

* Great for “cleanup passes” and finding non-compiler-hard errors (style/perf/idioms).

**Watch outs**

* Real-world instability/perf: some users report unreliable or “not real” errors + slow behavior when `check.command=clippy` on large repos; treat clippy diagnostics as a separate tier, not your only truth source. ([GitHub][7])

---

### 1.5 Proc-macro / macro-expansion related diagnostic failure modes (high-frequency Rust LSP pain)

These are *not just* “normal type errors” — they are “analysis universe is incomplete” signals.

#### 1.5.1 Unresolved proc-macro / derive macro failure signatures

Rust-analyzer commonly surfaces errors when proc-macro expansion is unavailable or misconfigured (e.g., “unresolved-proc-macro” across derive usage). ([GitHub][8])

Relevant configuration knobs:

* `rust-analyzer.procMacro.enable` (default `true`) ([Rust Analyzer][3])
* `rust-analyzer.procMacro.ignored` (ignore specific macros) ([Rust Analyzer][3])
* cargo build scripts (implied by procMacro.enable; build scripts config is central to macro correctness) ([Rust Analyzer][3])

**Value**

* For agents: these diagnostics mean “semantic model is degraded.” Gate symbol-resolution-heavy tasks (rename, impl navigation, reference graphs) until proc-macro is healthy.

**Watch outs**

* “False unresolved imports/idents” can flood the problem list if macros generate types; your baseline should detect macro-related diagnostic codes and classify them separately (so they don’t drown real compiler errors). ([GitHub][8])

---

### 1.6 Path remapping correctness for diagnostics (repro + workspace portability)

#### 1.6.1 `rust-analyzer.diagnostics.remapPrefix`

Config: “Map of prefixes to be substituted when parsing diagnostic file paths” and should be the reverse mapping of rustc `--remap-path-prefix`. ([Rust Analyzer][3])

**Value**

* Makes diagnostics stable across machines/containers/monorepo subroots by normalizing file paths before they become `(uri, range)` anchors.

**Watch outs**

* `--remap-path-prefix` is purely textual and “last matching wins” when multiple apply—easy to get wrong if you stack remaps. ([Rust Documentation][9])
* Misconfigured remapping can break “click to location” and other tooling; prefer logging + incremental rollout.

---

### 1.7 Agent-ready persistence contract (for this section)

Persist each diagnostic as a record keyed by `(uri, version?, range_start, range_end, source, code?)`:

* `uri`, `version?` ([Microsoft GitHub][1])
* `range`, `severity`, `code`, `source`, `message` ([Microsoft GitHub][1])
* `relatedInformation[]` as separate edges: `(diag_id -> (uri, range, message))` ([Microsoft GitHub][1])
* `tags[]`, `codeDescription.href` (if present / supported) ([Microsoft GitHub][1])
* `data` (opaque) only if client `dataSupport` true ([Microsoft GitHub][1])
* `producer_tier` (derive from config + `source`):

  * `native` / `native-experimental` / `cargo-check` / `clippy` ([Rust Analyzer][3])
* `environment_health_snapshot` (from `experimental/serverStatus` you already captured) so later you can down-rank diagnostics collected during degraded analysis.

If you want the next deep dive in the same style, the clean follow-on is: **“2) Symbol identity & navigation outputs”**, because diagnostics become dramatically more actionable once you can jump from error spans into definition/references/type-definition reliably.

[1]: https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/ "Specification"
[2]: https://github.com/rust-lang/rust-analyzer/issues/17300?utm_source=chatgpt.com "Diagnostics not clearing after fix and save · Issue #17300"
[3]: https://rust-analyzer.github.io/book/configuration.html "Configuration - rust-analyzer"
[4]: https://github.com/rust-lang/rust-analyzer/issues/4616?utm_source=chatgpt.com "Rust analyzer \"cargo check\" blocks debug builds #4616"
[5]: https://github.com/rust-lang/rust-analyzer/issues/14217?utm_source=chatgpt.com "flycheck: rustc diagnostics are not read properly #14217"
[6]: https://users.rust-lang.org/t/how-to-use-clippy-in-vs-code-with-rust-analyzer/41881?utm_source=chatgpt.com "How to use Clippy in VS Code with rust-analyzer?"
[7]: https://github.com/rust-lang/rust-analyzer/issues/19336?utm_source=chatgpt.com "Using clippy as the check command makes rust-analyzer ..."
[8]: https://github.com/rust-analyzer/rust-analyzer/issues/11150?utm_source=chatgpt.com "unresolved-proc-macro error on all proc macro usages"
[9]: https://doc.rust-lang.org/beta/rustc/remap-source-paths.html?utm_source=chatgpt.com "Remap source paths - The rustc book"

## 2) Symbol identity & navigation outputs (Rust LSP / rust-analyzer)

**Core idea:** these endpoints give you the *graph primitives* for a symbol graph:

* **nodes** = symbols / call-hierarchy items / type-hierarchy items
* **edges** = def→decl, ref→def, impl→trait, caller→callee, subtype→supertype
* **anchors** = `(uri, range)` plus (when available) `LocationLink` with origin/target selection ranges

rust-analyzer advertises these as first-class (“go-to-definition”, “find-all-references”, refactorings, etc.). ([Rust Analyzer][1])

---

### 2.0 Capability gates + response shape pitfalls (applies to everything below)

#### 2.0.1 Location vs LocationLink

Many navigation requests can return **either**:

* `Location` (target `uri` + `range`)
* `LocationLink` (adds `originSelectionRange` + `targetSelectionRange` + `targetRange`)

**Value**

* Prefer `LocationLink` when offered: it disambiguates “clicked token span” vs “full definition span” vs “selection span” for precise anchoring and UI jumps.

**Watch outs**

* Clients vary in which form they accept; always read `InitializeResult.capabilities` and persist it as feature gating. ([Microsoft GitHub][2])

#### 2.0.2 Staleness + position encoding

All ranges are LSP positions; encoding is negotiated (often defaults to UTF-16). If you persist positions as evidence, persist `positionEncoding` alongside your facts. ([Microsoft GitHub][2])

#### 2.0.3 “Health gating” is not optional in Rust

If rust-analyzer workspace health is degraded (missing deps/proc-macros), navigation endpoints degrade silently (missing refs/impls). In an agent pipeline: **gate “graph materialization” on workspace health = ok + quiescent** (from the serverStatus extension you already captured). ([Rust Analyzer][1])

---

## 2.1 Go to definition / declaration

### 2.1.1 `textDocument/definition`

**Method:** `textDocument/definition` (request) ([Microsoft GitHub][2])
**Params:** `TextDocumentPositionParams` (`textDocument`, `position`) ([Microsoft GitHub][2])
**Result:** `Location | Location[] | LocationLink[] | null` (depending on server/client) ([Microsoft GitHub][2])

**Value**

* Defines the canonical “symbol → defining location” edge.
* When returning multiple results, treat them as a disjunction: overloads, trait items, macro-generated expansions, etc.

**Watch outs**

* **Macro context:** definition may jump into expanded/generated code or stdlib source in ways that surprise agents. Don’t assume “definition is in same crate.”
* **Ambiguous tokens:** `position` inside `use` items, re-exports, and method calls can resolve to multiple plausible definitions.

### 2.1.2 `textDocument/declaration`

**Method:** `textDocument/declaration` ([Microsoft GitHub][2])
**Value**

* “Declaration” differs from “definition” in languages where they split; in Rust this is often identical, but still valuable for cross-language uniformity.

**Watch outs**

* Some servers return null or same as definition; treat as optional surface based on server capabilities. ([Microsoft GitHub][2])

---

## 2.2 Type definition

### 2.2.1 `textDocument/typeDefinition`

**Method:** `textDocument/typeDefinition` ([Microsoft GitHub][2])
**Params:** `TextDocumentPositionParams` ([Microsoft GitHub][2])
**Result:** `Location | Location[] | LocationLink[] | null` ([Microsoft GitHub][2])

**Value**

* Produces the “expression/identifier → its type’s definition” edge.
* High leverage for agent comprehension: jump from usage to struct/enum/trait definitions without manual inference.

**Watch outs**

* The “type definition” you get might be:

  * an alias target (`type Foo = …`)
  * a trait object type
  * an associated type definition site
* rust-analyzer exposes “Go to Type Definition” as a hover action (configuration-controlled), so expect this to be common in real client flows. ([Rust Analyzer][3])

---

## 2.3 Implementation targets

### 2.3.1 `textDocument/implementation`

**Method:** `textDocument/implementation` ([Microsoft GitHub][2])
**Params:** `TextDocumentPositionParams` ([Microsoft GitHub][2])
**Result:** `Location | Location[] | LocationLink[] | null` ([Microsoft GitHub][2])

**Value**

* Produces “trait/interface item → implementing methods/types” edges.
* Useful for:

  * trait method call sites → possible concrete impls
  * `trait Foo` → all `impl Foo for T` blocks
  * `dyn Trait` surfaces → candidate implementers (best-effort)

**Watch outs**

* Can be large-fanout. Agents should:

  * cap results
  * rank impls by proximity (same crate/module) or by type inference at call site (if provided)
* Configuration may expose “Implementations” as a hover action; expect high usage. ([Rust Analyzer][3])

---

## 2.4 References

### 2.4.1 `textDocument/references`

**Method:** `textDocument/references` ([Microsoft GitHub][2])
**Params:** `ReferenceParams`:

* `textDocument`, `position`
* `context: { includeDeclaration: boolean }` ([Microsoft GitHub][2])
  **Result:** `Location[] | null` ([Microsoft GitHub][2])

**Value**

* This is your core “usage edges” materializer: `(symbol_id) ← references[]`.
* A symbol graph build typically does:

  1. definition
  2. references(includeDeclaration=false) for usage-only
  3. optionally references(includeDeclaration=true) for “def included” convenience sets

**Watch outs**

* **`includeDeclaration` is frequently mishandled by servers in the wild** (not rust-analyzer-specific). If you rely on usage-only sets, defensively filter out the definition span by comparing against `definition` results. ([GitHub][4])
* rust-analyzer has config to exclude some categories from references:

  * `rust-analyzer.references.excludeImports` (exclude imports from find-all-references)
  * `rust-analyzer.references.excludeTests` (exclude tests from find-all-references and call-hierarchy) ([Rust Analyzer][3])
    Persist these toggles with your outputs; they change the meaning of “all references.”

---

## 2.5 Document symbols

### 2.5.1 `textDocument/documentSymbol`

**Method:** `textDocument/documentSymbol` ([Microsoft GitHub][2])
**Params:** `DocumentSymbolParams` (`textDocument`) ([Microsoft GitHub][2])
**Result:** `DocumentSymbol[] | SymbolInformation[] | null` ([Microsoft GitHub][2])

**Value**

* Produces a per-file “symbol index”:

  * top-level items: structs/enums/traits/fns/modules/impl blocks
  * nested items via `DocumentSymbol.children` when hierarchical symbols are supported
* Great for agent “file summarization without parsing”: stable names + ranges.

**Watch outs**

* Two result shapes:

  * `DocumentSymbol[]` = hierarchical, preferred
  * `SymbolInformation[]` = flat, older
    Your agent should normalize into a canonical internal schema:
  * `symbol_id` (derived hash) + `name` + `kind` + `range` + `selectionRange` + `container` (if any)

---

## 2.6 Workspace symbols

### 2.6.1 `workspace/symbol`

**Method:** `workspace/symbol` ([Microsoft GitHub][2])
**Params:** `WorkspaceSymbolParams` (`query`, optional progress/partial result tokens) ([Microsoft GitHub][2])
**Result:** `SymbolInformation[] | WorkspaceSymbol[] | null` (servers vary) ([Microsoft GitHub][2])

**Value**

* “Global symbol search” entry point for agents:

  * find candidate definitions by name fragment
  * seed follow-up `definition`/`references`/`documentSymbol` calls
* Useful fallback when you don’t know the URI yet.

**Watch outs**

* Query semantics are server-defined; treat it as fuzzy search, not exact.
* Results can be incomplete if indexing isn’t quiescent (tie back to workspace health/quiescent).

---

## 2.7 Call hierarchy

### 2.7.1 Prepare: `textDocument/prepareCallHierarchy`

**Method:** `textDocument/prepareCallHierarchy` ([Microsoft GitHub][2])
**Params:** `CallHierarchyPrepareParams` = `TextDocumentPositionParams` ([Microsoft GitHub][2])
**Result:** `CallHierarchyItem[] | null` ([Microsoft GitHub][2])

### 2.7.2 Expand edges

* **Incoming:** `callHierarchy/incomingCalls` → `CallHierarchyIncomingCall[]` ([Microsoft GitHub][2])
* **Outgoing:** `callHierarchy/outgoingCalls` → `CallHierarchyOutgoingCall[]` ([Microsoft GitHub][2])

**Value**

* Produces a caller/callee graph without full CFG extraction:

  * incoming = “who calls me”
  * outgoing = “who do I call”
* Very useful for agent “impact analysis” and for prioritizing refactor safety.

**Watch outs**

* rust-analyzer can be configured to exclude tests from call-hierarchy (`rust-analyzer.references.excludeTests`). If that’s enabled, your call graph is *not* “whole program.” ([Rust Analyzer][3])
* Dynamic dispatch / trait calls: hierarchy can be approximate (fanout or missing edges).

---

## 2.8 Type hierarchy

### 2.8.1 Prepare: `textDocument/prepareTypeHierarchy`

**Method:** `textDocument/prepareTypeHierarchy` ([Microsoft GitHub][2])
**Params:** `TypeHierarchyPrepareParams` = `TextDocumentPositionParams` ([Microsoft GitHub][2])
**Result:** `TypeHierarchyItem[] | null` ([Microsoft GitHub][2])

### 2.8.2 Expand edges

* **Supertypes:** `typeHierarchy/supertypes` → `TypeHierarchyItem[] | null` ([Microsoft GitHub][2])
* **Subtypes:** `typeHierarchy/subtypes` → `TypeHierarchyItem[] | null` ([Microsoft GitHub][2])

**Value**

* For Rust, this is most meaningful around:

  * trait relationships
  * impl relationships
  * type alias chains / “what is this ultimately”
* Good for agents trying to reason about polymorphism and trait-based architecture.

**Watch outs**

* Not all servers implement it fully; always gate on server capabilities.
* Rust “inheritance” isn’t nominal like OO; expect fewer obvious hierarchies than in Java/C#.

---

## 2.9 rust-analyzer-specific knobs that change semantics (must persist with outputs)

Even though these are “standard LSP surfaces,” rust-analyzer config meaningfully changes what you get:

* `rust-analyzer.references.excludeImports` (reference sets change) ([Rust Analyzer][3])
* `rust-analyzer.references.excludeTests` (reference sets + call hierarchy change) ([Rust Analyzer][3])
* Hover actions toggles for “Go to Type Definition / Implementations / References” affect how often clients request these and what they cache/display (important if you’re observing via a client plugin). ([Rust Analyzer][3])

---

## 2.10 Agent persistence contract for “symbol graph” materialization

For each navigation request, persist:

* `env.capabilities` + `env.positionEncoding` (so ranges remain interpretable) ([Microsoft GitHub][2])
* **Definition/Decl/TypeDef/Impl edges**

  * `origin: (uri, position)` → `targets: LocationLink[] normalized`
* **References edges**

  * `symbol_anchor` (definition LocationLink normalized) + `references: Location[]`
  * store `includeDeclaration` and whether you filtered by def span ([GitHub][4])
* **Document symbols**

  * `DocumentSymbol[]` flattened into `symbol_node` table with `parent_id`
* **Workspace symbols**

  * query + results, then “resolved” follow-up jumps if you do them
* **Call/type hierarchy**

  * `prepare items` as nodes, `incoming/outgoing` or `super/sub` as edges

If you want the next deep dive: **“3) Hover, signature, and explain outputs”** is the highest ROI follow-on because it turns these navigation anchors into **type/bounds/doc payloads** an LLM agent can directly reason over.

[1]: https://rust-analyzer.github.io/manual.html?utm_source=chatgpt.com "Introduction - rust-analyzer"
[2]: https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/?utm_source=chatgpt.com "Language Server Protocol Specification - 3.17"
[3]: https://rust-analyzer.github.io/book/configuration.html?utm_source=chatgpt.com "Configuration"
[4]: https://github.com/typescript-language-server/typescript-language-server/issues/206?utm_source=chatgpt.com "Method 'textDocument/references' does not respect ..."

## 3) Hover, signature, and “explain what this is” outputs (Rust LSP / rust-analyzer)

### 3.1 Hover: `textDocument/hover`

#### 3.1.1 Method + payload

**Method:** `textDocument/hover` (request) ([Microsoft GitHub][1])
**Params:** `HoverParams` (is `TextDocumentPositionParams` + progress) ([Microsoft GitHub][1])

* `textDocument: TextDocumentIdentifier`
* `position: Position`
* optional progress tokens

**Result:** `Hover | null` ([Microsoft GitHub][1])

#### 3.1.2 Hover result schema (persist this exactly)

`Hover` has: ([Microsoft GitHub][1])

* `contents: MarkupContent | MarkedString | MarkedString[]`
* `range?: Range` (the span the hover applies to; omit means “unknown / client decides”)

**Value**

* `contents` is your high-density comprehension payload: type display, inferred type, docs, trait bounds, etc.
* `range` is an anchor for “hover evidence applies to this token span”, useful for stable joins across repeated requests.

#### 3.1.3 Content format negotiation (plaintext vs markdown)

Client advertises `textDocument.hover.contentFormat?: MarkupKind[]` (typically `["markdown", "plaintext"]` or only `["plaintext"]`). Server should honor and respond accordingly. ([Microsoft GitHub][1])

**Watch outs**

* Some clients only render plaintext even if you send markdown; this is **client behavior**, not server correctness (e.g., Visual Studio LSP client advertises `["plaintext"]` and will render hover as plain text). ([Microsoft Learn][2])
* Agents should persist both:

  * the negotiated `contentFormat` from client capabilities
  * the actual `MarkupKind` returned (markdown vs plaintext), so downstream renderers don’t misinterpret.

#### 3.1.4 rust-analyzer hover knobs that materially change payload

These toggles change whether hover includes docs and/or “action links” (implementations, references, etc.):

* `rust-analyzer.hover.documentation.enable` (default `true`) ([Rust Analyzer][3])
* `rust-analyzer.hover.documentation.keywords.enable` (default `true`) ([Rust Analyzer][3])
* `rust-analyzer.hover.actions.*.enable` (examples below) ([Rust Analyzer][3])

  * `rust-analyzer.hover.actions.gotoTypeDef.enable`
  * `rust-analyzer.hover.actions.implementations.enable`
  * `rust-analyzer.hover.actions.references.enable`
  * `rust-analyzer.hover.actions.run.enable`

**Value**

* For an LLM agent, hover becomes a “semantic bundle”:

  * *type + bounds* (what it is)
  * *docs* (what it means)
  * *actions* (what next navigations are relevant)

**Watch outs**

* Hover actions are typically encoded inside the hover text (often as markdown links / commands). That means:

  * if markdown is disabled (client only plaintext), actions may degrade to raw text
  * do not treat hover actions as a stable API surface; treat them as UI hints unless you explicitly map them into separate graph edges.

---

### 3.2 Signature help: `textDocument/signatureHelp`

#### 3.2.1 Method + payload

**Method:** `textDocument/signatureHelp` (request) ([Microsoft GitHub][1])
**Params:** `SignatureHelpParams` includes: ([Microsoft GitHub][1])

* `textDocument: TextDocumentIdentifier`
* `position: Position`
* `context?: SignatureHelpContext`

  * `triggerKind: SignatureHelpTriggerKind` (invoked vs trigger char vs retrigger)
  * `triggerCharacter?: string`
  * `isRetrigger: boolean`
  * `activeSignatureHelp?: SignatureHelp` (for incremental updates)

**Result:** `SignatureHelp | null` ([Microsoft GitHub][1])

#### 3.2.2 SignatureHelp schema (what to persist)

`SignatureHelp` contains: ([Microsoft GitHub][1])

* `signatures: SignatureInformation[]`
* `activeSignature?: uinteger`
* `activeParameter?: uinteger`

`SignatureInformation`: ([Microsoft GitHub][1])

* `label: string` (the full signature display)
* `documentation?: string | MarkupContent`
* `parameters?: ParameterInformation[]`
* `activeParameter?: uinteger` (overrides global activeParameter for this signature)

`ParameterInformation`: ([Microsoft GitHub][1])

* `label: string | [uinteger, uinteger]` (range into the signature label)
* `documentation?: string | MarkupContent`

**Value**

* `activeSignature` + `activeParameter` is the “cursor-to-argument binding” output: the closest you get to an argument-binding oracle without parsing.
* `ParameterInformation.label` being a `[start,end]` range is extremely valuable for machine alignment: you can reliably highlight/extract the parameter substring from the signature label.

#### 3.2.3 Trigger and retrigger characters (server capabilities)

Server advertises `signatureHelpProvider: { triggerCharacters?: string[], retriggerCharacters?: string[] }`. ([Microsoft GitHub][1])

**Value**

* Agents can emulate editor behavior:

  * on `(` or `,` (triggerCharacters), request signatureHelp
  * on further typing (retriggerCharacters), refresh while UI remains “open”

**Watch outs**

* In the wild, some server/client stacks keep `activeParameter` stuck at 0; treat it as “best effort”, not a guarantee. If you need correctness, re-derive active parameter by parsing the call expression and counting commas with nesting awareness. ([GitHub][4])

---

### 3.3 Documentation resolution / deferred resolution (lazy payloads)

This is where you get “cheap first response, richer later”.

#### 3.3.1 Completion resolve: `completionItem/resolve`

**Mechanism:** server can return lightweight completion items, and client later calls `completionItem/resolve` to fill in heavy fields (docs, details, additional edits, etc.). This is explicitly part of LSP completion flow; many clients advertise resolve support to avoid up-front cost. ([Microsoft GitHub][1])

**Key fields commonly resolved (pattern, not a guarantee):**

* `documentation`
* `detail`
* `additionalTextEdits` (e.g., auto-import)
* sometimes `textEdit`/`command` metadata

**Value**

* For agents, this is the correct way to get “full docs for a completion candidate” without forcing the server to compute full docs for every item.

**Watch outs**

* Client support is uneven for `additionalTextEdits` and resolve-driven imports; some UIs ignore or mishandle them, so your agent should validate edits before applying (e.g., ensure edits don’t overlap and target current doc version). ([GitHub][5])
* rust-analyzer specifically tracks/implements completion resolve behavior over time; treat it as version-dependent. ([GitHub][6])

#### 3.3.2 Hover “resolution” is not a standard separate endpoint

There is no `hover/resolve` in LSP 3.17; hover is “request again at position” if you want new content. ([Microsoft GitHub][1])

**Practical implication**

* If you want “lazy hover”, you implement it as:

  * request hover only on demand (cursor/hover event)
  * throttle / debounce repeated hover requests
  * treat hover as ephemeral unless you pin it to a document version + range

#### 3.3.3 Agent persistence contract for deferred resolution

Persist two layers for completion+hover comprehension:

* `hover_evidence`:

  * `(uri, version?, range?, positionEncoding, markupKind, contents)`
* `completion_evidence`:

  * initial completion item snapshot (as returned)
  * resolved completion item snapshot (post `completionItem/resolve`)
  * diff of fields that changed (so you know what was “lazy”)

Include the negotiated `hover.contentFormat` (plaintext/markdown) and whether your client supports completion resolve (and which properties it claims it can resolve), so you can explain/diagnose missing docs deterministically. ([Microsoft GitHub][1])

[1]: https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/?utm_source=chatgpt.com "Language Server Protocol Specification - 3.17"
[2]: https://learn.microsoft.com/en-us/answers/questions/5619489/markdown-string-for-hover-%28textdocument-hover%29-in?utm_source=chatgpt.com "Markdown string for Hover (textDocument/hover) in Visual ..."
[3]: https://rust-analyzer.github.io/book/configuration.html?utm_source=chatgpt.com "Configuration"
[4]: https://github.com/microsoft/language-server-protocol/issues/2079?utm_source=chatgpt.com "SignatureHelp.activeParameter not changing · Issue #2079"
[5]: https://github.com/neovim/neovim/issues/12310?utm_source=chatgpt.com "LSP: Support for `additionalTextEdits` in `CompletionItem`"
[6]: https://github.com/rust-lang/rust-analyzer/issues/6366?utm_source=chatgpt.com "LSP 3.16: Consider implementing completionItem/resolve"

## 4) Completion & “possible next actions” outputs (Rust LSP / rust-analyzer)

### 4.1 `textDocument/completion` (the completion surface)

#### 4.1.1 Method + params

**Method:** `textDocument/completion` (request) ([Microsoft GitHub][1])
**Params:** `CompletionParams` (extends `TextDocumentPositionParams`) plus `context?: CompletionContext` (trigger kind / trigger char). ([Microsoft GitHub][1])

**Value**

* Primary “what can I type here?” channel:

  * symbol names, method/field completions, module paths
  * snippet-style expansions (fn call parens, postfix snippets)
  * *auto-import* completions that include edits to add `use …` lines

**Watch outs**

* Completion semantics are a **3-party negotiation**: server behavior depends on (a) your request params, (b) client capabilities, (c) rust-analyzer config.

---

#### 4.1.2 Result forms: `CompletionItem[]` vs `CompletionList`

**Result:** `CompletionItem[] | CompletionList | null` ([Microsoft GitHub][1])

**CompletionList** key fields (persist these):

* `isIncomplete: boolean` — client should re-request as user types because list may change. ([Microsoft GitHub][1])
* `items: CompletionItem[]` ([Microsoft GitHub][1])
* (3.17+) `itemDefaults?: { commitCharacters?, editRange?, insertTextFormat?, insertTextMode?, data? }` — defaults applied to items that omit fields. ([Microsoft GitHub][1])

**Value**

* `isIncomplete=true` is a big operational signal: you can’t safely treat the returned set as a stable “universe of candidates”.

**Watch outs**

* If you persist completion results as evidence, also persist whether defaults were used (`CompletionList.itemDefaults`) or item fields were explicit—this affects deterministic replay.

---

#### 4.1.3 `CompletionItem` schema (what matters for agents)

Core fields you should treat as “must capture”:

* `label: string` — display key. ([Microsoft GitHub][1])
* `kind?: CompletionItemKind` — symbol category (function, method, module, etc.). ([Microsoft GitHub][1])
* `detail?: string`, `documentation?: string | MarkupContent` — comprehension payload (often lazy; see resolve). ([Microsoft GitHub][1])
* `sortText?: string`, `filterText?: string` — ranking/filter keys; important for reproducible ordering. ([Microsoft GitHub][1])
* **Insertion mechanics** (most important):

  * `textEdit?: TextEdit | InsertReplaceEdit` (preferred when present)
  * `insertText?: string`
  * `insertTextFormat?: InsertTextFormat` (PlainText vs Snippet)
  * `insertTextMode?: InsertTextMode` (whitespace handling modes) ([Microsoft GitHub][1])
* **Additional edits:** `additionalTextEdits?: TextEdit[]` — e.g., add `use foo::Bar;` for auto-import. ([Microsoft GitHub][1])
* `commitCharacters?: string[]` — characters that accept a completion. ([Microsoft GitHub][1])
* `command?: Command` — optional “side effect action” after insert. ([Microsoft GitHub][1])
* `data?: any` — opaque server metadata, commonly used to support `completionItem/resolve`. ([Microsoft GitHub][1])

**Value**

* For LLM agents, the *real output* of completion isn’t the label—it’s the **edit plan**:

  * `textEdit` / `InsertReplaceEdit` tells you exactly what range gets replaced
  * `insertTextFormat=Snippet` tells you whether `${1:arg}`-style placeholders exist
  * `additionalTextEdits` is how auto-import and multi-location transforms happen

**Watch outs (insertion correctness)**

* Prefer `textEdit` over `insertText` when both appear; `textEdit` defines authoritative replacement range. ([Microsoft GitHub][1])
* If client advertises `insertReplaceSupport`, server may return `InsertReplaceEdit` (different insert vs replace ranges). Agents that assume plain `TextEdit` will misapply edits. ([Microsoft GitHub][1])
* Snippet placeholders require `snippetSupport`; if disabled, rust-analyzer will degrade snippet-y completions. (This is client capability negotiation.) ([Microsoft GitHub][1])

---

### 4.2 Client capability gates that directly affect Rust completion behavior

You should persist these from `ClientCapabilities.textDocument.completion` and `…completionItem` because they explain missing functionality:

* `completionItem.snippetSupport` (enables snippet insertTextFormat) ([Microsoft GitHub][1])
* `completionItem.commitCharactersSupport` ([Microsoft GitHub][1])
* `completionItem.documentationFormat` (markdown vs plaintext docs) ([Microsoft GitHub][1])
* `completionItem.deprecatedSupport`, `completionItem.tagSupport` (deprecated tagging vs hidden) ([Microsoft GitHub][1])
* `completionItem.insertReplaceSupport` (InsertReplaceEdit) ([Microsoft GitHub][1])
* `completionItem.resolveSupport: { properties: string[] }` (which fields can be lazily resolved) ([Microsoft GitHub][1])
* **CRITICAL FOR AUTO-IMPORT:** `completionItem.additionalTextEdits` capability support (client must support it for rust-analyzer autoimport to be “truly enabled”). rust-analyzer explicitly calls this out. ([Rust Analyzer][2])

**Watch out (auto-import “works but doesn’t import”)**

* rust-analyzer notes that auto-import requires the client to specify `additionalTextEdits` capability. ([Rust Analyzer][2])
* This is a frequent failure mode in non-VSCode clients (symptom: completion shows items, but selecting doesn’t add `use …`). ([Sublime Forum][3])

---

### 4.3 rust-analyzer completion controls (meaningfully change output set)

From rust-analyzer configuration (persist these with captured completion evidence):

* `rust-analyzer.completion.autoimport.enable` (default `true`) — enables auto-import completions; depends on `additionalTextEdits` support. ([Rust Analyzer][2])
* `rust-analyzer.completion.autoimport.exclude` — excludes specific items/traits from auto-import suggestions; inherits `completion.excludeTraits`. ([Rust Analyzer][2])
* `rust-analyzer.completion.excludeTraits` — prevents methods from those traits from being completed (except for `dyn/impl` trait contexts). ([Rust Analyzer][2])
* `rust-analyzer.completion.callable.snippets` (default `"fill_arguments"`) — controls whether function completions include parens/argument placeholders. ([Rust Analyzer][2])
* `rust-analyzer.completion.postfix.enable` — postfix snippets (`expr.dbg`, `expr.if`, etc.). ([Rust Analyzer][2])
* `rust-analyzer.completion.snippets.custom` — custom snippets (postfix/body/description/requires/scope). ([Rust Analyzer][2])
* `rust-analyzer.completion.fullFunctionSignatures.enable` — include full signatures in completion docs. ([Rust Analyzer][2])
* `rust-analyzer.completion.hideDeprecated` — hides deprecated items (vs tagging). ([Rust Analyzer][2])
* `rust-analyzer.completion.limit` — caps number of returned items (impacts completeness and ranking reproducibility). ([Rust Analyzer][2])

**Value**

* These toggles directly change the completion *candidate universe* and *payload richness*; you cannot treat completion evidence as comparable across runs unless you persist the config snapshot.

---

## 4.4 `completionItem/resolve` (lazy enrichment)

#### 4.4.1 Method + purpose

**Method:** `completionItem/resolve` (request) ([Microsoft GitHub][1])
**Input:** a `CompletionItem` previously returned by `textDocument/completion` (often containing `data` for lookup). ([Microsoft GitHub][1])
**Output:** a richer `CompletionItem` (same identity, more fields populated). ([Microsoft GitHub][1])

**What usually gets populated lazily (high-value fields):**

* `documentation`, `detail`
* `additionalTextEdits` (auto-import edits are the “big one”) ([GitHub][4])

**Value**

* Lets server return lightweight lists fast, then compute heavy docs/import edits only for:

  * the currently selected item
  * or (if your client chooses) multiple items for richer UI

**Watch outs**

* Many clients only resolve **the chosen item**, not every item shown. This is why “docs in completion menu” may be missing unless the client proactively resolves items. ([GitHub][5])
* rust-analyzer explicitly discussed implementing resolve to lazily provide `additionalTextEdits` (auto-import). If resolve isn’t called by the client, you may observe “autoimport enabled but no import edits”. ([GitHub][4])

---

## 4.5 Agent-ready persistence contract (completion evidence)

Persist two layers per completion request:

### 4.5.1 `completion.snapshot`

* `(uri, version?, position, triggerKind/char)`
* `CompletionList.isIncomplete`
* `CompletionList.itemDefaults` (if present)
* each `CompletionItem` normalized:

  * `label`, `kind`, `sortText`, `filterText`
  * insertion plan: `textEdit|InsertReplaceEdit`, `insertText`, `insertTextFormat`, `insertTextMode`
  * `additionalTextEdits` (if present)
  * `command` (if present)
  * `data` (opaque)

### 4.5.2 `completion.resolved`

For any items you resolve, store:

* pre-resolve item hash + post-resolve item hash
* which fields changed (typically docs/detail/additionalTextEdits)

### 4.5.3 Environment keys to store alongside

* client completion capabilities (snippetSupport, insertReplaceSupport, resolveSupport.properties, docs format, additionalTextEdits support) ([Microsoft GitHub][1])
* rust-analyzer completion config snapshot (autoimport/postfix/snippets/callable.snippets/limit/hideDeprecated/etc.) ([Rust Analyzer][2])

If you want to continue in the same style, the next deep dive is usually **“5) Rename, edits, and refactoring outputs”**, because completion edits (`textEdit` + `additionalTextEdits`) are a miniature version of the broader `WorkspaceEdit` machinery used by code actions and rename.

[1]: https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/?utm_source=chatgpt.com "Language Server Protocol Specification - 3.17"
[2]: https://rust-analyzer.github.io/book/configuration.html "Configuration - rust-analyzer"
[3]: https://forum.sublimetext.com/t/how-to-enable-import-autocompletion-with-rust-analyzer-in-sublime-text-lsp/74772?utm_source=chatgpt.com "How to Enable Import Autocompletion with rust-analyzer in ..."
[4]: https://github.com/rust-lang/rust-analyzer/issues/6366?utm_source=chatgpt.com "LSP 3.16: Consider implementing completionItem/resolve"
[5]: https://github.com/neovim/neovim/issues/29225?utm_source=chatgpt.com "LSP completion: display documentation if exists for an item"

## 5) Rename, edits, and refactoring outputs (Rust LSP / rust-analyzer)

### 5.0 The shared substrate: `WorkspaceEdit` (what you actually apply)

**Type:** `WorkspaceEdit` (used by `textDocument/rename`, `CodeAction.edit`, and often produced indirectly by commands). ([LSP 3.17 spec](https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/) ([Microsoft GitHub][1]))

Key shapes (persist + normalize):

* `changes?: { [uri]: TextEdit[] }`
* `documentChanges?: TextDocumentEdit[] | (TextDocumentEdit | CreateFile | RenameFile | DeleteFile)[]`

  * depends on `workspace.workspaceEdit.documentChanges` + `workspace.workspaceEdit.resourceOperations` client capabilities. ([Microsoft GitHub][1])
* **Ordering matters** when `documentChanges` includes file operations (create/rename/delete): clients must execute ops in order, and invalid sequences fail. ([Microsoft GitHub][1])

**Watch outs (edit correctness):**

* Prefer **`documentChanges`** over `changes` when available (versioned doc edits + file ops). ([Microsoft GitHub][1])
* If you apply edits yourself (agent-side), treat overlaps as fatal unless the client/server explicitly supports it; LSP assumes clients will apply a coherent edit set.

**Change annotations (high-value UX / audit trail):**

* Rename + code actions can include change annotations; client capability `honorsChangeAnnotations` exists for both rename + code actions. ([Microsoft GitHub][1])

---

## 5.1 Rename: `textDocument/prepareRename` + `textDocument/rename`

### 5.1.1 `textDocument/prepareRename` (legality + canonical rename span)

**Method:** `textDocument/prepareRename` (request) ([Microsoft GitHub][1])
**Params:** `PrepareRenameParams` = `TextDocumentPositionParams` (+ progress) ([Microsoft GitHub][1])
**Result union (critical):**
`Range | { range: Range, placeholder: string } | { defaultBehavior: boolean } | null` ([Microsoft GitHub][1])

**Value**

* Produces the **authoritative rename range** for the symbol at cursor (or tells you it’s invalid).
* `placeholder` is the server-validated “current name” string; use it to avoid client-side tokenization mistakes. ([Microsoft GitHub][1])
* `{ defaultBehavior: true }` means “valid here, but client should compute range using its default identifier rules” (less precise than server-provided `range`). ([Microsoft GitHub][1])

**Watch outs**

* If the server returns `null`, **don’t call rename** (it’s deemed invalid at that position). ([Microsoft GitHub][1])
* Encoding hazard: all `Range` positions use negotiated position encoding (often UTF-16); persist `positionEncoding` with rename evidence.

### 5.1.2 `textDocument/rename` (produces the full edit set)

**Method:** `textDocument/rename` ([Microsoft GitHub][1])
**Params:** `RenameParams` extends `TextDocumentPositionParams`, includes `newName: string`. ([Microsoft GitHub][1])
**Result:** `WorkspaceEdit | null` (null = no change required). ([Microsoft GitHub][1])

**Value**

* A rename is the cleanest “structured multi-file refactor” output: server has already done symbol resolution + reference enumeration.

**Watch outs**

* Invalid `newName` must return an LSP error with message (client should surface it). ([Microsoft GitHub][1])
* Rename can fail due to invalid code / nothing at position / unsupported symbol; treat as non-deterministic unless workspace health is OK+quiescent (rust-analyzer server-status extension).

---

## 5.2 Code actions: `textDocument/codeAction` (+ filtering + diagnostics tie-in)

### 5.2.1 Request: `textDocument/codeAction`

**Method:** `textDocument/codeAction` ([Microsoft GitHub][1])
**Params:** `CodeActionParams`

* `textDocument: TextDocumentIdentifier`
* `range: Range`
* `context: CodeActionContext` ([Microsoft GitHub][1])

`CodeActionContext` fields (these are operationally important): ([Microsoft GitHub][1])

* `diagnostics: Diagnostic[]` (client-known overlapping diagnostics; **not guaranteed accurate**)
* `only?: CodeActionKind[]` (client-side filter; servers can omit computing others)
* `triggerKind?: CodeActionTriggerKind` (`Invoked` vs `Automatic`, since 3.17)

**Value**

* The `only` filter is your “fast lane”:

  * `only=["quickfix"]` for diagnostic-driven fixes
  * `only=["refactor", "refactor.extract", ...]` for refactors
  * `only=["source.organizeImports"]` for import hygiene
  * `only=["source.fixAll"]` (since 3.17) for safe batch fixes ([Microsoft GitHub][1])

**Watch outs**

* `context.diagnostics` is advisory; spec explicitly says there’s **no guarantee** it reflects true server error state. Don’t treat it as truth; treat `range` as primary selector. ([Microsoft GitHub][1])

### 5.2.2 Response: `(Command | CodeAction)[] | null`

The response can mix:

* **`Command`**: requires `workspace/executeCommand` to actually run
* **`CodeAction`**: can include `edit` directly and/or a `command` ([Microsoft GitHub][1])

`CodeAction` key fields: ([Microsoft GitHub][1])

* `title: string`
* `kind?: CodeActionKind` (hierarchical strings, e.g. `refactor.extract.function`)
* `diagnostics?: Diagnostic[]` (what it claims to resolve)
* `isPreferred?: boolean` (critical for “apply best fix” automation)
* `disabled?: { reason: string }` (since 3.16)
* `edit?: WorkspaceEdit`
* `command?: Command` (if both edit+command, **edit first**, then command)
* `data?: any` (for `codeAction/resolve`, since 3.16)

**Value**

* If your client supports **CodeAction literals**, you can get the full `WorkspaceEdit` without an extra command hop (best case for agents). ([Microsoft GitHub][1])

**Watch outs**

* The spec guidance is explicit: if the client supports edits in code actions, prefer returning edits over requiring commands. ([Microsoft GitHub][1])
* Some ecosystems historically had trouble when servers returned commands “inside” code actions (old rust-analyzer era); modern flows prefer `edit` or resolve-first patterns. (Treat “command-only actions” as highest risk for client incompatibility.) ([Microsoft GitHub][1])

---

## 5.3 Code action resolve: `codeAction/resolve` (lazy edit/materialization)

### 5.3.1 Capability gating

Client advertises:

* `textDocument.codeAction.resolveSupport = { properties: string[] }`
* `textDocument.codeAction.dataSupport?: boolean` (preserve `CodeAction.data`) ([Microsoft GitHub][1])

Server advertises:

* `codeActionProvider.resolveProvider?: boolean` ([Microsoft GitHub][1])

### 5.3.2 Request/response

**Method:** `codeAction/resolve`
**Params:** the `CodeAction` literal (typically with `data`)
**Response:** the same `CodeAction`, with requested properties (often `edit`) filled in. ([Microsoft GitHub][1])

**Value**

* Enables “cheap listing, expensive compute later”: servers can skip computing huge edits until the user/agent selects a specific action. ([Microsoft GitHub][1])

**Watch outs**

* Spec warning: servers **shouldn’t alter existing attributes** during resolve; only fill additional information. Your agent can diff pre/post to detect non-compliant servers. ([Microsoft GitHub][1])
* If your client doesn’t call resolve, you’ll see “actions without edits” — a client integration limitation, not a server limitation.

---

## 5.4 Execute command: `workspace/executeCommand` (command-only actions + custom rust-analyzer commands)

### 5.4.1 LSP core: method + capability contract

**Method:** `workspace/executeCommand` (client → server) ([Microsoft GitHub][1])
**Server capability:** `executeCommandProvider: { commands: string[] }` (authoritative allowlist) ([Microsoft GitHub][1])
**Params:** `ExecuteCommandParams { command: string; arguments?: any[] }` ([Microsoft GitHub][1])

**Spec semantics:**

* Often the server executes by producing a `WorkspaceEdit` and sending `workspace/applyEdit` to the client. ([Microsoft GitHub][1])
* Arguments are typically supplied from prior server responses (codeAction / codeLens). ([Microsoft GitHub][1])

**Value**

* This is the universal escape hatch for “server-defined refactor operations” not expressible purely as a static edit in the initial response.

**Watch outs**

* If `command` is not in the server’s `executeCommandProvider.commands` list, execution should fail (client should treat as unsupported). ([Microsoft GitHub][1])
* Many editor clients have partial support here; failures often present as “method not supported” even when rust-analyzer can do it (client integration mismatch).

### 5.4.2 rust-analyzer extensions that affect “edits/refactors”

Two high-impact extensions for “apply structured edits correctly”:

#### A) `SnippetTextEdit` inside `WorkspaceEdit` (rust-analyzer extension)

If the client advertises experimental capability `{ "snippetTextEdit": boolean }`, rust-analyzer may return `SnippetTextEdit` (a `TextEdit` with `insertTextFormat?: Snippet`) inside `WorkspaceEdit` returned from `codeAction` and also inside `textDocument/onTypeFormatting`. ([Android Git Repositories][2])

Key constraint rust-analyzer states:

* Only **one** `TextDocumentEdit` may contain snippet edits; additional document edits will be plain text. ([Android Git Repositories][2])

**Value**

* Enables refactors like “Add derive” to return an edit with `$0` / placeholders that an editor can tab through. ([Android Git Repositories][2])

**Watch outs**

* If your client/tooling can’t apply snippet edits, you must either:

  * disable this capability, or
  * post-process `SnippetTextEdit` into plain text (dropping placeholders) with clear loss of UX.

#### B) `CodeAction` grouping (rust-analyzer extension)

Experimental client capability `{ "codeActionGroup": boolean }` enables an extra `group?: string` field on `CodeAction` so clients can render nested menus. ([Android Git Repositories][2])

**Value**

* Improves “action discoverability” and lets an agent preserve the server’s intended grouping taxonomy.

**Watch outs**

* Pure UI feature; do not rely on `group` for semantic meaning unless you own both producer and consumer.

---

## 5.5 Agent-ready persistence contract (for rename + refactor outputs)

Persist three layers:

### Layer 1: Capabilities snapshot (gates everything)

* `serverCapabilities.renameProvider`, `serverCapabilities.codeActionProvider`, `serverCapabilities.executeCommandProvider.commands` ([Microsoft GitHub][1])
* `clientCapabilities.textDocument.rename.prepareSupport`, `…prepareSupportDefaultBehavior`, `…honorsChangeAnnotations` ([Microsoft GitHub][1])
* `clientCapabilities.textDocument.codeAction.*` (`resolveSupport`, `dataSupport`, `honorsChangeAnnotations`, codeActionLiteralSupport valueSet) ([Microsoft GitHub][1])
* rust-analyzer experimental flags you enabled (`snippetTextEdit`, `codeActionGroup`) ([Android Git Repositories][2])

### Layer 2: Request/response evidence

* `prepareRename`: union result + any error string ([Microsoft GitHub][1])
* `rename`: `WorkspaceEdit` (full) + any error string ([Microsoft GitHub][1])
* `codeAction`: request `(uri, range, only[], triggerKind)` + returned `(Command|CodeAction)[]` ([Microsoft GitHub][1])
* `codeAction/resolve`: pre/post action diff (esp. `edit` materialization) ([Microsoft GitHub][1])
* `executeCommand`: `{command, arguments}` + returned result payload (if any) ([Microsoft GitHub][1])

### Layer 3: Normalized edit plan

Normalize all edit-bearing results into:

* `edit.document_ops[]` (Create/Rename/Delete + annotations if present)
* `edit.text_ops[]` (per-doc, versioned where possible)
* `edit.contains_snippet_edits` (true/false) + enforce rust-analyzer “single doc may contain snippets” invariant if you enabled it ([Android Git Repositories][2])

If you want the next deep dive, the natural continuation is **“6) Formatting & style outputs”** because it’s the other major “patch-producing” plane (`formatting`, `rangeFormatting`, `onTypeFormatting`) and it interacts directly with refactor pipelines (apply edit → format → re-diagnose).

[1]: https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/ "Specification"
[2]: https://android.googlesource.com/toolchain/rustc/%2B/HEAD/src/tools/rust-analyzer/docs/dev/lsp-extensions.md "LSP Extensions"

## 6) Formatting & style outputs (Rust LSP / rust-analyzer)

### 6.0 Shared substrate: `TextEdit[]` + `FormattingOptions`

All three formatting requests return **patches** (`TextEdit[] | null`) that you apply to the target document(s). ([Microsoft GitHub][1])

**`FormattingOptions` (in every formatting params object):** ([Microsoft GitHub][1])

* Required:

  * `tabSize: uinteger`
  * `insertSpaces: boolean`
* Common optional (since LSP 3.15):

  * `trimTrailingWhitespace?: boolean`
  * `insertFinalNewline?: boolean`
  * `trimFinalNewlines?: boolean`
* Extensible: `[key: string]: boolean | integer | string` (clients can pass tool-specific knobs). ([Microsoft GitHub][1])

**Agent value**

* Treat `FormattingOptions` as **part of the evidence contract**: different clients send different defaults; formatting output can drift if options drift.

**Watch outs**

* Don’t assume only the documented keys are present—LSP explicitly allows extra properties. ([Microsoft GitHub][1])

---

## 6.1 Format document: `textDocument/formatting`

### 6.1.1 Capability + request

**Client capability:** `textDocument.formatting` (`dynamicRegistration?: boolean`) ([Microsoft GitHub][1])
**Server capability:** `documentFormattingProvider: boolean | DocumentFormattingOptions` ([Microsoft GitHub][1])

**Method:** `textDocument/formatting` ([Microsoft GitHub][1])
**Params:** `DocumentFormattingParams` ([Microsoft GitHub][1])

* `textDocument: TextDocumentIdentifier`
* `options: FormattingOptions`

**Result:** `TextEdit[] | null` ([Microsoft GitHub][1])

### 6.1.2 What you get (semantic value)

* A deterministic “whole-file canonicalization” patch (indentation, whitespace normalization, etc.).
* Best used as the **post-transform normalizer** in an agent pipeline:

  1. apply rename/refactor edits
  2. apply full-document formatting
  3. re-run diagnostics

### 6.1.3 rust-analyzer formatting engine knobs (rustfmt integration surface)

rust-analyzer’s formatter is typically rustfmt-driven; its configuration is exposed as: ([Rust Analyzer][2])

* `rust-analyzer.rustfmt.extraArgs: []` (append args to rustfmt)
* `rust-analyzer.rustfmt.overrideCommand: null | string[]`

  * fully overrides the formatter command; **must behave like `rustfmt`, not `cargo fmt`**
  * rust-analyzer passes file contents on **stdin** and reads formatted output from **stdout** ([Rust Analyzer][2])

**Watch outs**

* `overrideCommand` must be an argv array; first element is executable. ([Rust Analyzer][2])
* Real-world failure mode: “formatting/rustfmt not working” in editor often traces to missing toolchain component / misconfigured rustfmt path / wrong command (e.g., using cargo fmt semantics). A representative rust-analyzer issue shows users hitting this with VS Code setups. ([GitHub][3])

---

## 6.2 Format range: `textDocument/rangeFormatting`

### 6.2.1 Capability + request

**Client capability:** `textDocument.rangeFormatting` (`dynamicRegistration?: boolean`) ([Microsoft GitHub][1])
**Server capability:** `documentRangeFormattingProvider: boolean | DocumentRangeFormattingOptions` ([Microsoft GitHub][1])

**Method:** `textDocument/rangeFormatting` ([Microsoft GitHub][1])
**Params:** `DocumentRangeFormattingParams` ([Microsoft GitHub][1])

* `textDocument`
* `range: Range`
* `options: FormattingOptions`

**Result:** `TextEdit[] | null` ([Microsoft GitHub][1])

### 6.2.2 rust-analyzer: range formatting is gated + often “nightly-only”

rust-analyzer explicitly exposes: ([Rust Analyzer][2])

* `rust-analyzer.rustfmt.rangeFormatting.enable` (default `false`)

  * enables rustfmt’s **unstable range formatting** for `textDocument/rangeFormatting`
  * **nightly rustfmt required** (unstable feature)

**Agent value**

* Range formatting is useful when you want:

  * minimal diffs
  * localized normalization after a small edit
* It’s also the best “post-edit formatter” for agent refactors that touch a tight region, *if* your setup supports it.

**Watch outs**

* If `rangeFormatting.enable=false` (default) or rustfmt doesn’t support it, servers may:

  * return null
  * fall back to whole-document formatting (client-dependent UX)
* Nightly requirement is a common portability hazard for CI or non-dev machines. ([Rust Analyzer][2])

---

## 6.3 On-type formatting: `textDocument/onTypeFormatting`

### 6.3.1 Capability + trigger characters

**Client capability:** `textDocument.onTypeFormatting` (`dynamicRegistration?: boolean`) ([Microsoft GitHub][1])
**Server capability:** `documentOnTypeFormattingProvider: DocumentOnTypeFormattingOptions` ([Microsoft GitHub][1])

* `firstTriggerCharacter: string`
* `moreTriggerCharacter?: string[]`

**Method:** `textDocument/onTypeFormatting` ([Microsoft GitHub][1])
**Params:** `DocumentOnTypeFormattingParams` ([Microsoft GitHub][1])

* `textDocument`
* `position: Position` (note: not guaranteed to be exactly where `ch` was typed) ([Microsoft GitHub][1])
* `ch: string` (typed char that triggered formatting; may not be last inserted due to auto-pairs) ([Microsoft GitHub][1])
* `options: FormattingOptions`

**Result:** `TextEdit[] | null` ([Microsoft GitHub][1])

### 6.3.2 rust-analyzer-specific extension: “snippet text edits” in on-type formatting

rust-analyzer has an LSP extension where “on-type formatting” can return **SnippetTextEdit** instead of plain `TextEdit` (tab stops/placeholders), if the client advertises experimental snippet support. This is documented in rust-analyzer’s extension docs and internal LSP ext index. ([Android Git Repositories][4])

**Agent value**

* On-type formatting can be used as a **micro-normalizer** in interactive agent loops (brace insertion, indentation nudges) without formatting the whole file.

**Watch outs**

* If your client/tooling doesn’t support snippet-style edits, you can lose certain edits or see missing actions (same class of issue affects some code actions too). The rust-analyzer extensions doc explicitly constrains snippet edits to avoid multi-doc complexity, and clients must be able to apply snippets correctly. ([Android Git Repositories][4])

---

## 6.4 Agent-ready persistence contract (formatting plane)

Persist per formatting invocation:

* **Request identity**

  * `(uri, version?, method)` where method ∈ `{formatting, rangeFormatting, onTypeFormatting}`
  * `range` (for rangeFormatting)
  * `position + ch` (for onTypeFormatting)
  * `FormattingOptions` full object (including extra keys) ([Microsoft GitHub][1])
* **Result**

  * `TextEdit[] | null` plus:

    * `edit_count`
    * overlap detection / apply-order validation
* **Environment knobs affecting semantics**

  * `rust-analyzer.rustfmt.extraArgs`
  * `rust-analyzer.rustfmt.overrideCommand` (stdin/stdout contract)
  * `rust-analyzer.rustfmt.rangeFormatting.enable` (nightly gate) ([Rust Analyzer][2])
  * whether snippet edit extensions are enabled/supported (if you’re using them) ([Android Git Repositories][4])

If you want to continue, the next deep dive that pairs best with formatting is **“7) Semantic highlighting & token classification outputs”** (semantic tokens), because formatting pipelines often need stable token spans to avoid applying edits across “wrong” syntactic regions.

[1]: https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/ "Specification"
[2]: https://rust-analyzer.github.io/book/configuration.html "Configuration - rust-analyzer"
[3]: https://github.com/rust-lang/rust-analyzer/issues/17740?utm_source=chatgpt.com "rustfmt not working in VSCode · Issue #17740 · rust-lang/ ..."
[4]: https://android.googlesource.com/toolchain/rustc/%2B/HEAD/src/tools/rust-analyzer/docs/dev/lsp-extensions.md?utm_source=chatgpt.com "LSP Extensions"

## 7) Semantic highlighting & token classification outputs (Semantic Tokens)

### 7.0 Negotiation surface (you must persist this; it defines how to decode everything)

#### 7.0.1 Client → server capability: `textDocument.semanticTokens`

Client advertises (subset of) these fields (names per LSP 3.17): ([Microsoft GitHub][1])

* `requests`:

  * `full: boolean | { delta?: boolean }` → enables `textDocument/semanticTokens/full` and optionally `/full/delta` ([Microsoft GitHub][1])
  * `range?: boolean` → enables `textDocument/semanticTokens/range` ([Microsoft GitHub][1])
* `formats: SemanticTokenFormat[]` (almost always `["relative"]`) ([Microsoft GitHub][1])
* `tokenTypes: string[]` + `tokenModifiers: string[]`

  * these are **the legend request from the client side** (client may constrain accepted token types/mods) ([Microsoft GitHub][1])
* `multilineTokenSupport?: boolean`
* `overlappingTokenSupport?: boolean`

**Value**

* This block is the *decode contract*:

  * the server will return integers that only make sense relative to the server’s `legend` (below)
  * overlap/multiline support dictates whether you can expect “clean” non-overlapping tokens

**Watch outs**

* Many clients only support the `relative` format; if you build an agent-side decoder, assume relative unless you explicitly see other formats advertised. ([Microsoft GitHub][1])

#### 7.0.2 Server → client capability: `semanticTokensProvider`

In `InitializeResult.capabilities`, server advertises: ([Microsoft GitHub][1])

* `semanticTokensProvider: SemanticTokensOptions | SemanticTokensRegistrationOptions`

  * `legend: { tokenTypes: string[]; tokenModifiers: string[] }`
  * `range?: boolean | {}` (supports `…/range`)
  * `full?: boolean | { delta?: boolean }` (supports `…/full` and optionally `…/full/delta`)

**Value**

* The **server legend is authoritative** for interpreting `tokenType`/`tokenModifiers` indices in the returned integer stream.

**Watch outs**

* Don’t assume the token strings match your editor theme’s names. The only stable join is:

  * `tokenTypeIndex` → `legend.tokenTypes[tokenTypeIndex]`
  * `modifierBitset` → map bits to `legend.tokenModifiers[bit]` ([Microsoft GitHub][1])

---

## 7.1 Endpoints (syntax + what they mean)

### 7.1.1 Full tokens: `textDocument/semanticTokens/full`

**Method:** `textDocument/semanticTokens/full` (request) ([Microsoft GitHub][1])
**Params:** `SemanticTokensParams` (document identifier) ([Microsoft GitHub][1])
**Result:** `SemanticTokens | null`

* `resultId?: string`
* `data: uinteger[]` ([Microsoft GitHub][1])

**Value**

* Snapshot classification for the entire document (best for batch indexing / first paint).

**Watch outs**

* Can be empty or partial early in server startup / indexing; treat as “eventual consistency.” (This pattern is broadly observed across servers; don’t baseline until workspace is quiescent.) ([GitHub][2])

### 7.1.2 Delta tokens: `textDocument/semanticTokens/full/delta`

**Method:** `textDocument/semanticTokens/full/delta` (request) ([Microsoft GitHub][1])
**Params:** `{ textDocument, previousResultId: string }` ([Microsoft GitHub][1])
**Result:** `SemanticTokens | SemanticTokensDelta | null`

* `SemanticTokensDelta` contains `edits: SemanticTokensEdit[]` and may include a new `resultId` ([Microsoft GitHub][1])

**Value**

* High-throughput incremental updating (critical when agents or editors keep documents open while making many edits).

**Watch outs**

* Delta can be *worse* than full in large documents depending on client behavior and server diff strategy; there are known performance complaints in the LSP ecosystem about delta edits. ([GitHub][2])

### 7.1.3 Range tokens: `textDocument/semanticTokens/range`

**Method:** `textDocument/semanticTokens/range` (request) ([Microsoft GitHub][1])
**Params:** `SemanticTokensRangeParams` = `{ textDocument, range }` ([Microsoft GitHub][1])
**Result:** `SemanticTokens | null` ([Microsoft GitHub][1])

**Value**

* Best “agent consumption” mode when you only need tokens for a window (e.g., hovered span, function body, diff hunk) and want to bound cost.

**Watch outs**

* Not all servers implement range tokens; must be negotiated via `semanticTokensProvider.range`. ([Microsoft GitHub][1])

---

## 7.2 Token encoding (the `data: uinteger[]` stream)

### 7.2.1 Relative encoding: 5 integers per token

Semantic tokens are not structured; each token is encoded as **5 integers** in a flat array: ([Stack Overflow][3])

For each token:

1. `deltaLine`
2. `deltaStart`
3. `length`
4. `tokenType` (index into server legend tokenTypes)
5. `tokenModifiers` (bitset over server legend tokenModifiers)

And the positions are **relative**:

* `deltaLine` is relative to previous token’s line
* if `deltaLine == 0`, `deltaStart` is relative to previous token’s start char; else it’s relative to 0 on the new line ([Pygls][4])

**Value**

* This is compact enough to stream frequently; it’s the intended design for editor repaint performance.

**Watch outs (common decoding bugs)**

* You must decode in-order; you cannot randomly access without replay.
* `tokenType` is an *index*, not the string; always map through `legend.tokenTypes`. ([Microsoft GitHub][1])
* `tokenModifiers` is a bitset; multiple modifiers can apply simultaneously (e.g., `mutable` + `declaration` if your legend includes them). ([Microsoft GitHub][1])

### 7.2.2 Multiline + overlap rules (capability-sensitive)

* If client says `multilineTokenSupport=false`, server should avoid tokens spanning multiple lines (or degrade). ([Microsoft GitHub][1])
* If client says `overlappingTokenSupport=false`, server should avoid overlapping tokens; otherwise overlaps may be emitted.

**Value**

* For an agent building “stable token categories,” disabling overlap/multiline (or at least detecting them) makes it easier to join tokens to AST/CST spans.

**Watch outs**

* Overlaps can cause “double classification” conflicts with syntax highlighters; in many editors, semantic tokens can override TextMate/tree-sitter highlighting. rust-analyzer explicitly calls out override behavior, especially for comments/strings. ([Rust Analyzer][5])

---

## 7.3 Refresh semantics (server-triggered invalidation)

### 7.3.1 `workspace/semanticTokens/refresh`

**Method:** `workspace/semanticTokens/refresh` (server → client request) ([Microsoft GitHub][1])
**Client gate:** `workspace.semanticTokens.refreshSupport?: boolean` ([Microsoft GitHub][1])

**Value**

* The only standard mechanism for “project-wide semantic token invalidation” (cfg flags changed, workspace reloaded, macro expansion now available, etc.).

**Watch outs**

* It is explicitly a **global hammer**: forces the client to refresh semantic tokens for all visible documents; spec warns to “use with absolute care.” ([Microsoft GitHub][1])
* It has no document parameter (can’t target just one file), which causes real-world pain during long indexing; ecosystem discussions call this out directly. ([GitHub][2])

---

## 7.4 rust-analyzer semantic highlighting knobs (change what tokens you get)

rust-analyzer exposes targeted toggles to avoid semantic tokens overriding other highlighters in specific regions: ([Rust Analyzer][5])

* `rust-analyzer.semanticHighlighting.comments.enable` (default `true`)
* `rust-analyzer.semanticHighlighting.strings.enable` (default `true`)

**Value**

* For agent pipelines that rely on semantic tokens as a stable classifier:

  * leave these enabled for maximum coverage
* For editor experiences that rely on other grammars inside strings/comments:

  * disable strings/comments semantic tokens so TextMate/tree-sitter can highlight embedded content (regexes, SQL, doc comment markup, etc.). ([Rust Analyzer][5])

**Watch outs**

* “Semantic highlighting takes precedence” is a recurring complaint; if you’re capturing tokens through a client, user-level settings can change what you observe. ([GitHub][6])

---

## 7.5 Agent-ready persistence contract (semantic tokens)

Persist these *as a single atomic record* per request, because decoding requires legend + encoding:

### 7.5.1 Environment + negotiation

* `env.positionEncoding`
* `cap.client.textDocument.semanticTokens` (requests/formats/tokenTypes/tokenModifiers/overlap/multiline)
* `cap.server.semanticTokensProvider.legend` + `cap.server.semanticTokensProvider.full/range` ([Microsoft GitHub][1])

### 7.5.2 Request record

* `method ∈ {full, full/delta, range}`
* `uri`, `version?`
* `range` if range request
* `previousResultId` if delta request ([Microsoft GitHub][1])

### 7.5.3 Response record

* `resultId?`
* `data: uinteger[]` OR `edits[]` (delta) ([Microsoft GitHub][1])
* Derived normalized token rows (store both raw + decoded):

  * `line`, `startChar`, `length`
  * `tokenTypeStr = legend.tokenTypes[tokenTypeIdx]`
  * `modifierStrs = { legend.tokenModifiers[i] | bitset has bit i }`

### 7.5.4 Derived “agent-grade” invariants (validate on ingest)

* Non-decreasing `(line,start)` order (relative decode sanity)
* If `overlappingTokenSupport=false`, assert no overlaps in decoded tokens (flag otherwise)
* If `multilineTokenSupport=false`, assert tokens do not span lines (flag otherwise) ([Microsoft GitHub][1])

If you want to continue: the next deep dive that stacks naturally on semantic tokens is **“8) Inlay hints & inline annotations outputs”** (they’re the other high-density “semantic overlay” plane and they interact heavily with the same refresh/latency constraints).

[1]: https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/?utm_source=chatgpt.com "Language Server Protocol Specification - 3.17"
[2]: https://github.com/microsoft/language-server-protocol/issues/1421?utm_source=chatgpt.com "SemanticTokens Edits not performant in large documents"
[3]: https://stackoverflow.com/questions/70490767/language-server-semantic-tokens?utm_source=chatgpt.com "Language server semantic tokens - visual studio code"
[4]: https://pygls.readthedocs.io/en/latest/protocol/howto/interpret-semantic-tokens.html?utm_source=chatgpt.com "How To Interpret Semantic Tokens - pygls v2.0.1"
[5]: https://rust-analyzer.github.io/book/configuration.html?utm_source=chatgpt.com "Configuration"
[6]: https://github.com/rust-lang/rust-analyzer/issues/15861?utm_source=chatgpt.com "Add configuration option for disabling semantic highlighting"

## 8) Inlay hints & inline annotations outputs (Rust LSP / rust-analyzer)

### 8.1 Capability negotiation (feature gating + lazy-resolution contract)

#### 8.1.1 Client capabilities: `textDocument.inlayHint`

Client advertises `InlayHintClientCapabilities` with:

* `dynamicRegistration?: boolean`
* `resolveSupport?: { properties: string[] }` (the **exact** fields the client can lazy-resolve) ([Microsoft GitHub][1])

**Value**

* `resolveSupport.properties` is the **contract** that determines whether the server should omit expensive fields (tooltip/locations/commands) on initial hint computation and fill them later via resolve. ([Microsoft GitHub][1])

#### 8.1.2 Server capabilities: `inlayHintProvider`

Server advertises `inlayHintProvider: InlayHintOptions`, including:

* `resolveProvider?: boolean` (server supports `inlayHint/resolve`) ([Microsoft GitHub][1])

**Watch outs**

* If **either** side doesn’t support resolve (client doesn’t list resolvable properties, or server doesn’t set `resolveProvider`), you must treat initial `textDocument/inlayHint` responses as “fully baked” (no second pass). ([Microsoft GitHub][1])

---

### 8.2 Inlay hints request: `textDocument/inlayHint`

#### 8.2.1 Request syntax

**Method:** `textDocument/inlayHint`
**Params:** `InlayHintParams`

* `textDocument: TextDocumentIdentifier`
* `range: Range` (the **visible** range for which hints should be computed) ([Microsoft GitHub][1])

**Result:** `InlayHint[] | null` ([Microsoft GitHub][1])

**Value**

* Range-scoped computation is ideal for agents: request hints only for the function/span you’re analyzing, not the entire file. ([Microsoft GitHub][1])

**Watch outs**

* This endpoint is explicitly “range/viewport” oriented; clients typically call it only for visible editors. If your agent wants whole-file hints, you must window your requests across ranges (and be prepared for different answers if the server isn’t quiescent). ([Microsoft GitHub][1])

---

### 8.3 Inlay hint object schema (what to persist + how to interpret)

`InlayHint` fields you should treat as first-class evidence:

#### 8.3.1 Anchoring + ordering

* `position: Position`

  * If multiple hints share a position, they are shown in response order. ([Microsoft GitHub][1])

#### 8.3.2 Label (simple or composite)

* `label: string | InlayHintLabelPart[]` (cannot be empty) ([Microsoft GitHub][1])

`InlayHintLabelPart` supports interactive labels:

* `value: string`
* `tooltip?: string | MarkupContent`
* `location?: Location` (turns label part into a clickable link that resolves to a symbol’s definition / hover / context menu) ([Microsoft GitHub][1])

#### 8.3.3 Kind taxonomy (minimal but stable)

* `kind?: InlayHintKind` where:

  * `Type = 1`
  * `Parameter = 2` ([Microsoft GitHub][1])

**Value**

* Minimal stable classification for downstream bucketing:

  * “type overlays” vs “parameter name overlays”.

#### 8.3.4 “Accept this hint” edits

* `textEdits?: TextEdit[]`

  * accepting the hint should change the document so the hint (or a near variant) becomes part of code, making the hint obsolete
  * may be lazy-resolved ([Microsoft GitHub][1])

**Value**

* Turns inlay hints into a **code action-like** mechanism:

  * hints can propose explicit annotations (e.g., making an inferred type explicit) via a deterministic patch set.

**Watch outs**

* Treat `textEdits` like any other patch: validate non-overlap, ensure they target the current doc version, and expect they may disappear after application because the hint becomes “baked into code”. ([Microsoft GitHub][1])

#### 8.3.5 Tooltip + layout

* `tooltip?: string | MarkupContent` (may be lazy-resolved) ([Microsoft GitHub][1])
* `paddingLeft?: boolean`, `paddingRight?: boolean` ([Microsoft GitHub][1])
* `data?: LSPAny` preserved across `textDocument/inlayHint` ↔ `inlayHint/resolve` ([Microsoft GitHub][1])

---

### 8.4 Lazy resolution: `inlayHint/resolve`

#### 8.4.1 What it’s for

**Method:** `inlayHint/resolve`
**Params:** `InlayHint`
**Result:** `InlayHint` ([Microsoft GitHub][1])

Resolve exists to compute expensive properties lazily, especially:

* `tooltip`
* `labelPart.location`
* `labelPart.command` ([Microsoft GitHub][1])

**Value**

* Lets you fetch *many cheap hints* quickly, then selectively resolve the few hints your agent actually needs to interpret deeply (e.g., resolve only type hints inside a diff hunk). ([Microsoft GitHub][1])

**Watch outs**

* The client must explicitly advertise which properties it can resolve (`resolveSupport.properties`), otherwise the server can’t rely on resolve and may omit useful metadata. ([Microsoft GitHub][1])

---

### 8.5 Refresh semantics: `workspace/inlayHint/refresh`

#### 8.5.1 Global refresh request

**Method:** `workspace/inlayHint/refresh` (server → client request)

* Params: none
* Result: void
* Client gate: `workspace.inlayHint.refreshSupport?: boolean` ([Microsoft GitHub][1])

Spec guidance:

* It is **global**, forces refresh of all currently shown hints
* “use with absolute care”
* clients may delay recalculation if editor isn’t visible ([Microsoft GitHub][1])

**Value**

* This is the “invalidate inlay overlay cache” lever after:

  * config changes (hint categories toggled)
  * workspace reload / dependency resolution / macro availability changes
  * server becomes quiescent after initial indexing

**Watch outs (rust-analyzer behavior you’ll see in practice)**

* rust-analyzer ecosystem discussion explicitly calls out the pattern “send `workspace/inlayHint/refresh` once the server becomes quiescent / workspace loaded” to fix clients that request hints too early. ([GitHub][2])

---

### 8.6 rust-analyzer inlay hints configuration knobs (output-set semantics)

rust-analyzer’s inlay hints are *not one feature*; they’re a family of independently gated hint producers. High-signal toggles you should persist alongside captured hint output:

#### 8.6.1 High-impact defaults

* `rust-analyzer.inlayHints.typeHints.enable` (default `true`) — type hints for variables ([Rust Analyzer][3])
* `rust-analyzer.inlayHints.parameterHints.enable` (default `true`) — parameter name hints at call sites ([Rust Analyzer][3])
* `rust-analyzer.inlayHints.chainingHints.enable` (default `true`) — method chain type hints ([Rust Analyzer][3])
* `rust-analyzer.inlayHints.closingBraceHints.enable` (default `true`) + `.minLines` (default `25`) — closing `}` annotations ([Rust Analyzer][3])

#### 8.6.2 Noise-control + meaning-shaping

* `rust-analyzer.inlayHints.maxLength` (default `25`; not strictly guaranteed) ([Rust Analyzer][3])
* `rust-analyzer.inlayHints.renderColons` (default `true`) — affects visual encoding of type/parameter hints ([Rust Analyzer][3])
* Type-hint suppression variants:

  * `typeHints.hideInferredTypes` (default `false`)
  * `typeHints.hideClosureParameter` / `hideClosureInitialization` / `hideNamedConstructor` ([Rust Analyzer][3])

#### 8.6.3 Advanced semantic overlays (often “off by default”)

* `bindingModeHints.enable` (default `false`) ([Rust Analyzer][3])
* `closureCaptureHints.enable` (default `false`) ([Rust Analyzer][3])
* `lifetimeElisionHints.enable` (default `"never"`) + `.useParameterNames` ([Rust Analyzer][3])
* generic parameter hints:

  * `.genericParameterHints.const.enable` (default `true`)
  * `.genericParameterHints.type.enable` (default `false`)
  * `.genericParameterHints.lifetime.enable` (default `false`) ([Rust Analyzer][3])

**Value**

* These toggles materially change what an agent will observe as “inline semantic evidence.” If you don’t persist config, you can’t compare hint sets across runs or across developers.

---

### 8.7 Agent-ready persistence contract (inlay hints)

Persist per request (treat as windowed overlay evidence):

1. **Negotiation snapshot**

* `serverCapabilities.inlayHintProvider.resolveProvider`
* `clientCapabilities.textDocument.inlayHint.resolveSupport.properties`
* `clientCapabilities.workspace.inlayHint.refreshSupport`
* `positionEncoding` ([Microsoft GitHub][1])

2. **Request record**

* `(uri, version?, range)` + timestamp
* `workspace_health_snapshot` (if you’re also tracking rust-analyzer serverStatus) to down-rank hints emitted before quiescence ([GitHub][2])

3. **Hint rows (normalized)**
   For each `InlayHint`:

* `position`, `kind`
* `label_text` (flattened) + `label_parts[]` (if composite)
* `tooltip` (if present)
* `textEdits` (if present)
* `paddingLeft/Right`
* `data` (opaque) ([Microsoft GitHub][1])

4. **Resolve enrichment**

* Pre/post `inlayHint/resolve` diff (what properties were actually filled)
* Which property was requested/expected by `resolveSupport.properties` ([Microsoft GitHub][1])

5. **Refresh events**

* Whenever `workspace/inlayHint/refresh` is received, mark all previously persisted “visible-range hint caches” as stale (global invalidation). ([Microsoft GitHub][1])

[1]: https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/ "Specification"
[2]: https://github.com/rust-lang/rust-analyzer/issues/13369?utm_source=chatgpt.com "Support for workspace/inlayHint/refresh to ensure inlay ..."
[3]: https://rust-analyzer.github.io/book/configuration.html "Configuration - rust-analyzer"

## 9) Folding, selection, and structural editing outputs (standard LSP)

### 9.1 Folding ranges: `textDocument/foldingRange`

#### 9.1.1 Capability negotiation (client + server)

**Client capability:** `textDocument.foldingRange: FoldingRangeClientCapabilities` with these high-impact knobs:

* `rangeLimit?: uinteger` (hint to cap number of folding ranges per doc) ([Microsoft GitHub][1])
* `lineFoldingOnly?: boolean` → if true, client **ignores** `startCharacter` / `endCharacter` and only supports whole-line folds ([Microsoft GitHub][1])
* `foldingRangeKind.valueSet?: FoldingRangeKind[]` (client-supported kinds; client must handle unknown kinds gracefully) ([Microsoft GitHub][1])
* `foldingRange.collapsedText?: boolean` (client supports server-supplied `collapsedText`) ([Microsoft GitHub][1])

**Server capability:** `foldingRangeProvider: boolean | FoldingRangeOptions | FoldingRangeRegistrationOptions` ([Microsoft GitHub][1])

**Value**

* These flags determine whether folding can be **character-precise** vs **line-only**, whether you can rely on `kind`, and whether you can persist/expect `collapsedText` labels.

#### 9.1.2 Request / response

**Method:** `textDocument/foldingRange` ([Microsoft GitHub][1])
**Params:** `FoldingRangeParams { textDocument }` ([Microsoft GitHub][1])
**Result:** `FoldingRange[] | null` ([Microsoft GitHub][1])

`FoldingRange` fields:

* `startLine: uinteger`, `endLine: uinteger` (0-based; clients may ignore invalid ranges) ([Microsoft GitHub][1])
* `startCharacter?: uinteger`, `endCharacter?: uinteger` (defaults to line length when omitted; ignored if `lineFoldingOnly=true`) ([Microsoft GitHub][1])
* `kind?: FoldingRangeKind` where standardized kinds include:

  * `comment`, `imports`, `region` (extensible string type) ([Microsoft GitHub][1])
* `collapsedText?: string` (optional label shown when folded; 3.17 “proposed” and must be capability-gated by client support) ([Microsoft GitHub][1])

**Agent value**

* A cheap, LSP-native “structure skeleton” per file:

  * foldable blocks approximate AST block boundaries (impls, fns, modules) without parsing
  * kind allows bucketing: fold-all-imports / fold-all-comments / fold-all-regions

#### 9.1.3 Watch outs (folding is deceptively lossy)

* **Line-only clients**: if `lineFoldingOnly=true`, character-precision is discarded; store this flag with your persisted folding evidence or your “fold span” joins will be wrong. ([Microsoft GitHub][1])
* **Server “hint” limits**: `rangeLimit` is only a hint; servers can exceed/ignore it. You must be able to handle large lists. ([Microsoft GitHub][1])
* **UI meaning inversion hazards**: folding choices can hide semantics (classic example: folding an `if` branch can visually “detach” an `else`), which makes folding ranges risky as a *semantic* signal (treat as structural-only). ([GitHub][2])
* **No folding refresh in LSP 3.17**: `workspace/foldingRange/refresh` is **not** part of 3.17; it lands as a 3.18 addition/proposal in tooling ecosystems. So you generally re-request on open/change rather than rely on a server push refresh. ([YouTrack][3])

---

### 9.2 Selection ranges: `textDocument/selectionRange`

#### 9.2.1 Capability + request/response

**Client capability:** `textDocument.selectionRange?: SelectionRangeClientCapabilities` (introduced as a distinct capability; clients may omit) ([Microsoft GitHub][1])

**Method:** `textDocument/selectionRange` ([Microsoft GitHub][1])
**Params:** `SelectionRangeParams { textDocument, positions: Position[] }` ([Microsoft GitHub][1])
**Result:** `SelectionRange[] | null` ([Microsoft GitHub][1])

`SelectionRange` schema:

* `range: Range` (the selection span) ([Microsoft GitHub][1])
* `parent?: SelectionRange` (must contain the child range: `parent.range` ⊇ `this.range`) ([Microsoft GitHub][1])

**Agent value**

* Produces a **nested containment chain** per cursor position:

  * token → expression → statement → block → item
* This is extremely useful for “structural context extraction”:

  * expand selection outward deterministically (no parsing required)
  * define “edit windows” for localized refactors (choose parent at desired granularity)

#### 9.2.2 Multi-position batching (high leverage)

Because params accept `positions: Position[]`, you can request selection chains for:

* multiple cursors
* multiple “interesting offsets” inside a span (e.g., start/mid/end of a function body)

This gives a cheap approximation to “what syntactic containers exist around these points” without exporting an AST. ([Microsoft GitHub][1])

#### 9.2.3 Watch outs

* **Containment is the only invariant**: spec only guarantees parent contains child; it does *not* promise the “meaning” of each level (expression vs statement) nor that levels are exhaustive. Treat it as a containment ladder, not a typed AST. ([Microsoft GitHub][1])
* **Position encoding**: all `Range/Position` values use negotiated LSP position encoding; persist `positionEncoding` alongside stored selection ladders if you later convert to byte spans.

---

### 9.3 Persistence contract (agent-ready)

#### 9.3.1 Folding ranges

Persist per `(uri, doc_version?, positionEncoding)`:

* `client.lineFoldingOnly`, `client.rangeLimit`, `client.kind.valueSet`, `client.collapsedTextSupport` ([Microsoft GitHub][1])
* `folds[]` normalized:

  * `startLine,startChar?,endLine,endChar?,kind?,collapsedText?`
* Derived invariants / QA:

  * validate `(startLine,endLine)` bounds; drop invalid (clients “free to ignore invalid ranges”) ([Microsoft GitHub][1])

#### 9.3.2 Selection ranges

Persist per request `(uri, doc_version?, positions[])`:

* `chains[]` where each chain is a linked list:

  * `level0.range`, `level1.range` … with containment validation (`parent ⊇ child`) ([Microsoft GitHub][1])
* Derived utility:

  * `closest_container(range_target)` = first parent whose range fully contains your target span (useful for “format/organize/fix within container” workflows)

---

If you want to continue, the next section that pairs tightly with this is **10) rust-analyzer-specific “deep Rust” extensions** (macro expansion views, runnables, cargo actions, analyzer status), because those often *explain* why folding/selection or other structure outputs look incomplete during partial workspace analysis.

[1]: https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/ "Specification"
[2]: https://github.com/rust-analyzer/rust-analyzer/issues/2033?utm_source=chatgpt.com "Confusing folding of if-else statements · Issue #2033"
[3]: https://youtrack.jetbrains.com/projects/IJPL/issues/IJPL-197981/LSP-API-support-workspace-foldingRange-refresh?utm_source=chatgpt.com "LSP API: support `workspace/foldingRange/refresh`"

## 10) rust-analyzer “deep Rust” LSP extensions (high leverage)

**Ground rules (scope + negotiation):**

* rust-analyzer keeps Rust-specific / not-yet-upstreamed endpoints under:

  * `rust-analyzer/*` (RA-specific)
  * `experimental/*` (extensions they hope to upstream)
* **All extension capabilities are negotiated via `experimental` in client/server capabilities.** ([Android Git Repositories][1])

---

# 10.1 Macro expansion / expansion views

## 10.1.1 Expand macro at cursor: `rust-analyzer/expandMacro`

**Method:** `rust-analyzer/expandMacro` (request) ([Rust Language][2])
**Params:** `ExpandMacroParams { textDocument, position }` ([Android Git Repositories][1])
**Result:** `Option<ExpandedMacro>` where `ExpandedMacro { name: string, expansion: string }` ([Rust Language][2])

**Value**

* Converts “macro call site” → “expanded source text” (critical in Rust: derives, attribute macros, declarative macros).
* Lets an agent:

  * attribute generated items to their macro origin
  * debug “where did this impl/type come from?” without running `cargo expand`

**Watch outs**

* Output is **textual expansion**, not a stable AST node ID; don’t treat it as canonical identity.
* Expansion quality depends heavily on proc-macro/build-script health; if proc-macros are stale/broken you’ll get partial/failed expansions (tie this to `serverStatus` health/quiescent from earlier).

---

# 10.2 Runnable discovery (tests/bins/bench/check targets)

## 10.2.1 Enumerate runnables: `experimental/runnables`

**Capability:** `experimental: { runnables: { kinds: string[] } }` (server advertises supported runnable `kind`s) ([Android Git Repositories][1])
**Method:** `experimental/runnables` (request) ([Android Git Repositories][1])

**Params**

```ts
interface RunnablesParams {
  textDocument: TextDocumentIdentifier;
  position?: Position; // if omitted, compute for whole file
}
```

([Android Git Repositories][1])

**Result**

```ts
interface Runnable {
  label: string;
  location?: LocationLink;
  kind: string; // advertised via server capabilities
  args: any;    // kind-specific
}
```

([Android Git Repositories][1])

rust-analyzer documents two runnable kinds: `"cargo"` and `"shell"` with structured args:

**`"cargo"` args** include:

* `cwd`, optional `workspaceRoot`
* `cargoArgs: string[]`
* `executableArgs: string[]` (post `--`)
* optional `environment: Record<string,string>`
* optional `overrideCargo: string` ([Android Git Repositories][1])

**`"shell"` args** include:

* `cwd`
* `program: string`
* `args: string[]`
* optional `environment` ([Android Git Repositories][1])

**Value**

* Turns code locations into concrete “run commands” without bespoke Cargo parsing:

  * unit tests (`cargo test -p … -- …`)
  * bin targets (`cargo run --bin …`)
  * workspace-scoped checks
* For agent automation:

  * persist runnable `args` as executable plans
  * use `location` to anchor “why this runnable exists” (function/module/test item)

**Watch outs**

* Execution is **client responsibility** (“actual running is handled by the client”). The server only emits a runnable plan. ([Android Git Repositories][1])
* `args` schema is **kind-specific**; do not hard-code beyond `"cargo"`/`"shell"` unless you also parse advertised `kinds`.

---

# 10.3 Structural Search & Replace (SSR) (AST-driven codemods)

## 10.3.1 `experimental/ssr` → returns a `WorkspaceEdit`

**Capability:** `experimental: { ssr: boolean }` ([Android Git Repositories][1])
**Method:** `experimental/ssr` (request) ([Android Git Repositories][1])

**Params**

```ts
interface SsrParams {
  query: string;               // SSR DSL (outside protocol)
  parseOnly: boolean;          // validate query only
  textDocument: TextDocumentIdentifier;
  position: Position;          // scope for path resolution
  selections: Range[];         // restrict search/replace if non-empty
}
```

([Android Git Repositories][1])

**Result:** `WorkspaceEdit` ([Android Git Repositories][1])

**Value**

* Server-side, syntax-aware transformations (far safer than regex):

  * consistent rewriting across macro-heavy / generic-heavy code
  * edits already packaged as `WorkspaceEdit` (ready to apply + then format)

**Watch outs**

* SSR query syntax is not standardized by LSP; treat as rust-analyzer-specific DSL.
* `parseOnly=true` is your “lint SSR query” mode; use it to fail fast without computing large edits. ([Android Git Repositories][1])
* Scope control is currently via `position` + `selections`; if you need repo-wide SSR, you must orchestrate multi-file selection windows yourself.

---

# 10.4 Cargo/workspace actions (reload, rebuild, flycheck control)

## 10.4.1 Reload workspace: `rust-analyzer/reloadWorkspace`

**Method:** `rust-analyzer/reloadWorkspace` (request)
**Params:** `null` → **Result:** `null`
**Semantics:** “re-executes `cargo metadata`” ([Android Git Repositories][1])

**Value**

* Deterministic resync after:

  * editing `Cargo.toml`
  * switching features/targets via client config
  * changing workspace members

**Watch outs**

* Treat as a global invalidation event: diagnostics, symbol indexes, semantic tokens, inlay hints, runnables can all churn until quiescent.

## 10.4.2 Rebuild proc-macros + build scripts: `rust-analyzer/rebuildProcMacros`

**Method:** `rust-analyzer/rebuildProcMacros` (request)
**Params:** `null` → **Result:** `null`
**Semantics:** “Rebuilds build scripts and proc-macros … reseed the build data.” ([Android Git Repositories][1])

**Value**

* “Fix my macro universe” lever:

  * when expansions/derive-generated items are missing
  * when unresolved proc-macro diagnostics appear
  * after toolchain changes affecting proc-macro execution

**Watch outs**

* High-cost; expect CPU + rebuild time. Gate it (only when macro-related correctness is degraded).

## 10.4.3 Flycheck control (check-on-save external diagnostics)

**Methods (notifications):**

* `rust-analyzer/runFlycheck` with `RunFlycheckParams { textDocument: TextDocumentIdentifier | null }`

  * starts flycheck for a workspace or a file’s workspace ([Android Git Repositories][1])
* `rust-analyzer/clearFlycheck` with empty params (clears flycheck diagnostics) ([Android Git Repositories][1])
* `rust-analyzer/cancelFlycheck` with empty params (cancels running flycheck processes) ([Android Git Repositories][1])

**Value**

* Lets an agent drive “compiler-grade” diagnostics lifecycle explicitly:

  * kick off a check after a refactor batch
  * cancel runaway checks during rapid edits
  * clear stale external diagnostics

**Watch outs**

* These are notifications, not requests (no response body); you need separate channels (publishDiagnostics / serverStatus) to observe effects. ([Android Git Repositories][1])

---

# 10.5 Server status / diagnostics beyond generic LSP

## 10.5.1 Health + quiescence: `experimental/serverStatus` (notification)

**Client capability:** `{ "serverStatusNotification": boolean }` ([Android Git Repositories][1])
**Method:** `experimental/serverStatus` (server → client notification) ([Android Git Repositories][1])

**Payload:** `ServerStatusParams { health: "ok"|"warning"|"error", quiescent: boolean, message?: string }` ([Android Git Repositories][1])

**Value**

* Your “truthiness gate” for *all* semantic facts:

  * `health != ok` → mark symbol graphs / expansions / references as low confidence
  * `quiescent=false` → expect churn; avoid baselining outputs

## 10.5.2 Debug status dump: `rust-analyzer/analyzerStatus`

**Method:** `rust-analyzer/analyzerStatus` (request)
**Params:** `{ textDocument?: TextDocumentIdentifier }`
**Result:** `string` (“internal status message”) ([Android Git Repositories][1])

**Value**

* Debug-grade “why is the universe broken” output; persist in run artifacts when your agent sees degraded status.

---

# 10.6 Navigation + editor actions that require real Rust parsing

## 10.6.1 Parent module: `experimental/parentModule`

**Capability:** `experimental: { parentModule: boolean }` ([Android Git Repositories][1])
**Method:** `experimental/parentModule` (request)
**Params:** `TextDocumentPositionParams`
**Result:** `Location | Location[] | LocationLink[] | null` ([Android Git Repositories][1])

**Value**

* “mod file ↔ mod declaration” linkage (e.g., jump from `src/foo.rs` to `mod foo;`).
* Useful for building a module graph without parsing `mod` declarations yourself.

## 10.6.2 Join lines: `experimental/joinLines`

**Capability:** `experimental: { joinLines: boolean }` ([Android Git Repositories][1])
**Method:** `experimental/joinLines` (request)
**Params:** `{ textDocument, ranges: Range[] }` (multi-cursor aware)
**Result:** `TextEdit[]` ([Android Git Repositories][1])

**Value**

* Server-side “structure-aware join” (can remove braces / normalize syntax safely).

**Watch outs**

* Cursor placement after join is not specified; if you care, convert to snippet edits (or accept client discretion). ([Android Git Repositories][1])

## 10.6.3 Matching brace: `experimental/matchingBrace`

**Capability:** `experimental: { matchingBrace: boolean }` ([Android Git Repositories][1])
**Method:** `experimental/matchingBrace` (request)
**Params:** `{ textDocument, positions: Position[] }`
**Result:** `Position[]` ([Android Git Repositories][1])

**Value**

* Correctly disambiguates cases editors can’t (e.g., generics `<T>` vs `<` comparisons) because the server has the real Rust parser. ([Android Git Repositories][1])

---

# 10.7 “Introspection” views (debug outputs that are still agent-useful)

These are textual dumps; treat them as **diagnostic evidence**, not stable machine schemas:

* `rust-analyzer/syntaxTree` (request) → `string` parse-tree dump; optional `range` ([Android Git Repositories][1])
* `rust-analyzer/viewHir` (request) → `string` HIR dump for function containing cursor ([Android Git Repositories][1])
* `rust-analyzer/viewMir` (request) → `string` MIR dump for function containing cursor ([Android Git Repositories][1])
* `rust-analyzer/viewItemTree` (request) → `string` ItemTree dump ([Android Git Repositories][1])
* `rust-analyzer/viewFileText` (request) → `string` “file text as seen by server” (sync debugging) ([Android Git Repositories][1])
* `rust-analyzer/viewCrateGraph` (request) with `{ full: boolean }` → `string` (SVG image text) ([Android Git Repositories][1])

**Value**

* Agent debugging of “semantic drift”:

  * parse tree vs expected (syntaxTree)
  * name resolution / lowering checks (HIR/ItemTree)
  * “server and client disagree about file contents” (viewFileText)
  * dependency topology (crate graph)

**Watch outs**

* Free-form text; do not build strict parsers. Use for human/agent comprehension and attaching to issue reports.

---

# 10.8 “Open external information” + logs

## 10.8.1 Open server logs: `rust-analyzer/openServerLogs`

**Method:** `rust-analyzer/openServerLogs` (notification)
**Params:** `()` ([Rust Language][3])

**Value**

* One-shot “show me server logs” action for clients that can present them; extremely useful for diagnosing workspace load / proc-macro failures.

## 10.8.2 External docs lookup: `experimental/externalDocs`

**Method:** `experimental/externalDocs` (request)
**Params:** `TextDocumentPositionParams`
**Result:** `ExternalDocsResponse` ([Rust Language][4])

**Value**

* Cursor → documentation destination (often “open docs.rs / rustdoc / external docs” style behavior), without the client reimplementing Rust item identification.

**Watch outs**

* Response schema is extension-defined; treat it as “openable doc target(s)” and keep it opaque unless you commit to the exact RA version.

---

# 10.9 Agent-ready persistence contract for RA extensions

Persist extension-derived facts with **strong provenance**, because these endpoints are version-sensitive:

### 10.9.1 Capability snapshot

* `cap.server.experimental` (presence/shape of `runnables`, `ssr`, `parentModule`, `matchingBrace`, etc.) ([Android Git Repositories][1])
* `cap.client.experimental` toggles you enable (e.g., `serverStatusNotification`) ([Android Git Repositories][1])

### 10.9.2 Output records (normalize by “kind”)

* `macro.expansion`:

  * `(uri, position)` → `{ name, expansion }` ([Android Git Repositories][1])
* `runnables`:

  * `(uri, position?)` → `Runnable[]` with `kind` + structured args (cargo/shell) ([Android Git Repositories][1])
* `ssr`:

  * `SsrParams` → `WorkspaceEdit` (store query + parseOnly + selections) ([Android Git Repositories][1])
* `workspace.actions`:

  * reload/rebuildProcMacros timestamps (treat as “global invalidation events”) ([Android Git Repositories][1])
* `status`:

  * serverStatus stream + analyzerStatus dumps ([Android Git Repositories][1])
* `introspection.dumps`:

  * syntaxTree/viewHir/viewMir/viewItemTree/viewCrateGraph/viewFileText blobs (debug evidence) ([Android Git Repositories][1])
* `logs/docs`:

  * openServerLogs invoked; externalDocs response ([Rust Language][3])

If you want the next deep dive in the same style, the natural continuation is **11) Configuration & “knob surfaces” as outputs** (how rust-analyzer settings materially change these extension outputs, and what an agent should persist as “environment config fingerprints”).

[1]: https://android.googlesource.com/toolchain/rustc/%2B/HEAD/src/tools/rust-analyzer/docs/dev/lsp-extensions.md "LSP Extensions"
[2]: https://rust-lang.github.io/rust-analyzer/rust_analyzer/lsp/ext/enum.ExpandMacro.html "ExpandMacro in rust_analyzer::lsp::ext - Rust"
[3]: https://rust-lang.github.io/rust-analyzer/rust_analyzer/lsp/ext/enum.OpenServerLogs.html "OpenServerLogs in rust_analyzer::lsp::ext - Rust"
[4]: https://rust-lang.github.io/rust-analyzer/rust_analyzer/lsp/ext/enum.ExternalDocs.html "ExternalDocs in rust_analyzer::lsp::ext - Rust"

## 11) Configuration & “knob surfaces” as outputs (rust-analyzer)

### 11.1 The two LSP config planes you must model

#### 11.1.1 Bootstrap config: `initialize.initializationOptions`

* rust-analyzer explicitly treats **`InitializeParams.initializationOptions`** as the *initial configuration blob*. ([Rust Analyzer][1])
* Even though LSP types it as `any?`, rust-analyzer expects a **JSON object** built from its settings list. ([Rust Analyzer][1])
* Mapping rule (RA-specific): remove the `rust-analyzer.` prefix and interpret the remainder as a **JSON path**.

  * Example from RA docs: enabling proc macros + build scripts becomes:

    ```json
    {
      "cargo": { "buildScripts": { "enable": true } },
      "procMacro": { "enable": true }
    }
    ```

    ([Rust Analyzer][1])

**Value**

* This is the only configuration that is guaranteed to exist before any other request flow starts.
* Persisting this blob is a **hard reproducibility requirement**: it changes diagnostics, completions, inlay hints, semantic tokens, macro expansion, etc.

**Watch outs**

* Different editors expose different config UIs, but they all ultimately funnel into LSP messages; do not assume a file-based config exists. ([Rust Analyzer][1])
* rust-analyzer mentions a **work-in-progress** `rust-analyzer.toml` file mode (project root or user config dir), but also warns many options aren’t supported yet—treat it as non-authoritative for now. ([Rust Analyzer][1])

---

#### 11.1.2 Runtime config (the “pull model”): `workspace/configuration` + `workspace/didChangeConfiguration`

LSP 3.17 defines a *pull* configuration system:

##### A) Server pulls config: `workspace/configuration`

* **Method:** `workspace/configuration` (server → client request) ([Microsoft GitHub][2])
* **Client capability gate:** `workspace.configuration: boolean` ([Microsoft GitHub][2])
* **Params:**

  ```ts
  interface ConfigurationParams { items: ConfigurationItem[] }
  interface ConfigurationItem {
    scopeUri?: URI
    section?: string
  }
  ```

  ([Microsoft GitHub][2])
* **Result:** `LSPAny[]` (ordered 1:1 with items; `null` if client can’t provide scoped value) ([Microsoft GitHub][2])

**Value**

* Lets rust-analyzer request *multiple sections in one roundtrip* and request scoped values (per-workspace-folder or per-file) via `scopeUri`. ([Microsoft GitHub][2])
* The server-defined `section` string does **not** need to match how the client stores settings; client is responsible for mapping. ([Microsoft GitHub][2])

**Watch outs**

* If the client does not implement `workspace/configuration`, rust-analyzer configuration becomes fragile; this is a known integration pain point for non-mainstream clients. ([GitHub][3])
* Scoped configs: if you pass `scopeUri`, the client should return configuration scoped to that resource; if it can’t, it must return `null` in that slot—your agent should treat `null` as “unknown”, not “false”. ([Microsoft GitHub][2])

##### B) Client notifies “something changed”: `workspace/didChangeConfiguration`

* **Method:** `workspace/didChangeConfiguration` (client → server notification) ([Microsoft GitHub][2])
* **Params:** `DidChangeConfigurationParams { settings: LSPAny }` ([Microsoft GitHub][2])
* **Client capability:** `workspace.didChangeConfiguration.dynamicRegistration?: boolean` (for dynamic registration) ([Microsoft GitHub][2])

**Critical spec nuance**

* LSP explicitly positions `workspace/configuration` as the replacement for the old push model; if the server still needs to react to config changes because it caches config values, it should register for “empty” configuration change notifications (dynamic registration pattern). ([Microsoft GitHub][2])

**Value**

* Gives you a *reproducible invalidation signal*: on receipt of didChangeConfiguration, the server is expected to re-pull config and recompute affected caches.

**Watch outs**

* `settings: LSPAny` is intentionally underspecified. Many clients send `{}` (empty) just to trigger server re-pull; you cannot treat `settings` as authoritative content unless you control the client. ([Microsoft GitHub][2])

---

### 11.2 rust-analyzer-specific configuration “schema” behavior (practically important)

#### 11.2.1 Config keyspace is large; treat it as a versioned API surface

rust-analyzer publishes an explicit list of settings (e.g., `rust-analyzer.cargo.allTargets`, `rust-analyzer.cargo.autoreload`, `rust-analyzer.procMacro.enable`, `rust-analyzer.inlayHints.*`, `rust-analyzer.rustfmt.overrideCommand`, etc.). ([Rust Analyzer][1])

**Value**

* This settings list is the “schema” you should use for:

  * config fingerprinting
  * diffing between runs
  * explaining why outputs differ (e.g., references missing because `cargo.noDeps=true`, macros missing because procMacro disabled, etc.)

**Watch outs**

* Some settings explicitly require restart to take effect (e.g., sysroot/sysrootSrc are documented as restart-required). If you flip them via config messages mid-session, you may not see changes. ([Rust Analyzer][1])

#### 11.2.2 Verification channel: RA_LOG (config observability)

rust-analyzer recommends setting `RA_LOG=rust_analyzer=info` and inspecting logs for:

* the JSON config it actually received
* the updated internal config ([Rust Analyzer][1])

**Value**

* This is your “ground truth” when agent-collected outputs don’t match expected settings (client mapping bugs, wrong scope, silent defaults).

---

### 11.3 High-leverage reproducibility artifacts you should persist (treat them as “outputs”)

#### 11.3.1 Config fingerprint (minimum sufficient)

Persist:

* `serverInfo.name/version` (from `initialize` result)
* `initialize.initializationOptions` (raw JSON)
* last pulled values from `workspace/configuration` (ordered results + the `ConfigurationItem[]` you asked for)
* a stable hash of the above (e.g., SHA256 over canonical JSON)

**Why**: without this, “semantic outputs” (diagnostics, completions, inlay hints, semantic tokens, macro expansion) are not comparable across runs. ([Rust Analyzer][1])

#### 11.3.2 Config invalidation log

Persist every time you observe:

* `workspace/didChangeConfiguration` received (timestamp + raw params.settings)
* subsequent `workspace/configuration` pulls and returned values
* any explicit “big hammer” actions you invoked that are config-adjacent (e.g., `rust-analyzer/reloadWorkspace`, `rust-analyzer/rebuildProcMacros`) because they change the “analysis universe” even if config didn’t change (you already captured these in section 10). ([Microsoft GitHub][2])

---

### 11.4 Integration watch-outs (the stuff that breaks non-VSCode clients)

#### 11.4.1 Missing `workspace/configuration` support = degraded capability surface

Multiple ecosystems call out that lacking `workspace/configuration` support breaks or limits language server configuration in practice, and rust-analyzer users have repeatedly raised it as an integration requirement. ([GitHub][3])

**Agent implication**

* Persist `clientCapabilities.workspace.configuration` (boolean) and treat it as a first-class “environment constraint”.
* If false, expect:

  * settings stuck at defaults
  * partial feature availability
  * confusing mismatches between docs and observed behavior

#### 11.4.2 Capability diffs can masquerade as “config” diffs

Even with identical rust-analyzer settings, different clients expose different LSP capabilities (resolve support, snippet edits, etc.), which changes observed outputs (e.g., missing certain assists in non-VSCode clients). ([GitHub][4])

---

### 11.5 “Agent-ready” contract: how to treat config as data products

**Persist as tables (recommended):**

* `ra_config_bootstrap`: `{ session_id, initializationOptions_json, hash }` ([Rust Analyzer][1])
* `ra_config_pull_requests`: `{ session_id, items[], scopeUri?, section?, ts }` ([Microsoft GitHub][2])
* `ra_config_pull_results`: `{ session_id, idx, value_json_or_null, ts }` (ordered 1:1) ([Microsoft GitHub][2])
* `ra_config_change_events`: `{ session_id, settings_payload_json, ts }` ([Microsoft GitHub][2])
* `ra_config_effective_snapshot`: merged view (bootstrap + latest pulls), plus `serverInfo.version` and capability fingerprint

**Validation invariants:**

* If `workspace.configuration=false`, mark `effective_snapshot.confidence="low"` and record “defaults likely”.
* If you see `didChangeConfiguration` but no subsequent `workspace/configuration` pull, mark `effective_snapshot.stale=true` (server caching mismatch relative to spec guidance). ([Microsoft GitHub][2])

[1]: https://rust-analyzer.github.io/book/configuration.html "Configuration - rust-analyzer"
[2]: https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/ "Specification"
[3]: https://github.com/rust-analyzer/rust-analyzer/issues/4267?utm_source=chatgpt.com "`workspace/configuration` clarification · Issue #4267 · rust- ..."
[4]: https://github.com/rust-analyzer/rust-analyzer/issues/11577?utm_source=chatgpt.com "I get \"Extract into variable\" assist but not \"Extract into function\""
