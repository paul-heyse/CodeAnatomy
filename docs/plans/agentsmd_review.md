Below is what “best practice” looks like right now for **AGENTS.md** (cross-agent, Codex-first) and **CLAUDE.md** (Claude Code memory), with concrete patterns for **skills**, **MCP tools**, **code standards**, and “**use the advanced stack, don’t hack around it**”—*without* turning these files into unreadable sludge.

---

## 1) The core design constraint: what gets loaded *every time* vs *on demand*

The highest-leverage principle is **progressive disclosure**:

* **Always-loaded**: `AGENTS.md` (for many coding agents) and `CLAUDE.md`/`.claude/CLAUDE.md` (for Claude Code) are pulled into context early and repeatedly, so they must be **short** and **high-signal**. ([OpenAI Developers][1])
* **On-demand**: **Skills** load only their metadata at startup, and load full instructions only when invoked; supporting files load only when needed. Use this for anything “sometimes relevant.” ([Claude Code][2])
* **Deterministic enforcement**: If something must happen “every time,” don’t rely on prose—use **hooks** (Claude Code) or CI. Claude’s docs explicitly position hooks as the deterministic alternative to advisory instructions. ([Claude Code][3])

This split is your answer to “how do I capture everything without overwhelming context?”

---

## 2) AGENTS.md: what to include, how to keep it small, how it’s actually discovered

### 2.1 How Codex (and many others) load it

Codex constructs an **instruction chain** and concatenates `AGENTS.md` files from:

1. a global file in `~/.codex` (optionally `AGENTS.override.md`), then
2. the repo root down to your current directory, taking at most **one** instruction file per directory, with `AGENTS.override.md` taking precedence over `AGENTS.md`. ([OpenAI Developers][1])

Codex stops adding files once a size limit is reached (default **32 KiB**, configurable), and recommends using nested files rather than bloating one file. ([OpenAI Developers][1])

### 2.2 Best-practice contents (AGENTS.md should be “a briefing packet”)

A tight `AGENTS.md` usually covers:

1. **Commands first** (fast feedback loop)
   Put the exact commands the agent should run near the top—install, lint/format, typecheck, tests. This is consistently called out as a differentiator in large-scale analysis of agents.md usage. ([The GitHub Blog][4])

2. **Boundaries / safety**
   “Never touch” directories, secrets policy, “ask before adding deps,” etc. Again: repeatedly observed as a high-value section. ([The GitHub Blog][4])

3. **Project map**
   A few pointers to “start here” files and where key subsystems live. (“One real file path beats paragraphs.”) ([The GitHub Blog][4])

4. **Progressive disclosure pointers**
   Link to deeper docs / design specs instead of copying them. The open AGENTS.md guidance is explicitly “README for agents; keep README for humans.” ([Agents][5])

5. **MCP + skills** (minimal, but explicit)
   Put *just enough* so the agent actually uses them (see §4 and §5). OpenAI’s Docs MCP page even provides a snippet intended to live inside `AGENTS.md` so agents reliably consult an MCP docs server. ([OpenAI Developers][6])

### 2.3 The “keep it small” rule that actually works

Use this rubric (mirrors Claude’s own guidance about pruning CLAUDE.md):

> For each line: **Would removing this cause the agent to make mistakes?**
> If not, move it to a referenced doc, a skill, or a hook.

Claude Code documentation explicitly warns that long instruction files cause rules to get lost. ([Claude Code][3])
Codex also enforces a hard-ish byte cap and recommends splitting across nested directories. ([OpenAI Developers][1])

---

## 3) CLAUDE.md: how Claude Code loads it, and the modern “modular rules + imports” approach

### 3.1 Claude Code memory hierarchy (built-in progressive disclosure)

Claude Code supports multiple memory locations:

* Project memory: `./CLAUDE.md` or `./.claude/CLAUDE.md` (team-shared)
* Project rules: `./.claude/rules/*.md` (topic-specific, can be path-scoped)
* User memory: `~/.claude/CLAUDE.md`
* Local project memory: `./CLAUDE.local.md` (gitignored automatically) ([Claude Code][7])

It also supports:

* **Imports** in CLAUDE.md via `@path/to/file`, with an approval prompt the first time external imports are encountered. ([Claude Code][7])
* **Nested discovery**: CLAUDE.md files are discovered recursively up and down directories; some are loaded on demand when Claude starts working in subtrees. ([Claude Code][7])

### 3.2 What belongs in CLAUDE.md (per Anthropic)

Claude’s own best-practices page gives a very practical include/exclude list:

✅ include:

* commands Claude can’t guess
* testing instructions / preferred runners
* repo etiquette and non-obvious architectural decisions
* environment quirks (env vars, tooling constraints)

❌ exclude:

* things Claude can infer by reading code
* huge API docs (link instead)
* long tutorials, per-file descriptions, frequently changing info

…and it explicitly says: **use skills for domain knowledge/workflows that aren’t always relevant**. ([Claude Code][3])

Also: CLAUDE.md can be used to **control compaction** behavior (what survives `/compact`), which is a very “real world” way to keep rules from evaporating in long sessions. ([Claude Code][3])

### 3.3 A key caution: don’t make CLAUDE.md your linter

A widely-shared failure pattern is stuffing CLAUDE.md with formatting rules; better practice is to rely on deterministic tooling (formatter/linter) and—if needed—enforce via hooks. ([HumanLayer][8])

---

## 4) Skills: how to use skills to offload “big guidance” from AGENTS.md / CLAUDE.md

### 4.1 Claude Code skills (repo + nested skills)

Claude Code skills live in `.claude/skills/<skill-name>/SKILL.md` (or higher scopes like `~/.claude/skills/...`) and can be discovered from nested directories for monorepos. The docs emphasize:

* `SKILL.md` is required
* use supporting files (templates, examples, scripts, reference docs) to keep `SKILL.md` focused
* YAML frontmatter (name/description/etc.), and optional `allowed-tools` to pre-approve tool use ([Claude Code][2])

### 4.2 Codex skills (same “Agent Skills” concept)

OpenAI’s Codex also supports skills: a folder with `SKILL.md` plus optional scripts/references/assets, explicitly framed as **progressive disclosure** (metadata at startup; full instructions when activated). ([OpenAI Developers][9])

### 4.3 The practical rule

Use skills for:

* “how to do X here” workflows (release, deploy, generate fixtures, run a specialized pipeline)
* deep reference material (schemas, protocol notes, architecture deep dives)
* specialized tool usage (your MCP server interfaces)

…and keep AGENTS.md / CLAUDE.md as the **index + invariants**.

---

## 5) MCP tools: make them discoverable, safe, and actually used

### 5.1 Codex MCP (configuration + what to document)

Codex supports STDIO and streamable HTTP MCP servers (including OAuth flows), configured via `~/.codex/config.toml` or project `.codex/config.toml`. ([OpenAI Developers][10])

OpenAI’s “Docs MCP” page is a good pattern to emulate:

* keep server names short/descriptive
* add a one-line directive in `AGENTS.md` telling the agent to use a specific MCP server when relevant (otherwise it often won’t) ([OpenAI Developers][6])

### 5.2 Claude Code MCP (configuration + guardrails)

Claude Code’s MCP docs are unusually explicit about:

* transport types (HTTP recommended; SSE deprecated; local stdio)
* **prompt injection risk** with untrusted MCP servers
* output size warnings and an env var to raise max tool output tokens (`MAX_MCP_OUTPUT_TOKENS`)
* dynamic tool updates (`list_changed`) ([Claude Code][11])

### 5.3 What to put in AGENTS.md / CLAUDE.md about MCP

Keep it to a short “routing table”:

* **Which MCP servers exist** (name → purpose)
* **When to use them**
* **Output discipline** (summarize; don’t dump 50k tokens)
* **Safety** (trusted servers only; ask before adding new servers; never exfiltrate secrets)

Everything else (full API reference, schemas, examples) goes into:

* `.claude/skills/<mcp-something>/references/...`, and/or
* `.claude/rules/<topic>.md` with `paths:` scoping, and/or
* a docs folder referenced by imports.

---

## 6) Capturing “use advanced Python features; no easy workarounds” without bloating

This is best handled as **a small invariant in AGENTS.md/CLAUDE.md**, plus **deep rules/skills**.

### 6.1 Put *only* the invariant in always-loaded files

Example invariant wording (short enough to survive):

* “Prefer existing project abstractions and advanced library features already in use; don’t introduce regex/string hacks or Python loops where a canonical engine exists. If you must deviate, explain why and add a test.”

### 6.2 Put the actual “do this, not that” in scoped rules or skills

For your stack, you already have “advanced features” expectations that are too big for always-loaded context. For example:

* **DataFusion-first / streaming-first**: prefer streaming surfaces and Arrow C Stream interop; treat plans as artifacts. 
* **LibCST for lossless parsing/rewrites** and byte-span-accurate anchoring for tooling pipelines. 
* **FastMCP**: tools/resources/prompts, typed args, Context DI, and structured output conventions. 
* **CPG semantics**: canonicalize to byte spans; treat relationship compilation as a “relational compiler.” 

Best practice is to put these into:

* `.claude/rules/python-stack.md` (and path-scope if needed) ([Claude Code][7])
* and/or a skill like `.claude/skills/codeanatomy-pipeline/SKILL.md` with `references/` docs for the deep material. ([Claude Code][2])

---

## 7) Templates you can copy-paste

### 7.1 Minimal but high-leverage `AGENTS.md` (root)

````md
# AGENTS.md

## Quickstart
- Install: `uv sync`  (or your actual command)
- Tests: `pytest -q`
- Lint/format: `ruff check . && ruff format .`
- Typecheck (if applicable): `mypy -p yourpkg`

## Project map (start here)
- Architecture overview: docs/ARCHITECTURE.md
- Pipeline entrypoints: src/...
- Conventions: docs/CONVENTIONS.md

## Non-negotiables
- Do not add new dependencies without asking.
- Do not bypass existin:contentReference[oaicite:29]{index=29}uick hacks” (regex parsing, ad-hoc file IO, pandas loops) if a canonical library/pipeline exists.
- Af:contentReference[oaicite:30]{index=30} the checks above and report results.

## MCP / tools
- If you need external docs or system integratio:contentReference[oaicite:31]{index=31}d MCP servers (see docs/MCP.md).
- Keep tool outputs small: summarize + link to artifacts instead of dumping lo:contentReference[oaicite:32]{index=32}al `CLAUDE.md` that stays small by importing and delegating
```md
# CLAUDE.md

Follow @AGENTS.md.

## Context hygiene
- When compacting, preserve: (1) list of modified files, (2) commands run + results, (3) open TODOs.

## Project-specific rules live in:
- @.claude/rules/  (scoped and modular)
- Skills in @.claude/skills/ for workflows and deep references
````

Imports and `.claude/rules/*.md` are first-class mechanisms in Claude Code specifically. ([Claude Code][7])

---

## 8) How to keep these docs “paid attention to”

This is the maintenance loop that works in practice:

1. **Measure drift**: whenever an agent makes the same mistake twice, add *one line* to AGENTS/CLAUDE **or** add a scoped rule/skill. (Claude’s own best practices call out that repeated corrections usually mean clutter or missing constraints.) ([Claude Code][3])
2. **Prune** quarterly: delete anything the agent already does correctly without it. ([Claude Code][3])
3. **Enforce deterministically**: format/lint/test via CI or hooks, not prose. ([Claude Code][3])
4. **Prefer examples over essays**: point to 1–3 “golden” files that demonstrate the desired pattern. ([The GitHub Blog][4])

---

If you want, I can also provide a **recommended file tree** for a monorepo-style setup (root + per-subdir AGENTS.md, plus `.claude/rules/` and `.claude/skills/` layout) that minimizes duplication while making the “advanced Python stack” rules apply only where they matter.

[1]: https://developers.openai.com/codex/guides/agents-md/ "Custom instructions with AGENTS.md"
[2]: https://code.claude.com/docs/en/skills "Extend Claude with skills - Claude Code Docs"
[3]: https://code.claude.com/docs/en/best-practices "Best Practices for Claude Code - Claude Code Docs"
[4]: https://github.blog/ai-and-ml/github-copilot/how-to-write-a-great-agents-md-lessons-from-over-2500-repositories/ "How to write a great agents.md: Lessons from over 2,500 repositories - The GitHub Blog"
[5]: https://agents.md/ "AGENTS.md"
[6]: https://developers.openai.com/resources/docs-mcp/ "Docs MCP"
[7]: https://code.claude.com/docs/en/memory "Manage Claude's memory - Claude Code Docs"
[8]: https://www.humanlayer.dev/blog/writing-a-good-claude-md "Writing a good CLAUDE.md | HumanLayer Blog"
[9]: https://developers.openai.com/codex/skills/ "Agent Skills"
[10]: https://developers.openai.com/codex/mcp/ "Model Context Protocol"
[11]: https://code.claude.com/docs/en/mcp "Connect Claude Code to tools via MCP - Claude Code Docs"

I reviewed your current `AGENTS.md`  against the **actual repo snapshot** in `CodeAnatomy_Post_Semantic_Model_Implementation.tar.gz` (notably `pyproject.toml`, `pyrefly.toml`, `pyrightconfig.json`, `scripts/*`, and the `src/` layout) and the **three skills** in `skills.zip` (`cq`, `ast-grep`, `datafusion-stack`).

### The biggest structural problems (why agents will ignore it)

1. **It will be truncated by many agent loaders.**
   Your file is ~92KB. Codex-style loaders commonly cap around ~32KB for accumulated instructions, so only the *front chunk* reliably lands in context. Net effect: the most important bits might be missing while the agent still “believes” it read everything.

2. **Large parts are for a different codebase (and are actively misleading).**
   The file references `src/CodeIntel_common/...`, Pandera schema registries, Ibis 11 patterns, `DuckDBPolicyBackend`, Problem Details, etc. Those paths/APIs **do not exist** in this snapshot. Agents will waste cycles hunting phantom modules or will infer wrong architectural constraints.

3. **Several “hard rules” contradict reality in this repo snapshot.**

   * It says “pyright strict” but `pyrightconfig.json` has `"typeCheckingMode": "basic"`.
   * It says “no monkeypatch / no unittest.mock”, but tests in this snapshot use `monkeypatch` widely and also import `unittest.mock` in places.
   * It asserts Bandit-like security enforcement, but Ruff `select` in this snapshot does **not** include `S` (flake8-bandit).

4. **You’re duplicating the ruff/typing ruleset in prose.**
   Since the authoritative rule set is in `pyproject.toml`, re-listing every “must/must not” becomes stale *immediately* and bloats context so the *important* instructions don’t get followed.

---

## What you should do instead: a layered instruction system

Think “**one-screen AGENTS.md + path-scoped supplements + skills for deep reference**”.

### Layer 0 (always loaded): `./AGENTS.md` (target: **8–16KB**)

Purpose: *operating protocol + routing*.
Contents should be only what (a) cannot be inferred, and (b) prevents repeated mistakes.

**Keep:**

* `uv` / Python 3.13.11 invariants + bootstrap scripts
* one “run all checks” command
* “no suppressions / fix structurally”
* repo map (real modules)
* “prefer project primitives; no easy hacks”
* skills index (short routing table)

**Remove from root AGENTS.md:**

* the giant rule-by-rule compliance list
* full testing charter
* long how-to guides (query engine, wiring packs, etc.)
* any content referencing modules that don’t exist in this repo snapshot

### Layer 1 (path-scoped, auto-loaded when working there)

Add small `AGENTS.md` files in subtrees that need specialized rules:

* `tests/AGENTS.md` — test policy **as actually practiced** (including when monkeypatch is allowed), fixture/golden conventions.
* `rust/AGENTS.md` — “how to rebuild”, maturin/cargo conventions, when to update `dist/wheels` + plugin manifest.
* `src/datafusion_engine/AGENTS.md` — DataFusion runtime profiles, plugin/UDF expectations; explicitly route to the `datafusion-stack` skill.
* `src/semantics/AGENTS.md` — span canonicalization rules (bstart/bend), join strategies invariants.
* `src/cli/AGENTS.md` — Cyclopts patterns, CLI golden tests, exit codes conventions.

This keeps the root file small while still ensuring the right rules are loaded “just in time”.

### Layer 2 (on-demand): skills (`.claude/skills/**/SKILL.md` and/or `.codex/skills/**/SKILL.md`)

You already have exactly the right split:

* **`cq`** = high-recall repo analysis
* **`ast-grep`** = structural matching/rewrites
* **`datafusion-stack`** = “don’t guess APIs; probe and follow local patterns”

In `AGENTS.md`, you only need a **3–6 line routing table** that causes agents to actually use them.

---

## Concrete restructure: what to change in *your* AGENTS.md

### 1) Replace the “giant compliance contract” with a *pointer + a short summary*

Instead of listing rules, do this:

* “Ruff is canonical. The full enforced rules live in `pyproject.toml` under `[tool.ruff]` and `[tool.ruff.lint]`.”
* “No `noqa` / no `type: ignore` / no rule disables unless pre-existing + justified.”
* Mention only the few non-obvious enforcement points that change behavior:

  * ban relative imports (`ban-relative-imports = "all"`)
  * complexity caps (`max-complexity=10`, `max-branches=12`, `max-returns=6`)
  * NumPy docstring convention (`pydocstyle numpy`)
  * `TC` family is enabled (but your config ignores `TC001–TC004`, so don’t claim those are enforced)

### 2) Fix “strictness claims” to match config (or tighten config)

Pick one:

* **Option A (doc-only):** AGENTS says “pyright basic (IDE baseline), pyrefly is the strict gate”.
* **Option B (make it true):** change `pyrightconfig.json` to `strict`.
  Either is fine — but AGENTS must not lie, or agents stop trusting it.

### 3) Move test charter into `tests/AGENTS.md` and align it to reality

Right now your root file says “no monkeypatch ever” but the suite uses it extensively. A sane near-term policy:

* “Prefer DI and config where feasible.”
* “Monkeypatch is allowed for env vars, filesystem, and tight seams in unit tests.”
* “Avoid patching deep internals across modules unless there’s no seam.”

If you *want* the “no monkeypatch” world, keep it as a **migration goal** with an issue link, not as a hard rule today.

### 4) Add a short “Use project primitives; no easy hacks” section that is repo-specific

In this repo snapshot, the most important “advanced features / no hacks” guidance is:

* Persisted artifacts → **msgspec** contracts (`src/serde_artifacts.py`, registry in `src/serde_schema_registry.py`, encoding policy in `src/serde_msgspec.py`)
* Schema/view definitions → `src/schema_spec/**` (don’t invent ad-hoc schemas)
* Spans → treat `bstart/bend` as canonical in semantics/CPG layers
* DataFusion + plugin/UDF surfaces → use the engine modules + rebuild scripts (`scripts/bootstrap.sh`, `scripts/rebuild_rust_artifacts.sh`)

Put that in the root AGENTS as *four bullets*, not a manifesto.

### 5) Add a skills “routing table” near the top

Agents follow what they see early. Put this in the first ~80 lines:

* “Before refactors: use `cq` to find callsites/impact.”
* “Before structural rewrites: use `ast-grep`.”
* “Before touching DataFusion/Delta/Rust UDF APIs: use `datafusion-stack` skill; probe versions; don’t guess.”

---

## A proposed replacement `AGENTS.md` (root) you can paste

This is intentionally short and “always-load safe” (~1–2 screens per section). Adapt names if you want.

````md
# AGENTS.md — CodeAnatomy Agent Operating Protocol

## Read this first (non-negotiables)
- Use the **uv-managed env**. Prefer `uv run …` (or an explicit `.venv/bin/python` when a script hardcodes it).
- Python is pinned to **3.13.11**. Bootstrap outside sandboxes with:
  - `bash scripts/bootstrap.sh`
  - In restricted agent runners: `bash scripts/bootstrap_codex.sh`
- Do not suppress quality gates. Avoid new `# noqa`, `# type: ignore`, or rule disables. Fix causes structurally.
- Prefer project primitives over “easy workarounds”:
  - Persisted artifacts/contracts: `src/serde_artifacts.py` + `src/serde_schema_registry.py` + `src/serde_msgspec.py`
  - Table/view schema specs: `src/schema_spec/**`
  - Span semantics: canonical byte spans `bstart/bend` (avoid line/col as primary identity)
  - DataFusion/Delta/UDF/plugin: `src/datafusion_engine/**` + `scripts/rebuild_rust_artifacts.sh`
- Before any refactor: run the full gate (below) and keep files you touched **0-error**.

## One-command quality gate (run after edits)
```bash
uv run ruff format \
  && uv run ruff check --fix \
  && uv run pyrefly check \
  && uv run pyright \
  && uv run pytest -q
````

## Repo map (start points)

* `src/cli/**` — Cyclopts CLI entrypoint and commands (`codeanatomy …`)
* `src/engine/**` — runtime/session/materialization orchestration
* `src/hamilton_pipeline/**` — Hamilton DAG modules + driver factory
* `src/datafusion_engine/**` — DataFusion session/catalog/views/sql/udf/delta integration
* `src/schema_spec/**` — schema + view specs + contracts
* `src/cpg/**`, `src/semantics/**`, `src/relspec/**` — semantic model + joins + relationship compilation
* `rust/**` — Rust extensions, plugin host,I, and DataFusion bindings

## Skills / analysis tools (use these instead of guessing)

* **cq** (high-recall impact/calls/imports): use before changing signatures or moving code
* **ast-grep** (structural search/rewrites): use for refactors, not regex/grep
* **datafusion-stack**: if touching DataFusion/PyArrow/Delta/Rust-UDF APIs, probe and follow existing patterns

## Tooling binaries in this repo

* `./rg` and `./ast-grep` are shipped for deterministic local analysis when needed.

## Ruff / typing sources of truth

* Ruff config + rule selection: `pyproject.toml` → `[tool.ruff]`, `[tool.ruff.lint]`
* Pyrefly strict gate: `pyrefly.toml`
* Pyright baseline/IDE gate: `pyrightconfig.json`

If instructions here conflict with repo configs, treat **repo configs as canonical** and update this file.

```

---

## Suggested new files (small, scoped, high ROI)
These are the splits that will immediately reduce “ignored guidance”:

1) `tests/AGENTS.md`
- test categories used here (unit/integration/golden/msgspec_contract)
- allowed patching policy that matches reality (monkeypatch is currently used)
- golden fixture update rules

2) `rust/AGENTS.md`
- “when you edit rust: run `bash scripts/rebuild_rust_artifacts.sh`”
- what artifacts should exist afterward (`dist/wheels/*`, `build/datafusion_plugin_manifest.json`, `rust/datafusion_ext_py/plugin/*`)

3) `src/datafusion_engine/AGENTS.md`
- “use datafusion-stack skill; don’t guess APIs”
- where runtime profiles and plugin loading live
- common footguns (plugin path, UDF registry, delta maintenance scripts)

4) `src/semantics/AGENTS.md`
- canonical span policy (`bstart/bend`), join strategy invariants, what must remain stable

---

## Two cleanups you should do (because agents will notice the mismatch)
- **Decide what “strict typing” means for pyright** in this repo. Either:
  - keep pyright “basic” and say “pyrefly is the strict gate”, **or**
  - set pyright to strict and enforce it.  
  Right now AGENTS claims strict but config is basic → trust erosion.

- **Fix the “no monkeypatch” claim** (either remove, or make it a migration target). Your tests currently use it extensively.

---

If you want, I can also draft the **`tests/AGENTS.md`** and **`rust/AGENTS.md`** contents based on the actual test suite and rebuild scripts in this snapshot, and a **`src/datafusion_engine/AGENTS.md`** that explicitly routes DataFusion work to the `datafusion-stack` skill without duplicating that skill’s reference docs.
::contentReference[oaicite:1]{index=1}
```

