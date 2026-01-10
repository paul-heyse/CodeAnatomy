Below is a deep‑dive technical overview of **Pyrefly**—what it is, how it works, what it can do today, how to wire it into IDEs/CI, and where it stands relative to Pyre for **LibCST** metadata (including concrete guidance if you want to experiment with Pyrefly‑backed metadata). I’ve modeled the breadth and structure on your FAISS example: architecture + capabilities + operations + integration + migration + limitations + prescriptive best practices.

---

## 1) What is Pyrefly?

**Pyrefly** is Meta’s next‑generation **Python type checker and language server**. It replaces the legacy OCaml‑based Pyre with a **Rust** implementation focused on speed, incrementality, and a first‑class IDE experience. It ships as a CLI (`pyrefly`) and a language server (`pyrefly lsp`) with official VS Code and OpenVSX extensions. Pyrefly is actively developed (e.g., v0.41.0 released Nov 11, 2025). ([Pyrefly][1])

High‑level design choices include: module‑centric checking (solve a whole module quickly rather than fine‑grained symbol queries), substantial local **type inference** (variables and returns; not parameters), **flow‑sensitive refinement**, and a pragmatic approach to empty containers (infer on first use). Internally, it’s split into Rust crates including `pyrefly_types`, `pyrefly_config`, a bundled **typeshed**, and a WASM target used by the online sandbox. ([GitHub][2])

Performance claims are aggressive (e.g., *1.85M LoC/sec* headline demos and large‑repo IDE responsiveness), though as with any benchmark, treat them as directional. ([Pyrefly][3])

---

## 2) Architecture & Core Algorithms

**Pipeline (per module):**

1. Compute module exports (resolve `import *` transitively).
2. Convert the CST/AST into **bindings** and scope—tracking definitions/uses and control‑flow facts.
3. **Solve** those bindings, allowing placeholders (`Var`) to stand in when recursion/unknowns occur; refine them as constraints are solved.
   This favors raw throughput and simpler incremental behavior at the module level. ([GitHub][2])

**Type system features & inference posture:**

* **Inference** everywhere except function parameters; returns are inferred.
* **Flow types** refine static types along control flow (e.g., literal narrowing).
* **First‑use inference** for unsolved type variables/empty containers (`[]`, `{}`), configurable via `infer-with-first-use`. ([GitHub][2])

**Bundled stubs:** Pyrefly ships with a bundled **typeshed** but can be pointed at a custom typeshed via `typeshed-path`. ([Pyrefly][4])

---

## 3) CLI surface & operational model

**Primary commands (today):**

* `pyrefly check` — run the type checker. Useful flags include `--summarize-errors`, `--suppress-errors`, `--remove-unused-ignores`, `--baseline`, and `--update-baseline`. Baseline files let you “freeze” current issues and only report regressions. ([Pyrefly][5])
* `pyrefly init` — generate/extend config, and **auto‑migrate** from mypy/pyright configs into a `pyrefly.toml` / `[tool.pyrefly]` section. ([Pyrefly][5])
* `pyrefly infer` — experimental **auto‑annotation** that writes inferred types back to files (batch by path). Review changes before committing. ([Pyrefly][6])
* `pyrefly lsp` — run the **Language Server Protocol** endpoint for IDE/editor integrations. ([Pyrefly][7])
* `pyrefly dump-config` — print the **effective config** seen by Pyrefly for import/search‑path and environment debugging. ([Pyrefly][8])

**Pre‑commit & CI:** Official hooks are available (`pyrefly-typecheck-system` or version‑pinned) and recommended to run **full‑repo** checks rather than per‑file hooks. Example GitHub Actions snippets are in the docs. ([GitHub][9])

---

## 4) Configuration model (TOML; `pyrefly.toml` or `pyproject.toml`)

**Configuration precedence**: CLI flags → config file → defaults. Pyrefly will **auto‑discover** Python interpreter/version/platform & third‑party site‑packages unless disabled (`skip-interpreter-query`). Defaults to Python **3.13** if it cannot query. ([Pyrefly][4])

**Key knobs:**

* **Project files**: `project-includes`/`project-excludes`, **ignore files** (`use-ignore-files`), and Unix‑style globbing. ([Pyrefly][4])
* **Imports & environment**: `search-path`, `disable-search-path-heuristics`, `site-package-path`, interpreter selection, conda/venv discovery, `typeshed-path`. ([Pyrefly][4])
* **Error policy**: `[errors]` to enable/disable categories; `disable-type-errors-in-ide` for LSP‑only mode. ([Pyrefly][4])
* **Behavioral parity**: `untyped-def-behavior` to emulate mypy/pyright defaults; `permissive-ignores` to honor `# mypy: ignore` or `# pyre-ignore`. ([Pyrefly][4])
* **Sub‑configs** (per‑path overrides) let you emulate pyright execution environments and mypy per‑module toggles for specific error kinds. ([Pyrefly][10])

> **Note**: The config is still evolving; option names/semantics can change as the project matures. ([Pyrefly][11])

---

## 5) Import resolution & stubs

**Resolution strategy (absolute imports):** search **project search‑path** → **typeshed** → **fallback search path** (heuristic when no config root is found) → **site‑packages** path → error. Stubs (`.pyi` and `-stubs` packages) are preferred when available; otherwise fall back to source (`.py`). ([Pyrefly][8])

**Editable installs:** Avoid import‑hook‑based editable installs (the default in setuptools). Use **path‑based `.pth`** for compatibility with static analyzers (pip “compat/strict”, uv “configured”, etc.). ([Pyrefly][8])

**Debugging:** `pyrefly dump-config` shows the computed import roots/search paths per file. ([Pyrefly][8])

---

## 6) Language server & IDE capabilities

The VS Code / OpenVSX extension can **fully replace Pylance** (optionally disable particular LSP methods). Implemented features include: **go‑to‑definition, find‑references, document/workspace symbols, hover (types + docstrings), document highlights, signature help, completions, rename, diagnostics, semantic tokens, inlay hints**, plus **notebook support** (VS Code & JupyterLab). Pyrefly also reuses Pyright’s inlay‑hints settings for compatibility. ([Pyrefly][12])

Editors supported via LSP: VS Code, OpenVSX‑enabled editors (Windsurf/Cursor), Neovim (mason+lspconfig), coc.nvim/ALE, Emacs (eglot), Helix, Sublime, and a community JetBrains plugin. ([Pyrefly][7])

---

## 7) Error model, suppression, and baselines

Pyrefly’s diagnostics are organized into **error kinds** (e.g., `bad-assignment`, `missing-import`, `invalid-argument`, many others). These power fine‑grained suppression and config mapping from other checkers. ([Pyrefly][13])

**Suppressions and cleanup:**

* Inline: `# pyrefly: ignore` or `# pyrefly: ignore[bad-assignment]` (also honors `# type: ignore` by default; optionally mypy/pyre comments via `permissive-ignores`). ([Pyrefly][14])
* Automated: `pyrefly check --suppress-errors` and `--remove-unused-ignores`.
* **Baselines (experimental):** generate with `--baseline=... --update-baseline`, then check against it to only surface **new** issues. ([Pyrefly][14])

---

## 8) Ecosystem integrations & special support

* **Pydantic v2**: Experimental, **built‑in** support (no plugin) that mirrors Pydantic’s lax/strict validation choices and common config (e.g., `extra='forbid'`) to reduce false positives. ([Pyrefly][15])
* **Auto‑typing**: `pyrefly infer` annotates parameters/returns/containers; recommended in **small batches** with manual review. ([Pyrefly][6])
* **Migrations**: `pyrefly init` transforms configs from mypy/pyright and provides parity options (`untyped_def_behavior`, diagnostic mappings, sub‑configs). ([Pyrefly][10])
* **Relationship to Pyre**: Pyrefly is a **ground‑up rewrite** (Rust; no shared checker code) for performance, cross‑platform support (including Windows), and maintainability. ([Pyrefly][1])

---

## 9) “Can Pyrefly replace **Pyre** for **LibCST metadata** resolution?”

### Short answer

**Not as a drop‑in.** LibCST’s `TypeInferenceProvider` is explicitly implemented on top of **Pyre’s Query API** (with Watchman + a running Pyre server) and is wired through the `FullRepoManager`. There is no out‑of‑the‑box `PyreflyTypeInferenceProvider` today. If you rely on LibCST’s `TypeInferenceProvider`, the supported path remains **Pyre**. ([LibCST][16])

### Why not?

* **LibCST contracts**: the **metadata interface** is tool‑agnostic, but the **type inference provider** is Pyre‑specific. Its runtime model expects a Pyre server and queries it for types at positions. ([LibCST][17])
* **Pyrefly APIs**: Pyrefly exposes **LSP** features (hover, definition, references, semantic tokens) and CLI checks, but it does **not** ship a public “types‑in‑file” query API compatible with LibCST’s Pyre query dialect. ([Pyrefly][12])

### Practical options (best‑practice methodology)

If your end goal is “LibCST transforms that sometimes need types,” you have three viable patterns:

**A) Keep using Pyre for LibCST type metadata (recommended if you need exact parity today).**
This preserves `TypeInferenceProvider` semantics and performance since LibCST already integrates with Pyre’s server model via `FullRepoManager`. Continue to use Pyrefly for CLI checks/IDE features in parallel if you like. ([LibCST][18])

**B) Replace type *queries* with a Pyrefly‑backed provider (advanced / custom).**
You can build a custom LibCST `MetadataProvider` that talks to a long‑lived **Pyrefly LSP** process and asks for types via **`textDocument/hover`** at LibCST node spans (range‑start is usually enough). The provider would:

1. Start `pyrefly lsp` once per repo; ensure its working directory sees the project’s `pyrefly.toml`.
2. On each file, send LSP `didOpen` and subsequent `didChange` notifications to mirror the buffer you are analyzing.
3. For nodes you care about, issue `hover` requests and parse the type strings from the hover contents.
4. Cache results by `(documentURI, version, byteRange)` and batch requests to control latency.
5. Normalize type string representations (they may differ from Pyre’s) before storing as LibCST metadata.
   This approach is workable for **targeted** nodes (call sites, attribute accesses, etc.), but it is **not** identical to `TypeInferenceProvider` and will stress the LSP if you try to query every node. ([Pyrefly][12])

**C) Eliminate dynamic type queries by materializing types in code.**
Run `pyrefly infer` to **write annotations**; then your LibCST transforms can rely on concrete annotations plus other built‑in providers (e.g., `QualifiedNameProvider`, `ScopeProvider`, `PositionProvider`) without needing an external type server at all. This trades inference *time* for runtime *simplicity* and reproducibility in CI. ([Pyrefly][6])

> **Setup tips for B/C:**
>
> * Align environments: run `pyrefly dump-config` to ensure the same search paths/interpreter that your transform expects (venv/conda). ([Pyrefly][8])
> * Prefer **path‑based** editable installs (not import hooks) so static import resolution can see sources. ([Pyrefly][8])
> * Use `permissive-ignores = true` when migrating to coexist with legacy `# pyre-ignore`/`# mypy:` comments. ([Pyrefly][4])

**Bottom line:** If you require LibCST’s **existing** `TypeInferenceProvider`, keep Pyre. If you can **constrain the set of nodes** that require type info, an LSP‑based custom provider is feasible; if you can **write types** into code, `pyrefly infer` plus non‑type LibCST providers can remove the need for a live type server entirely. ([LibCST][16])

---

## 10) Detailed capability catalog (exhaustive quick reference)

**Type system**

* Local inference (variables/returns), flow refinement, empty‑container inference (configurable), Literal handling, protocols/overrides typed through error kinds, TypedDict/tuple index checks, narrowing via control flow. ([GitHub][2])

**Importing & environment**

* Search path heuristics, fallback search path when no config root, stub vs source precedence, site‑package discovery from interpreter, editable‑install caveats, config dump. ([Pyrefly][8])

**Configuration parity & toggles**

* mypy/pyright migration; `untyped-def-behavior` modes; permissive ignores; sub‑configs akin to execution environments; `replace-imports-with-any` vs `ignore-missing-imports`; switchable `typeshed-path`. ([Pyrefly][10])

**Language Server**

* Goto/refs/symbols/hover/signature/completions/rename/diagnostics/semantic tokens/inlay hints; notebooks; ability to disable selected LSP methods or all diagnostics within IDE. ([Pyrefly][12])

**Suppression & baselines**

* Inline `# pyrefly: ignore` (and `[kind]`), `--suppress-errors`, `--remove-unused-ignores`, and baseline files with `--baseline`/`--update-baseline`. ([Pyrefly][14])

**Auto‑typing**

* `pyrefly infer path/or/dir` with flags to control parameters/returns/containers; review recommended. ([Pyrefly][6])

**Pydantic**

* v2‑focused, built‑in modeling of coercion vs strict, `extra`, aliases, constraints, root models—no plugin required. ([Pyrefly][15])

**Tooling & ecosystem**

* VS Code / OpenVSX extensions, community JetBrains plugin, Neovim setups (mason/lspconfig), Emacs eglot, Helix, Sublime. Official pre‑commit hooks and CI examples. ([Pyrefly][7])

**Project status & provenance**

* Rust implementation from the Pyre team; Windows support; long‑term maintenance intent; public roadmap via GitHub issues. ([Pyrefly][1])

---

## 11) Recommended deployment patterns

**Small/medium repos**

* Start with `pip install pyrefly && pyrefly init && pyrefly check`.
* Add the IDE extension; set `python.pyrefly.displayTypeErrors = "force-on"` during rollout; dial back later. ([Pyrefly][7])

**Large monorepos**

* Establish a shared `pyrefly.toml` with `project-includes` and known `site-package-path` behavior; rely on `use-ignore-files` so `.gitignore` reduces file lists; generate a baseline; enforce in CI and pre‑commit. ([Pyrefly][5])

**Hybrid typing migrations**

* Use `pyrefly init` to port settings; set `untyped-def-behavior` to match prior expectations; turn on `permissive-ignores`; gradually remove stale ignores with `--remove-unused-ignores`. ([Pyrefly][10])

---

## 12) Known limitations & open questions

* **No drop‑in LibCST TypeInferenceProvider**: you must keep Pyre or author your own Pyrefly‑backed provider via LSP `hover`. ([LibCST][16])
* **Config surface still moving** (anticipate some renames/semantics changes). ([Pyrefly][11])
* **Pydantic support is marked experimental**; expect behavior changes. ([Pyrefly][15])

---

## 13) Example snippets

**Minimal `pyproject.toml` (tool section)**

```toml
[tool.pyrefly]
project-includes = ["src"]
use-ignore-files = true

# Parity with mypy defaults:
untyped-def-behavior = "skip-and-infer-return-any"
permissive-ignores = true
```

(See the config reference for full option lists and defaults.) ([Pyrefly][4])

**CI (excerpt)**

```yaml
# .github/workflows/typecheck.yml
name: Pyrefly Type Check
on: [pull_request, workflow_dispatch]
jobs:
  typecheck:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
      - run: pip install pyrefly
      - run: pyrefly check
```

(Representative of the docs’ example.) ([Pyrefly][5])

**Sketch: building a Pyrefly‑backed LibCST provider (conceptual)**

```python
# Pseudocode / outline only
class PyreflyHoverTypeProvider(cst.MetadataProvider[str]):
    def __init__(self, lsp_client, uri):
        self.client = lsp_client       # long-lived JSON-RPC client to `pyrefly lsp`
        self.uri = uri
        self.cache = {}
    def on_visit(self, node):
        rng = byte_range_for_node(node)     # compute start position from LibCST metadata
        key = (self.uri, doc_version(), rng.start)
        if key in self.cache:
            t = self.cache[key]
        else:
            hover = self.client.hover(self.uri, rng.start)
            t = extract_type(hover)         # parse hover contents into a canonical type string
            self.cache[key] = t
        self.set_metadata(node, t)
```

This deliberately **does not** claim parity with `TypeInferenceProvider`; it demonstrates how to **target** specific nodes where types matter (e.g., call args/returns) while keeping request volume low and caching results. You still need robust LSP lifecycle code (`initialize`, `didOpen/Change/Close`) and to align `pyrefly` config/import paths. ([Pyrefly][12])

---

## 14) Decision matrix (for LibCST users)

| Requirement                            | Best option today                                                    |
| -------------------------------------- | -------------------------------------------------------------------- |
| Exact `TypeInferenceProvider` behavior | **Pyre** via `FullRepoManager`                                       |
| Occasional types at specific nodes     | Custom provider + **Pyrefly LSP** `hover`                            |
| No live types; reproducible transforms | Run **`pyrefly infer`**, then use existing non‑type LibCST providers |

Supporting sources: LibCST metadata docs; TypeInferenceProvider/FullRepoManager references; Pyrefly LSP features; `infer` documentation. ([LibCST][17])

---

## 15) Takeaways

* **As a checker/LSP**, Pyrefly is production‑grade and feature‑rich: fast checks, strong inference, robust import resolution, excellent IDE integration, CI/pre‑commit, and growing framework support (Pydantic v2). ([Pyrefly][12])
* **For LibCST type metadata**, there’s **no drop‑in** Pyrefly replacement for Pyre’s provider **today**. You can either keep Pyre for that provider, or implement a **custom LSP‑based provider** for targeted type queries, or lean on **auto‑annotation** to remove the need for dynamic type lookups. ([LibCST][16])

If you’d like, I can turn this into an internal “how‑to” package with a reference `PyreflyHoverTypeProvider` (client code + caching + benchmarks) and a side‑by‑side comparison vs Pyre on a sample repo.

[1]: https://pyrefly.org/en/docs/pyrefly-faq/ "FAQ | Pyrefly"
[2]: https://github.com/facebook/pyrefly "GitHub - facebook/pyrefly: A fast type checker and language server for Python"
[3]: https://pyrefly.org/ "Pyrefly: A Fast Python Type Checker and Language Server | Pyrefly"
[4]: https://pyrefly.org/en/docs/configuration/ "Configuration | Pyrefly"
[5]: https://pyrefly.org/en/docs/installation/ "Installation | Pyrefly"
[6]: https://pyrefly.org/en/docs/autotype/ "Infer | Pyrefly"
[7]: https://pyrefly.org/en/docs/IDE/ "IDE Installation | Pyrefly"
[8]: https://pyrefly.org/en/docs/import-resolution/ "Import Resolution | Pyrefly"
[9]: https://github.com/facebook/pyrefly-pre-commit "GitHub - facebook/pyrefly-pre-commit: A pre-commit hook for Pyrefly."
[10]: https://pyrefly.org/en/docs/migrating-from-mypy/ "Migrating from Mypy | Pyrefly"
[11]: https://pyrefly.org/en/docs/configuration/?utm_source=chatgpt.com "Pyrefly Configuration"
[12]: https://pyrefly.org/en/docs/IDE-features/ "IDE Supported Features | Pyrefly"
[13]: https://pyrefly.org/en/docs/error-kinds/ "Pyrefly Error Kinds | Pyrefly"
[14]: https://pyrefly.org/en/docs/error-suppressions/ "Pyrefly Error Suppressions | Pyrefly"
[15]: https://pyrefly.org/en/docs/pydantic/ "Experimental Pydantic Support | Pyrefly"
[16]: https://libcst.readthedocs.io/en/latest/_modules/libcst/metadata/type_inference_provider.html?utm_source=chatgpt.com "Source code for libcst.metadata.type_inference_provider"
[17]: https://libcst.readthedocs.io/en/latest/metadata.html?utm_source=chatgpt.com "Metadata — LibCST documentation"
[18]: https://libcst.readthedocs.io/en/latest/_modules/libcst/metadata/full_repo_manager.html?utm_source=chatgpt.com "Source code for libcst.metadata.full_repo_manager"
