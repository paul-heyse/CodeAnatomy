### 1) What to update to align with **DataFusion 52.1.x** (from your workspace `Cargo.toml`)

**Pin all Apache DataFusion crates to `52.1.0`.** That’s the current `52.1` line on docs.rs, and it uses **Arrow `^57.1.0`** and **object_store `^0.12.4`**. ([Docs.rs][1])

You already have `arrow = "57.1.0"` / `arrow-ipc = "57.1.0"` ✅, which matches DF 52.1. ([Docs.rs][1])

**Two notable exceptions in your list** (they’re *not* ASF DataFusion crates):

* `datafusion-tracing` latest is **`52.0.0`** and depends on `datafusion ^52.0.0` (so it’s compatible with `52.1.0`). ([Docs.rs][2])
* `instrumented-object-store` latest is **`52.0.0`** (also compatible with DataFusion 52.x / object_store 0.12.x). ([Docs.rs][3])

**Recommended `[workspace.dependencies]` edits (version-only):**

```toml
# keep (already aligned w/ DF 52.1 deps)
arrow = "57.1.0"
arrow-ipc = "57.1.0"

# upgrade all ASF DataFusion crates to 52.1.0
datafusion = { version = "52.1.0", default-features = false, features = ["parquet"] }
datafusion-catalog-listing = "52.1.0"
datafusion-common = "52.1.0"
datafusion-datasource = { version = "52.1.0", default-features = false }
datafusion-datasource-csv = { version = "52.1.0", default-features = false }
datafusion-datasource-parquet = { version = "52.1.0", default-features = false }
datafusion-doc = "52.1.0"
datafusion-expr = "52.1.0"
datafusion-expr-common = "52.1.0"
datafusion-ffi = "52.1.0"
datafusion-functions = "52.1.0"
datafusion-functions-aggregate = "52.1.0"
datafusion-functions-nested = "52.1.0"
datafusion-functions-table = "52.1.0"
datafusion-functions-window = "52.1.0"
datafusion-functions-window-common = "52.1.0"
datafusion-physical-expr-adapter = "52.1.0"
datafusion-physical-expr-common = "52.1.0"
datafusion-macros = "52.1.0"
datafusion-sql = "52.1.0"

# non-ASF helper crates: latest currently 52.0.0 (still compatible with DF 52.1)
datafusion-tracing = "52.0.0"
instrumented-object-store = "52.0.0"

# object_store: DF 52.1 wants ^0.12.4; your 0.12.5 is within-range (keep)
object_store = "0.12.5"
```

Why I’m confident in the Arrow/object_store alignment:

* DF 52.1.0 declares `arrow ^57.1.0` and `object_store ^0.12.4`. ([Docs.rs][1])
* `instrumented-object-store` 52.0.0 also uses `object_store ^0.12.4`. ([Docs.rs][3])

---

### 2) Best-in-class “highly structured docs” for LLM programming agents

You’re right: **rustdoc JSON** is the most “structured” canonical artifact. docs.rs explicitly hosts it, supports zstd/gzip, and warns you to handle multiple `format_version`s (via `rustdoc-types`). ([Docs.rs][4])

From there, “best in class” for agent usability usually means **tooling + normalization** rather than dumping raw JSON.

#### 2.1 Highest leverage for agents: *tool-access* (MCP) instead of preloading docs

If your agents can call tools, the most effective pattern is **on-demand retrieval**:

* **`docsrs-mcp`** (fetches from docs.rs rustdoc JSON + caches + indexes):

  * browse crate structure, lookup item docs/signatures/fields, search, inspect impl blocks
  * auto-resolves versions from your `Cargo.lock`
  * normalizes rustdoc JSON format versions “53–57+” ([GitHub][5])

* **`rustdoc-mcp`** (builds/serves docs from your workspace; nightly required):

  * tools: list crates, search, get item/module details
  * notes: requires nightly (unstable rustdoc JSON), optional `rust-docs-json` component for std/core/alloc/proc_macro/test ([Docs.rs][6])

This gives you *interactive*, low-context, “retrieve exactly what I need” documentation for LLMs.

#### 2.2 Best offline / artifact approach: JSON → LLM-friendly Markdown (module-per-file)

Raw rustdoc JSON is great for machines but not great for LLM *reading*. The best “agent-ready” compromise I’ve seen is: **per-module Markdown with indices + breadcrumbs**, generated automatically:

* **`cargo-doc-md`**:

  * generates Markdown for current crate + dependencies (`--workspace` supported)
  * output: `target/doc-md/index.md` + per-crate `index.md` + one file per module
  * handles multiple versions of same dependency
  * requires nightly (unstable rustdoc JSON) ([GitHub][7])

That output shape is typically *much* more usable as reference context than giant JSON blobs.

#### 2.3 Going beyond “docs”: generate *API contracts* (tiny, stable, diffable)

For upgrade work (like your DF51→DF52 guides), you often want a **minimal contract surface**:

* **`cargo-public-api`**:

  * lists + diffs public API across versions/commits
  * explicitly relies on rustdoc JSON (requires a recent nightly installed) ([GitHub][8])

This produces *small*, structured artifacts (one line per public item) that are excellent for LLMs doing upgrade reasoning, regressions, and “what changed?” without drowning.

---

### Practical recommendation (if you’re building an “LLM docs pipeline”)

1. **Use MCP** for interactive access (start with `docsrs-mcp` if you want “any crate” and Cargo.lock version matching). ([GitHub][5])
2. Generate offline “doc packs” via **`cargo doc-md --workspace`** for the subset you want agents to browse without network. ([GitHub][7])
3. For upgrades + CI: add **`cargo public-api diff`** snapshots for key crates (your DF extension crates, plugin API crates, etc.). ([GitHub][8])

If you want, I can turn this into:

* a **single `docs/llm_docs.md` spec** (inputs → rustdoc-json build → markdown emit → MCP config → storage layout), and
* a **one-command script** that reads `Cargo.lock`, downloads the matching docs.rs rustdoc JSON (`.zst`), and generates an indexed markdown bundle (ready for your CQ tool to “open / search / expand”).

[1]: https://docs.rs/crate/datafusion/latest "datafusion 52.1.0 - Docs.rs"
[2]: https://docs.rs/datafusion-tracing "datafusion_tracing - Rust"
[3]: https://docs.rs/instrumented-object-store "instrumented_object_store - Rust"
[4]: https://docs.rs/about/rustdoc-json "Rustdoc JSON"
[5]: https://github.com/dmvk/docsrs-mcp "GitHub - dmvk/docsrs-mcp"
[6]: https://docs.rs/crate/rustdoc-mcp/latest "rustdoc-mcp 0.5.0 - Docs.rs"
[7]: https://github.com/Crazytieguy/cargo-doc-md "GitHub - Crazytieguy/cargo-doc-md: Convert rustdoc JSON output to clean, LLM-friendly markdown documentation"
[8]: https://github.com/cargo-public-api/cargo-public-api "GitHub - cargo-public-api/cargo-public-api: List and diff the public API of Rust library crates between releases and commits. Detect breaking API changes and semver violations via CI or a CLI."


Below is a single **end-to-end bash script** that:

1. installs the **nightly** toolchain (required by `cargo-doc-md`)
2. installs `cargo-doc-md`
3. creates a **temporary “doc-bundle” crate** pinned to **DF 52.1.0** (+ Arrow 57.1.0, object_store 0.12.5, etc.)
4. runs `cargo doc-md` to emit **per-module Markdown** into your chosen output dir (default: `./docs/llm/rustdoc-md/df-52.1.0/`)

`cargo-doc-md` supports `-p` (repeatable), `--workspace`, `--no-deps`, and `-o` output directory, and writes a master `index.md` plus `crate_name/index.md` + one file per module. ([GitHub][1])

```bash
#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------------------
# generate_df52_rustdoc_md.sh
#
# Generates LLM-friendly markdown docs using cargo-doc-md for:
# - DataFusion 52.1.0 family of crates listed below
# - Arrow 57.1.0 (+ arrow-ipc)
# - object_store 0.12.5
# - datafusion-tracing + instrumented-object-store (defaults to 52.0.0 as of today)
#
# cargo-doc-md:
#   - requires nightly (unstable rustdoc JSON features) :contentReference[oaicite:1]{index=1}
#   - output: master index + per-crate/per-module markdown :contentReference[oaicite:2]{index=2}
# ------------------------------------------------------------------------------

# ---- Versions (override via env) ---------------------------------------------
DF_VER="${DF_VER:-52.1.0}"
ARROW_VER="${ARROW_VER:-57.1.0}"
OBJECT_STORE_VER="${OBJECT_STORE_VER:-0.12.5}"

# These two crates may lag DataFusion patch/minor; override if you want.
DF_TRACING_VER="${DF_TRACING_VER:-52.0.0}"
INSTR_OBJ_STORE_VER="${INSTR_OBJ_STORE_VER:-52.0.0}"

# Nightly toolchain to use (override if you pin nightly-YYYY-MM-DD)
NIGHTLY_TOOLCHAIN="${NIGHTLY_TOOLCHAIN:-nightly}"

# Output directory (override via env)
OUT_DIR="${OUT_DIR:-$PWD/docs/llm/rustdoc-md/df-${DF_VER}}"

# Documentation mode:
#   top_only  -> docs for the crates listed in CRATES only (no transitive deps)
#   with_deps -> docs for the crates listed in CRATES + all transitive deps (HUGE)
DOC_MODE="${DOC_MODE:-top_only}"

# Include private items in rustdoc output? (0/1)
INCLUDE_PRIVATE="${INCLUDE_PRIVATE:-0}"

# ------------------------------------------------------------------------------
# Crates to document (matches your workspace list / our prior enumeration)
# ------------------------------------------------------------------------------
CRATES=(
  arrow
  arrow-ipc
  object_store

  datafusion
  datafusion-catalog-listing
  datafusion-common
  datafusion-datasource
  datafusion-datasource-csv
  datafusion-datasource-parquet
  datafusion-doc
  datafusion-expr
  datafusion-expr-common
  datafusion-ffi
  datafusion-functions
  datafusion-functions-aggregate
  datafusion-functions-nested
  datafusion-functions-table
  datafusion-functions-window
  datafusion-functions-window-common
  datafusion-physical-expr-adapter
  datafusion-physical-expr-common
  datafusion-macros
  datafusion-sql

  datafusion-tracing
  instrumented-object-store
)

# ---- Create temp workdir (macOS + linux compatible) --------------------------
TMPDIR="$(mktemp -d 2>/dev/null || mktemp -d -t df-doc-md)"
cleanup() { rm -rf "$TMPDIR"; }
trap cleanup EXIT

BUNDLE_DIR="$TMPDIR/df_doc_bundle"
mkdir -p "$BUNDLE_DIR/src"

# Keep build artifacts out of your repo
export CARGO_TARGET_DIR="$TMPDIR/target"

# ---- Ensure nightly + cargo-doc-md installed --------------------------------
if ! command -v rustup >/dev/null 2>&1; then
  echo "ERROR: rustup not found. Install Rust via rustup first."
  exit 1
fi

rustup toolchain install "$NIGHTLY_TOOLCHAIN" --profile minimal

# cargo-doc-md is a cargo subcommand; installing provides 'cargo-doc-md' binary
if ! command -v cargo-doc-md >/dev/null 2>&1; then
  cargo +"$NIGHTLY_TOOLCHAIN" install cargo-doc-md --locked
fi

# ---- Create bundle Cargo.toml pinned to the requested versions ---------------
cat >"$BUNDLE_DIR/Cargo.toml" <<TOML
[package]
name = "df_doc_bundle"
version = "0.1.0"
edition = "2024"
publish = false

[dependencies]
# Pin exact versions so docs match DF 52.1.0 stack.
arrow = { version = "=${ARROW_VER}" }
arrow-ipc = { version = "=${ARROW_VER}" }
object_store = { version = "=${OBJECT_STORE_VER}" }

datafusion = { version = "=${DF_VER}", default-features = false, features = ["parquet"] }
datafusion-catalog-listing = { version = "=${DF_VER}" }
datafusion-common = { version = "=${DF_VER}" }
datafusion-datasource = { version = "=${DF_VER}", default-features = false }
datafusion-datasource-csv = { version = "=${DF_VER}", default-features = false }
datafusion-datasource-parquet = { version = "=${DF_VER}", default-features = false }
datafusion-doc = { version = "=${DF_VER}" }
datafusion-expr = { version = "=${DF_VER}" }
datafusion-expr-common = { version = "=${DF_VER}" }
datafusion-ffi = { version = "=${DF_VER}" }
datafusion-functions = { version = "=${DF_VER}" }
datafusion-functions-aggregate = { version = "=${DF_VER}" }
datafusion-functions-nested = { version = "=${DF_VER}" }
datafusion-functions-table = { version = "=${DF_VER}" }
datafusion-functions-window = { version = "=${DF_VER}" }
datafusion-functions-window-common = { version = "=${DF_VER}" }
datafusion-physical-expr-adapter = { version = "=${DF_VER}" }
datafusion-physical-expr-common = { version = "=${DF_VER}" }
datafusion-macros = { version = "=${DF_VER}" }
datafusion-sql = { version = "=${DF_VER}" }

# Helper crates (override via env if you want different versions)
datafusion-tracing = { version = "=${DF_TRACING_VER}" }
instrumented-object-store = { version = "=${INSTR_OBJ_STORE_VER}" }
TOML

cat >"$BUNDLE_DIR/src/lib.rs" <<'RS'
// Intentionally empty: this crate exists only to pull in deps for documentation.
RS

# Lock the dependency graph (reproducible runs)
(
  cd "$BUNDLE_DIR"
  cargo +"$NIGHTLY_TOOLCHAIN" generate-lockfile
)

# ---- Run cargo doc-md on the crates list -------------------------------------
mkdir -p "$OUT_DIR"

CMD=(cargo +"$NIGHTLY_TOOLCHAIN" doc-md -o "$OUT_DIR")

# package selection: document exactly these crates (and optionally their deps)
if [[ "$DOC_MODE" == "top_only" ]]; then
  CMD+=(--no-deps)
elif [[ "$DOC_MODE" != "with_deps" ]]; then
  echo "ERROR: DOC_MODE must be 'top_only' or 'with_deps' (got: $DOC_MODE)"
  exit 1
fi

if [[ "$INCLUDE_PRIVATE" == "1" ]]; then
  CMD+=(--include-private)
fi

for c in "${CRATES[@]}"; do
  CMD+=(-p "$c")
done

(
  cd "$BUNDLE_DIR"
  echo "Running: ${CMD[*]}"
  "${CMD[@]}"
)

echo
echo "✅ Done."
echo "Docs written to: $OUT_DIR"
echo "Start here:      $OUT_DIR/index.md"
```

### How to run

```bash
chmod +x ./generate_df52_rustdoc_md.sh
./generate_df52_rustdoc_md.sh
```

Useful overrides:

```bash
# Put output somewhere else
OUT_DIR="$PWD/.artifacts/rustdoc-md" ./generate_df52_rustdoc_md.sh

# Also include transitive deps (this can get very large)
DOC_MODE=with_deps ./generate_df52_rustdoc_md.sh

# Include private items (bigger output; sometimes helpful for agent deep dives)
INCLUDE_PRIVATE=1 ./generate_df52_rustdoc_md.sh

# Pin a specific nightly for determinism
NIGHTLY_TOOLCHAIN=nightly-2026-02-10 ./generate_df52_rustdoc_md.sh
```


[1]: https://github.com/Crazytieguy/cargo-doc-md "GitHub - Crazytieguy/cargo-doc-md: Convert rustdoc JSON output to clean, LLM-friendly markdown documentation"
