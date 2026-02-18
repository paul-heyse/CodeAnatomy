#!/usr/bin/env bash
set -euo pipefail

# Generates LLM-friendly markdown docs for the DF52.1 + Delta 0.31 stack.
# Default output:
#   docs/llm/rustdoc-md/df-52.1.0/

DF_VER="${DF_VER:-52.1.0}"
ARROW_VER="${ARROW_VER:-57.1.0}"
OBJECT_STORE_VER="${OBJECT_STORE_VER:-0.12.5}"
DELTA_VER="${DELTA_VER:-0.31.0}"

# These helper crates are currently on 52.0.x and compatible with DF 52.1.x.
DF_TRACING_VER="${DF_TRACING_VER:-52.0.0}"
INSTR_OBJ_STORE_VER="${INSTR_OBJ_STORE_VER:-52.0.0}"

NIGHTLY_TOOLCHAIN="${NIGHTLY_TOOLCHAIN:-nightly}"
OUT_DIR="${OUT_DIR:-$PWD/docs/llm/rustdoc-md/df-${DF_VER}}"
DOC_MODE="${DOC_MODE:-top_only}" # top_only | with_deps
INCLUDE_PRIVATE="${INCLUDE_PRIVATE:-0}" # 0 | 1

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
  deltalake
  deltalake-core
)

if ! command -v rustup >/dev/null 2>&1; then
  echo "ERROR: rustup not found."
  exit 1
fi
if ! command -v cargo >/dev/null 2>&1; then
  echo "ERROR: cargo not found."
  exit 1
fi
if ! command -v cargo-doc-md >/dev/null 2>&1; then
  echo "ERROR: cargo-doc-md not found. Install with:"
  echo "  cargo +${NIGHTLY_TOOLCHAIN} install cargo-doc-md --locked"
  exit 1
fi

TMPDIR="$(mktemp -d 2>/dev/null || mktemp -d -t df-doc-md)"
cleanup() { rm -rf "$TMPDIR"; }
trap cleanup EXIT

BUNDLE_DIR="$TMPDIR/df_doc_bundle"
mkdir -p "$BUNDLE_DIR/src"
export CARGO_TARGET_DIR="$TMPDIR/target"

cat >"$BUNDLE_DIR/Cargo.toml" <<TOML
[package]
name = "df_doc_bundle"
version = "0.1.0"
edition = "2024"
publish = false

[dependencies]
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

datafusion-tracing = { version = "=${DF_TRACING_VER}" }
instrumented-object-store = { version = "=${INSTR_OBJ_STORE_VER}" }

deltalake = { version = "=${DELTA_VER}", features = ["datafusion"] }
deltalake-core = { version = "=${DELTA_VER}", features = ["datafusion"] }
TOML

cat >"$BUNDLE_DIR/src/lib.rs" <<'RS'
// Intentionally empty. This crate exists only to pull in dependencies for docs.
RS

(
  cd "$BUNDLE_DIR"
  cargo +"$NIGHTLY_TOOLCHAIN" generate-lockfile
)

mkdir -p "$OUT_DIR"
CMD=(cargo +"$NIGHTLY_TOOLCHAIN" doc-md -o "$OUT_DIR")

if [[ "$DOC_MODE" == "top_only" ]]; then
  CMD+=(--no-deps)
elif [[ "$DOC_MODE" != "with_deps" ]]; then
  echo "ERROR: DOC_MODE must be top_only or with_deps"
  exit 1
fi

if [[ "$INCLUDE_PRIVATE" == "1" ]]; then
  CMD+=(--include-private)
fi

for crate in "${CRATES[@]}"; do
  CMD+=(-p "$crate")
done

(
  cd "$BUNDLE_DIR"
  echo "Running: ${CMD[*]}"
  "${CMD[@]}"
)

echo "Done."
echo "Docs: $OUT_DIR"
echo "Index: $OUT_DIR/index.md"
