# Packaging contract (Rust + Python wheels)

This document defines the **packaging contract** for the Rust-backed DataFusion/Delta extensions.
It is a **policy summary**; the authoritative build flags and dependencies live in:
- `scripts/build_datafusion_wheels.sh`
- `rust/datafusion_python/Cargo.toml.orig`
- `rust/datafusion_ext_py/Cargo.toml.orig`
- `pyproject.toml`

## Artifact strategy

- **Default**: ABI-stable wheels (PyO3 `abi3`).
- **Fallback**: per-Python wheels only when ABI stability is not possible.

## Linux portability

- **Baseline**: manylinux2014.
- Wheels are built with `--compatibility pypi` and must import cleanly without LD_LIBRARY_PATH.

## Module name invariants

- The compiled extension module must be importable as `datafusion._internal`.
- The Cargo `[lib].name` and `#[pymodule]` name must remain compatible with the Python package import path.
- Wheel smoke tests must import both `datafusion._internal` and `datafusion.substrait`.

## Required smoke tests

- Import `datafusion._internal` from the wheel.
- Validate `datafusion.substrait.Producer.to_substrait_plan` exists.
- Validate `datafusion_ext.capabilities_snapshot()` returns a payload.

## CI expectations

- CI must run `maturin` with `--locked --compatibility pypi` and the selected manylinux baseline.
- Build outputs are staged under `dist/wheels/` and plugin artifacts under `rust/datafusion_ext_py/plugin/`.
