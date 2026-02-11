# Relspec Decommission Migration Checklist

Date: 2026-02-10
Scope baseline: `docs/plans/relspec_decommission_baseline_2026-02-10.md`

## Wave 0

- [x] Capture import baseline in docs.
- [x] Add CI guard against reintroduction of decommissioned relspec imports.
- [ ] Confirm guard runs in CI.

## Wave 1

- [ ] Delete immediate modules.
- [ ] Remove deleted-module exports from `src/relspec/__init__.py`.
- [ ] Remove tests that only validate immediate-delete modules.

## Wave 2

- [ ] Remove scheduling-stack types/imports from `src/serde_schema_registry.py`.
- [ ] Remove `TaskGraph` type-only dependency from `src/relspec/policy_compiler.py`.
- [ ] Replace relspec planning/scheduling integration tests with Rust compile-contract tests.

## Wave 3

- [ ] Delete scheduling/planning relspec modules.
- [ ] Remove remaining graph/schedule exports in `src/relspec/__init__.py`.
- [ ] Remove tests importing deleted scheduling/planning modules.

## Wave 4

- [ ] Regenerate msgspec schema contract goldens.
- [ ] Verify no deleted relspec classes appear in schema goldens.
- [ ] Update docs to reflect Rust compile contract and Rust task schedule authority.

## Final Gate

- [ ] `uv run ruff format`
- [ ] `uv run ruff check --fix`
- [ ] `uv run pyrefly check`
- [ ] `uv run pyright`
- [ ] `uv run pytest -q`
