# CQ Run Plan — Remaining CPG/schema_spec Rust Cutover

This file is the canonical CQ command checklist to run before edits and before deletion gates.

## Baseline Discovery

```bash
./cq search view_builders_df --in src --format summary
./cq search schema_spec.system --in src --format summary
./cq search schema_spec.dataset_spec_ops --in src --format summary
./cq search run_build --in rust --lang rust --format summary
```

## Signature/Caller Safety (Per Function Change)

```bash
./cq calls <function_name>
./cq impact <function_name> --param <param_name>
./cq sig-impact <function_name> --to "<new_signature>"
```

## Structural Legacy-Import Checks

```bash
./cq q "entity=import name=dataset_spec_ops in=src"
./cq q "entity=import name=view_builders_df in=src"
./cq q "pattern='from schema_spec.system import $X'"
```

## Rust Discovery for Implementation

```bash
./cq search CpgEmit --lang rust --in rust/codeanatomy_engine
./cq q "entity=function lang=rust in=rust/codeanatomy_engine/src/compiler"
./cq q "entity=function lang=rust in=rust/codeanatomy_engine/src/schema"
./cq q "entity=function lang=rust in=rust/codeanatomy_engine_py/src"
```

## Deletion Gates

```bash
./cq search view_builders_df --in src --format summary
./cq search dataset_spec_ops --in src --format summary
./cq search "from schema_spec.system import" --in src --format summary
```
