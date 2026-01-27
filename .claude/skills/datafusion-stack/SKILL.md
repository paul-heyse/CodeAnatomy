---
name: datafusion-stack
description: DataFusion/PyArrow/Delta/Rust-UDF operations manual for this repo. Use lookup + local probes; do not guess APIs.
allowed-tools: Read, Grep, Glob, Bash
---

## Operating rule: never guess DataFusion/PyArrow/UDF APIs
When uncertain:
1) Probe local environment (versions + available methods).
2) Search the repo for how we already use it.
3) Open the relevant reference file below (only the section you need).
4) Implement using *existing local patterns* unless the plan says otherwise.

## Quick probes (insert live output)
DataFusion: !bash scripts/df_probe.sh
Rust UDFs: !bash scripts/rust_udf_probe.sh

## Reference map (open these files as needed)
- Core DataFusion Python surfaces (IO, catalog, SQL, DataFrame API): reference/datafusion.md
- “Best-in-class deployment gaps” (caching, stats, observability, planning knobs): reference/datafusion_addendum.md
- Plan introspection + Ibis/DataFusion planning overlays (logical/physical/substrait): reference/datafusion_and_ibis_planning.md
- Rust UDF contracts (Scalar/UDAF/UDWF/Async/named args): reference/datafusion_rust_UDFs.md
- Schema management + schema pitfalls: reference/datafusion_schema.md
- DeltaLake ↔ DataFusion integration details: reference/deltalake_datafusion_integration.md
