---
name: datafusion-and-deltalake-stack
description: DataFusion + DeltaLake operations manual for this repo. DataFusion is the core query engine; DeltaLake provides the storage layer and integrates tightly via scan providers, schema bridging, and predicate pushdown. Use lookup + local probes; do not guess APIs.
allowed-tools: Read, Grep, Glob, Bash
---

## Operating rule: never guess DataFusion/DeltaLake/PyArrow/UDF APIs
When uncertain:
1) Probe local environment (versions + available methods).
2) Search the repo for how we already use it.
3) Open the relevant reference file below (only the section you need).
4) Implement using *existing local patterns* unless the plan says otherwise.


## Reference map (open these files as needed)
- Core DataFusion Python surfaces (IO, catalog, SQL, DataFrame API): reference/datafusion.md
- "Best-in-class deployment gaps" (caching, stats, observability, planning knobs): reference/datafusion_addendum.md
- Planning deep dive (logical/physical plan pipeline, introspection, optimization rules): reference/datafusion_planning.md
- Rust UDF contracts (Scalar/UDAF/UDWF/Async/named args): reference/datafusion_rust_UDFs.md
- Schema management + schema pitfalls: reference/datafusion_schema.md
- DeltaLake ↔ DataFusion integration details: reference/deltalake_datafusion_integration.md
- Advanced Rust integration (PyO3 packaging, wheels, CI, native module distribution): reference/datafusion_deltalake_advanced_rust_integration.md
- DataFusionMixins trait (Delta snapshot schema + predicate parsing helpers): reference/deltalake_datafusionmixins.md
- DeltaLake core (format/protocol, client APIs, 3-layer model): reference/deltalake.md
