# Relspec Decommission Baseline

Date: 2026-02-10

Reference capture used to execute the relspec full decommission.

## Import Baseline

`module,src_imports,tests_imports`

- `calibration_bounds,1,2`
- `compiled_policy,5,4`
- `contracts,1,0`
- `counterfactual_replay,0,1`
- `decision_provenance,2,2`
- `decision_recorder,0,1`
- `errors,3,1`
- `evidence,4,6`
- `execution_authority,0,1`
- `execution_package,1,1`
- `execution_planning_runtime,1,7`
- `extract_plan,2,1`
- `fallback_quarantine,0,1`
- `graph_edge_validation,2,1`
- `incremental,0,0`
- `inference_confidence,7,6`
- `inferred_deps,6,8`
- `pipeline_policy,3,1`
- `policy_calibrator,1,2`
- `policy_compiler,1,4`
- `policy_validation,0,1`
- `runtime_artifacts,0,1`
- `rustworkx_graph,6,14`
- `rustworkx_schedule,3,4`
- `schedule_events,3,0`
- `table_size_tiers,3,1`
- `view_defs,8,0`

## Delete Scope

Immediate delete:

- `src/relspec/incremental.py`
- `src/relspec/counterfactual_replay.py`
- `src/relspec/decision_recorder.py`
- `src/relspec/fallback_quarantine.py`
- `src/relspec/policy_validation.py`
- `src/relspec/runtime_artifacts.py`
- `src/relspec/execution_authority.py`

Delete after callsite moves:

- `src/relspec/execution_planning_runtime.py`
- `src/relspec/rustworkx_graph.py`
- `src/relspec/rustworkx_schedule.py`
- `src/relspec/schedule_events.py`
- `src/relspec/graph_edge_validation.py`
- `src/relspec/evidence.py`
- `src/relspec/extract_plan.py`
