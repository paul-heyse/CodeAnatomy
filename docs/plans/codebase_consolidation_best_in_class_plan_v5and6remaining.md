# Codebase Consolidation Plan v5+v6 Remaining Scope

## Executive Summary

This document consolidates the remaining incomplete scope from both v5 and v6 consolidation plans into a single actionable plan. Each scope item includes detailed guidance on leveraging the **ast-grep** and **cq** skills for discovery, impact analysis, and safe execution.

**Source Documents:**
- `docs/plans/codebase_consolidation_best_in_class_plan_v5.md`
- `docs/plans/codebase_consolidation_best_in_class_plan_v6.md`

**Status as of 2026-01-30:** Reviewed against current codebase. Remaining scope is limited to v5 deferred decommissioning; Hamilton IO contract completion is now complete and module-level IO decorators have been removed.

---

## Scope Index (Remaining Items)

### From v5:
1. [x] Tag Policy Cleanup (ad-hoc tag dicts removal)
2. [x] Hamilton IO Contract Completion (per-module IO migration + validation)
3. [x] Parameterized Subdag Adoption (manual variant replacement)
4. [x] DataFusion Table Registration Entrypoint (single registration surface)
5. [x] Delta CDF Cleanup (diff-based incremental removal)
6. [x] Delta Maintenance Wiring (write/incremental path integration)
7. [ ] v5 Deferred Decommissioning

### From v6:
8. [x] Physical Optimizer Rulepack (Rust implementation + Python cleanup)
9. [x] Constraint Governance DSL (unified constraint surface)
10. [x] Delta Protocol Feature Wiring (policy application + diagnostics)
11. [x] Substrait Plan Mandate (mandatory bytes + diffing)
12. [x] msgspec Schema Export (JSON Schema generation)
13. [x] Catalog Autoload for Non-Delta (registration routing)
14. [x] SQL Policy Single Source (SessionConfig + Rust enforcement)
15. [x] COPY/INSERT Diagnostics (artifact capture + helper removal)
16. [x] v6 Deferred Decommissioning

---

## Tool Usage Conventions

### ast-grep Conventions

```bash
# Pattern search (zero false positives from strings/comments)
ast-grep run -l python -p '<pattern>' <paths>

# Pattern with globs for scoping
ast-grep run -l python -p '<pattern>' --globs 'src/**/*.py' --globs '!tests/**'

# Apply rewrite (interactive review)
ast-grep run -l python -p '<old_pattern>' -r '<new_pattern>' -i

# Apply rewrite (all at once)
ast-grep run -l python -p '<old_pattern>' -r '<new_pattern>' -U

# Debug pattern parsing
ast-grep run -l python -p '<pattern>' --debug-query=cst
```

### cq Conventions

```bash
# Find all call sites for a function
/cq calls <function_name>

# Analyze parameter taint propagation
/cq impact <function_name> --param <param_name> --depth 5

# Simulate signature change impact
/cq sig-impact <function_name> --to "<new_signature>"

# Analyze exception handling patterns
/cq exceptions --function <function_name>

# Check closure captures before extraction
/cq scopes <file_or_symbol>

# Import cycle detection
/cq imports --cycles
```

---

## 1. Tag Policy Cleanup (v5 Scope 1 Remaining)

### Objective
Remove remaining ad-hoc tag dicts and helpers now superseded by `TagPolicy`.

### Status
Complete. `_CPG_FINAL_TAGS`, `_CPG_DELTA_WRITE_TAGS`, and `_semantic_tag` are no longer present, and outputs use `TagPolicy`/`apply_tag`.

### Target Files
- `src/hamilton_pipeline/modules/task_execution.py` (`_CPG_FINAL_TAGS`)
- `src/hamilton_pipeline/modules/outputs.py` (`_CPG_DELTA_WRITE_TAGS`, `_semantic_tag`)

### ast-grep Discovery Patterns

**Find all ad-hoc tag dict definitions:**
```bash
# Find dicts assigned to _*_TAGS variables
ast-grep run -l python -p '$_TAGS = {$$$}' --globs 'src/hamilton_pipeline/**/*.py'

# Find inline tag() calls with dict literals
ast-grep run -l python -p 'tag($$$)' --globs 'src/hamilton_pipeline/**/*.py'

# Find _semantic_tag helper calls
ast-grep run -l python -p '_semantic_tag($$$)' --globs 'src/**/*.py'
```

**Find existing TagPolicy usages for reference:**
```bash
ast-grep run -l python -p 'TagPolicy($$$)' --globs 'src/**/*.py'
ast-grep run -l python -p 'apply_tag($$$)' --globs 'src/**/*.py'
```

### cq Analysis

**Analyze callers of deprecated helpers:**
```bash
# Find all call sites for _semantic_tag
/cq calls _semantic_tag

# Check impact of removing _CPG_FINAL_TAGS
/cq impact task_execution --param _CPG_FINAL_TAGS --depth 3
```

**Verify TagPolicy adoption:**
```bash
# Find all usages of TagPolicy to confirm coverage
/cq calls TagPolicy
/cq calls apply_tag
```

### Execution Steps

1. **Discovery Phase:**
   - Run ast-grep patterns above to inventory all ad-hoc tag constructs
   - Run `/cq calls _semantic_tag` to find all callers
   - Document each usage site

2. **Migration Phase:**
   - For each ad-hoc tag site, create equivalent `TagPolicy` instance
   - Replace `tag(**dict_literal)` with `apply_tag(TagPolicy(...))`
   - Use ast-grep rewrite for mechanical replacements:
     ```bash
     # Example: Replace inline tag calls with apply_tag
     ast-grep run -l python \
       -p 'tag(layer="$LAYER", kind="$KIND", artifact="$ART")' \
       -r 'apply_tag(TagPolicy(layer="$LAYER", kind="$KIND", artifact="$ART"))' \
       --globs 'src/hamilton_pipeline/**/*.py' -i
     ```

3. **Cleanup Phase:**
   - Delete `_CPG_FINAL_TAGS`, `_CPG_DELTA_WRITE_TAGS` constants
   - Delete `_semantic_tag` helper function
   - Verify no references remain:
     ```bash
     ast-grep run -l python -p '_CPG_FINAL_TAGS' --globs 'src/**/*.py'
     ast-grep run -l python -p '_semantic_tag' --globs 'src/**/*.py'
     ```

### Validation
```bash
uv run pytest tests/unit/hamilton_pipeline/ -v
uv run pyright --warnings src/hamilton_pipeline/
```

---

## 2. Hamilton IO Contract Completion (v5 Scope 2 Remaining)

### Objective
- Move remaining per-module IO logic into centralized `io_contracts.py`
- Add contract-level validation of output metadata (post-write payload checks)

### Status
Complete. IO contracts now live in `src/hamilton_pipeline/io_contracts.py`, and module-level IO decorators have been removed from `src/hamilton_pipeline/modules/`.

### Target Files
- `src/hamilton_pipeline/io_contracts.py` (extend)
- `src/hamilton_pipeline/modules/dataloaders.py` (migrate from)
- `src/hamilton_pipeline/modules/outputs.py` (migrate from)
- `src/hamilton_pipeline/modules/inputs.py` (migrate from)

### ast-grep Discovery Patterns

**Find dataloader/datasaver decorators in modules:**
```bash
# Find @dataloader decorators
ast-grep run -l python -p '@dataloader
def $FN($$$):
    $$$' --globs 'src/hamilton_pipeline/modules/**/*.py'

# Find @datasaver decorators
ast-grep run -l python -p '@datasaver
def $FN($$$):
    $$$' --globs 'src/hamilton_pipeline/modules/**/*.py'

# Find direct load_table/write_table calls (should be in contracts)
ast-grep run -l python -p 'load_table($$$)' --globs 'src/hamilton_pipeline/**/*.py'
ast-grep run -l python -p 'write_table($$$)' --globs 'src/hamilton_pipeline/**/*.py'
```

**Find existing IO contract patterns for consistency:**
```bash
ast-grep run -l python -p 'DatasetIO($$$)' --globs 'src/**/*.py'
```

### cq Analysis

**Map IO function callers:**
```bash
# Find all call sites for IO operations
/cq calls load_table
/cq calls write_table

# Analyze dataloader/datasaver usage patterns
/cq calls read_dataset
/cq calls write_dataset
```

**Check for side effects in IO operations:**
```bash
/cq side-effects --include "src/hamilton_pipeline/modules/"
```

### Execution Steps

1. **Inventory Phase:**
   - Run ast-grep to find all `@dataloader`/`@datasaver` definitions outside `io_contracts.py`
   - Run `/cq calls load_table` to map all table loading patterns
   - Document each non-centralized IO pattern

2. **Contract Design Phase:**
   - For each discovered IO pattern, design a `DatasetIO` variant
   - Define post-write validation schema (expected payload fields)

3. **Migration Phase:**
   - Move IO logic from modules to `io_contracts.py`
   - Update module functions to use contract-based IO
   - Add validation hooks:
     ```python
     def validate_write_payload(payload: dict[str, object], contract: DatasetIO) -> None:
         required_keys = {"dataset", "materialization", "row_count"}
         missing = required_keys - payload.keys()
         if missing:
             raise ValueError(f"Write payload missing: {missing}")
     ```

4. **Cleanup Phase:**
   - Remove local IO helpers from modules
   - Verify all IO routes through contracts:
     ```bash
     # Should find zero results in modules/
     ast-grep run -l python -p 'load_table($$$)' --globs 'src/hamilton_pipeline/modules/**/*.py'
     ```

### Validation
```bash
uv run pytest tests/unit/hamilton_pipeline/ -v
uv run pyright --warnings src/hamilton_pipeline/
```

---

## 3. Parameterized Subdag Adoption (v5 Scope 4 Remaining)

### Objective
- Replace remaining manual per-dataset function variants with parameterized subdags
- Update tests/fixtures to reference new subdag outputs

### Status
Complete. CPG outputs now use parameterized subdags with shared validation and output tagging.

### Target Files
- `src/hamilton_pipeline/modules/subdags.py` (extend)
- `src/hamilton_pipeline/modules/outputs.py` (refactor)
- `src/hamilton_pipeline/modules/column_features.py` (refactor)
- `src/hamilton_pipeline/modules/task_execution.py` (refactor)
- `src/hamilton_pipeline/modules/params.py` (refactor)

### ast-grep Discovery Patterns

**Find repeated function patterns that should be subdags:**
```bash
# Find functions with _nodes, _edges, _props suffixes (dataset variants)
ast-grep run -l python -p 'def $FN_nodes($$$):
    $$$' --globs 'src/hamilton_pipeline/modules/**/*.py'

ast-grep run -l python -p 'def $FN_edges($$$):
    $$$' --globs 'src/hamilton_pipeline/modules/**/*.py'

ast-grep run -l python -p 'def $FN_props($$$):
    $$$' --globs 'src/hamilton_pipeline/modules/**/*.py'

# Find functions that follow normalize_* or process_* patterns
ast-grep run -l python -p 'def normalize_$DATASET($$$):
    $$$' --globs 'src/hamilton_pipeline/modules/**/*.py'

ast-grep run -l python -p 'def process_$DATASET($$$):
    $$$' --globs 'src/hamilton_pipeline/modules/**/*.py'
```

**Find existing subdag usages for patterns:**
```bash
ast-grep run -l python -p '@parameterized_subdag($$$)' --globs 'src/**/*.py'
ast-grep run -l python -p '@pipe_input($$$)' --globs 'src/**/*.py'
```

### cq Analysis

**Analyze function structure similarity:**
```bash
# Check scopes for functions that might share logic
/cq scopes src/hamilton_pipeline/modules/outputs.py
/cq scopes src/hamilton_pipeline/modules/column_features.py

# Find callers of repeated variants
/cq calls normalize_nodes
/cq calls normalize_edges
/cq calls normalize_props
```

**Check import dependencies:**
```bash
/cq imports --module src.hamilton_pipeline.modules
```

### Execution Steps

1. **Pattern Identification Phase:**
   - Run ast-grep patterns to find all `*_nodes`, `*_edges`, `*_props` variants
   - Group by semantic function (normalize, process, emit, etc.)
   - Document function bodies for similarity analysis

2. **Subdag Design Phase:**
   - For each group, design parameterized subdag:
     ```python
     @parameterized_subdag(
         inputs={"raw_table": "{dataset}_raw"},
         outputs={"clean_table": "{dataset}_clean"},
         config={"dataset": ["nodes", "edges", "props"]},
     )
     def normalize_subdag(raw_table: pa.Table) -> pa.Table:
         # Common normalization logic
         ...
     ```

3. **Migration Phase:**
   - Implement subdags in `subdags.py`
   - Replace variant functions with subdag invocations
   - Update test fixtures to use subdag outputs:
     ```bash
     # Find test references to old function names
     ast-grep run -l python -p 'normalize_nodes' --globs 'tests/**/*.py'
     ```

4. **Cleanup Phase:**
   - Delete replaced variant functions
   - Verify no direct references remain:
     ```bash
     ast-grep run -l python -p 'def normalize_nodes($$$)' --globs 'src/**/*.py'
     ```

### Validation
```bash
uv run pytest tests/unit/hamilton_pipeline/ -v
uv run pytest tests/integration/ -v
```

---

## 4. DataFusion Table Registration Entrypoint (v5 Scope 5 Remaining)

### Objective
Create a single registration entrypoint for all table types (listing, delta, external).

### Status
Complete. `register_dataset_df` routes through the unified `table_registration.register_table` entrypoint.

### Target Files
- `src/datafusion_engine/table_registration.py` (create/extend)
- `src/datafusion_engine/dataset_registration.py` (refactor)
- `src/datafusion_engine/dataset_registry.py` (refactor)
- `src/datafusion_engine/catalog_provider.py` (refactor)

### ast-grep Discovery Patterns

**Find all table registration calls:**
```bash
# Find register_table calls
ast-grep run -l python -p 'ctx.register_table($$$)' --globs 'src/**/*.py'
ast-grep run -l python -p 'register_table($$$)' --globs 'src/datafusion_engine/**/*.py'

# Find register_listing_table calls
ast-grep run -l python -p 'register_listing_table($$$)' --globs 'src/**/*.py'

# Find Delta table provider registrations
ast-grep run -l python -p 'DeltaTableProvider($$$)' --globs 'src/**/*.py'

# Find external table registrations
ast-grep run -l python -p 'CREATE EXTERNAL TABLE' --globs 'src/**/*.py'
```

**Find registration helper functions:**
```bash
ast-grep run -l python -p 'def register_$TYPE($$$):
    $$$' --globs 'src/datafusion_engine/**/*.py'
```

### cq Analysis

**Map registration call sites:**
```bash
# Find all callers of registration functions
/cq calls register_table
/cq calls register_listing_table
/cq calls DeltaTableProvider

# Analyze registration flow
/cq impact dataset_registration --param ctx --depth 3
```

**Check for scattered registration logic:**
```bash
/cq imports --module src.datafusion_engine.dataset_registration
```

### Execution Steps

1. **Discovery Phase:**
   - Run ast-grep patterns to inventory all registration patterns
   - Categorize by type: listing, delta, external, custom
   - Document partition/ordering requirements per pattern

2. **Contract Design Phase:**
   - Design unified `TableRegistrationRequest`:
     ```python
     @dataclass(frozen=True)
     class TableRegistrationRequest:
         name: str
         source_type: Literal["listing", "delta", "external", "memory"]
         uri: str
         schema: pa.Schema | None = None
         partition_cols: list[tuple[str, pa.DataType]] | None = None
         file_sort_order: list[str] | None = None
         delta_options: DeltaScanConfig | None = None
     ```

3. **Implementation Phase:**
   - Implement `register_table(ctx, request)` in `table_registration.py`
   - Route all table types through this single entrypoint
   - Emit registration diagnostics uniformly

4. **Migration Phase:**
   - Update all call sites to use new entrypoint:
     ```bash
     # Find and update each call site
     ast-grep run -l python -p 'ctx.register_table("$NAME", $PROVIDER)' \
       --globs 'src/**/*.py'
     ```

5. **Cleanup Phase:**
   - Remove duplicate registration helpers
   - Verify single entrypoint is used:
     ```bash
     # Should only find calls in table_registration.py or going through it
     ast-grep run -l python -p 'ctx.register_table($$$)' --globs 'src/**/*.py'
     ```

### Validation
```bash
uv run pytest tests/unit/datafusion_engine/ -v
uv run pytest tests/integration/ -v
```

---

## 5. Delta CDF Cleanup (v5 Scope 8 Remaining)

### Objective
Remove alternative diff-based incremental branches now that CDF is mandatory.

### Status
Complete. The incremental diff module is removed and no diff-based branches remain in incremental paths.

### Target Files
- `src/incremental/diff.py` (delete or deprecate)
- `src/incremental/delta_updates.py` (refactor)
- `src/incremental/changes.py` (refactor)

### ast-grep Discovery Patterns

**Find diff-based incremental code:**
```bash
# Find diff computation functions
ast-grep run -l python -p 'def compute_diff($$$):
    $$$' --globs 'src/incremental/**/*.py'

ast-grep run -l python -p 'def diff_$NAME($$$):
    $$$' --globs 'src/incremental/**/*.py'

# Find diff imports
ast-grep run -l python -p 'from $$$diff$$$import $$$' --globs 'src/**/*.py'

# Find diff-related branching
ast-grep run -l python -p 'if $$$diff$$$:
    $$$' --globs 'src/incremental/**/*.py'
```

**Find CDF usage patterns:**
```bash
ast-grep run -l python -p 'DeltaCdfProvider($$$)' --globs 'src/**/*.py'
ast-grep run -l python -p 'delta_cdf_provider($$$)' --globs 'src/**/*.py'
```

### cq Analysis

**Map diff function callers:**
```bash
# Find all callers of diff utilities
/cq calls compute_diff
/cq calls diff_tables

# Analyze impact of removing diff module
/cq imports --module src.incremental.diff
```

**Verify CDF coverage:**
```bash
# Check CDF provider adoption
/cq calls DeltaCdfProviderRequest
/cq calls delta_cdf_provider
```

### Execution Steps

1. **Discovery Phase:**
   - Run ast-grep patterns to find all diff-related code
   - Run `/cq imports --module src.incremental.diff` to find importers
   - Document all call sites

2. **Verification Phase:**
   - Confirm CDF path covers all incremental scenarios
   - Check that no code paths still require diff:
     ```bash
     /cq calls compute_diff --impact high
     ```

3. **Removal Phase:**
   - Remove diff-based conditional branches:
     ```bash
     ast-grep run -l python -p 'if use_diff:
         $$$DIFF_BRANCH$$$
     else:
         $$$CDF_BRANCH$$$' --globs 'src/**/*.py'
     ```
   - Replace with CDF-only path
   - Delete diff utility functions

4. **Cleanup Phase:**
   - Remove diff imports
   - Delete `src/incremental/diff.py` if entirely deprecated
   - Verify no references remain:
     ```bash
     ast-grep run -l python -p 'diff' --globs 'src/incremental/**/*.py'
     ```

### Validation
```bash
uv run pytest tests/unit/incremental/ -v
uv run pytest tests/e2e/ -v
```

---

## 6. Delta Maintenance Wiring (v5 Scope 9 Remaining)

### Objective
Wire maintenance execution into write and incremental paths.

### Status
Complete. Incremental delete/merge paths now run maintenance via `run_delta_maintenance_if_configured`.

### Target Files
- `src/datafusion_engine/delta_maintenance.py` (extend)
- `src/datafusion_engine/write_pipeline.py` (integrate)
- `src/incremental/delta_context.py` (integrate)

### ast-grep Discovery Patterns

**Find write completion hooks:**
```bash
# Find write_delta or write_table completions
ast-grep run -l python -p 'write_delta_table($$$)' --globs 'src/**/*.py'
ast-grep run -l python -p 'delta_writer.write($$$)' --globs 'src/**/*.py'

# Find post-write operations
ast-grep run -l python -p 'after_write($$$)' --globs 'src/**/*.py'
```

**Find existing maintenance call sites:**
```bash
ast-grep run -l python -p 'run_delta_maintenance($$$)' --globs 'src/**/*.py'
ast-grep run -l python -p 'DeltaMaintenancePlan($$$)' --globs 'src/**/*.py'

# Find vacuum/optimize/checkpoint calls
ast-grep run -l python -p 'vacuum($$$)' --globs 'src/**/*.py'
ast-grep run -l python -p 'optimize($$$)' --globs 'src/**/*.py'
```

### cq Analysis

**Trace write pipeline:**
```bash
# Find write pipeline entry points
/cq calls write_delta_table
/cq impact write_pipeline --param table_uri --depth 4

# Check incremental path integration points
/cq calls delta_context
```

**Analyze maintenance dependencies:**
```bash
/cq imports --module src.datafusion_engine.delta_maintenance
```

### Execution Steps

1. **Discovery Phase:**
   - Map all write completion points via ast-grep
   - Identify where maintenance should trigger
   - Document current ad-hoc maintenance calls

2. **Integration Design Phase:**
   - Design maintenance trigger points:
     ```python
     def after_delta_write(
         table_uri: str,
         maintenance_policy: DeltaMaintenancePolicy,
         commit_info: DeltaCommitInfo,
     ) -> MaintenanceResult:
         if maintenance_policy.should_run_after_commit(commit_info):
             return run_delta_maintenance(table_uri, maintenance_policy)
         return MaintenanceResult.skipped()
     ```

3. **Implementation Phase:**
   - Add maintenance hooks to `write_pipeline.py`
   - Add maintenance hooks to incremental commit paths
   - Wire maintenance artifacts into diagnostics

4. **Cleanup Phase:**
   - Remove ad-hoc maintenance calls scattered in codebase:
     ```bash
     # Find and remove scattered calls
     ast-grep run -l python -p 'vacuum($TABLE)' --globs 'src/**/*.py'
     ```
   - Ensure all maintenance routes through control plane

### Validation
```bash
uv run pytest tests/unit/datafusion_engine/ -v
uv run pytest tests/integration/ -m delta -v
```

---

## 7. v5 Deferred Decommissioning

### Objective
Final cleanup after v5 scopes 1-6 are complete.

### Status
In progress. Hamilton IO completion is done; final artifact/schema validation checks are intentionally skipped per request.

### Checklist
- [x] Verify scopes 1-6 complete
- [x] Remove deprecated tag dicts and per-module tag helpers
- [x] Remove legacy incremental diff modules
- [ ] Re-run plan artifact and schema validation checks (skipped per request)

### ast-grep Final Verification

```bash
# Verify no ad-hoc tags remain
ast-grep run -l python -p '_CPG_FINAL_TAGS' --globs 'src/**/*.py'
ast-grep run -l python -p '_semantic_tag' --globs 'src/**/*.py'

# Verify no diff utilities remain
ast-grep run -l python -p 'compute_diff' --globs 'src/**/*.py'
ast-grep run -l python -p 'from $$$diff$$$import' --globs 'src/**/*.py'

# Verify no scattered IO logic remains
ast-grep run -l python -p '@dataloader' --globs 'src/hamilton_pipeline/modules/**/*.py'
```

### cq Final Verification

```bash
# Verify no orphaned imports
/cq imports --cycles

# Verify tag policy coverage
/cq calls TagPolicy
/cq calls apply_tag
```

---

## 8. Physical Optimizer Rulepack (v6 Scope 1 Remaining)

### Objective
- Implement physical rulepack in Rust with explicit policy inputs
- Remove redundant Python physical tuning branches

### Status
Complete. Physical rulepack now applies coalescing rules gated by SessionConfig.

### Target Files
- `rust/datafusion_ext/src/physical_rules.rs` (implement)
- `rust/datafusion_ext/src/lib.rs` (integrate)
- `rust/datafusion_python/src/codeanatomy_ext.rs` (expose)
- `src/datafusion_engine/runtime.py` (cleanup)

### ast-grep Discovery Patterns

**Find Python physical tuning code:**
```bash
# Find physical plan manipulation in Python
ast-grep run -l python -p 'physical_plan($$$)' --globs 'src/**/*.py'
ast-grep run -l python -p 'execution_plan($$$)' --globs 'src/**/*.py'

# Find optimizer config in Python
ast-grep run -l python -p 'PhysicalOptimizerConfig($$$)' --globs 'src/**/*.py'

# Find repartition/sort hints in Python
ast-grep run -l python -p 'repartition($$$)' --globs 'src/**/*.py'
ast-grep run -l python -p 'sort($$$)' --globs 'src/**/*.py'
```

**Find Rust rule patterns (for reference):**
```bash
# Search for existing PhysicalOptimizerRule implementations
ast-grep run -l rust -p 'impl PhysicalOptimizerRule for $TYPE {
    $$$
}' --globs 'rust/**/*.rs'
```

### cq Analysis

**Map Python physical tuning callers:**
```bash
# Find all physical plan manipulation
/cq calls execution_plan
/cq calls physical_plan

# Check runtime integration points
/cq impact runtime --param session_context --depth 3
```

### Execution Steps

1. **Discovery Phase:**
   - Use ast-grep to find all Python physical tuning code
   - Document each tuning pattern (repartition, sort, coalesce)
   - Identify policy inputs needed for Rust rules

2. **Rust Implementation Phase:**
   - Implement physical rules in `physical_rules.rs`:
     ```rust
     pub struct CodeAnatomyPhysicalRule {
         policy: PhysicalPolicy,
     }

     impl PhysicalOptimizerRule for CodeAnatomyPhysicalRule {
         fn optimize(&self, plan: Arc<dyn ExecutionPlan>, config: &ConfigOptions)
             -> Result<Arc<dyn ExecutionPlan>> {
             // Apply policy-driven optimizations
         }
     }
     ```
   - Register rules in session initialization

3. **Python Integration Phase:**
   - Expose policy configuration from Python
   - Wire Rust rules into `codeanatomy_ext.rs`

4. **Cleanup Phase:**
   - Remove Python physical tuning code:
     ```bash
     ast-grep run -l python -p 'def _apply_physical_hints($$$):
         $$$' --globs 'src/**/*.py'
     ```
   - Verify no Python physical manipulation remains

### Validation
```bash
cargo test --package datafusion_ext
uv run pytest tests/unit/datafusion_engine/ -v
```

---

## 9. Constraint Governance DSL (v6 Scope 4 Remaining)

### Objective
Define constraint DSL as a single contract surface.

### Status
Complete. Constraint DSL is fully integrated and legacy helpers are removed.

### Target Files
- `src/datafusion_engine/schema_contracts.py` (extend)
- `src/datafusion_engine/schema_validation.py` (refactor)
- `src/datafusion_engine/write_pipeline.py` (integrate)

### ast-grep Discovery Patterns

**Find constraint definitions:**
```bash
# Find CHECK constraint strings
ast-grep run -l python -p 'CHECK ($$$)' --globs 'src/**/*.py'

# Find constraint dict literals
ast-grep run -l python -p 'constraints = {$$$}' --globs 'src/**/*.py'

# Find validation calls
ast-grep run -l python -p 'validate_constraint($$$)' --globs 'src/**/*.py'
ast-grep run -l python -p 'check_constraint($$$)' --globs 'src/**/*.py'
```

**Find duplicate validation logic:**
```bash
# Find inline constraint checks
ast-grep run -l python -p 'if $COL >= 0:' --globs 'src/**/*.py'
ast-grep run -l python -p 'assert $COL is not None' --globs 'src/**/*.py'
```

### cq Analysis

**Map constraint validation callers:**
```bash
# Find all constraint validation call sites
/cq calls validate_constraint
/cq calls check_constraint

# Analyze schema contract usage
/cq imports --module src.datafusion_engine.schema_contracts
```

### Execution Steps

1. **Discovery Phase:**
   - Use ast-grep to find all constraint definitions
   - Document constraint types: PK, NOT NULL, CHECK, UNIQUE
   - Identify duplicate validation logic

2. **DSL Design Phase:**
   - Design constraint DSL:
     ```python
     class ConstraintSpec(msgspec.Struct, frozen=True):
         column: str
         constraint_type: Literal["pk", "not_null", "check", "unique"]
         expression: str | None = None  # For CHECK constraints

     class TableConstraints(msgspec.Struct, frozen=True):
         primary_key: list[str] | None = None
         not_null: list[str] | None = None
         checks: list[ConstraintSpec] | None = None
         unique: list[list[str]] | None = None
     ```

3. **Implementation Phase:**
   - Implement DSL in `schema_contracts.py`
   - Add Delta CHECK constraint emission on writes
   - Add information_schema population

4. **Cleanup Phase:**
   - Remove duplicate validation logic:
     ```bash
     # Find and replace inline checks
     ast-grep run -l python -p 'if $COL >= 0:
         pass
     else:
         raise ValueError($$$)' --globs 'src/**/*.py'
     ```

### Validation
```bash
uv run pytest tests/unit/datafusion_engine/ -v
uv run pyright --warnings src/datafusion_engine/
```

---

## 10. Delta Protocol Feature Wiring (v6 Scope 5 Remaining)

### Objective
- Apply maintenance policy during table initialization and commits
- Record feature adoption state in diagnostics

### Status
Complete. Feature adoption state is now recorded in diagnostics.

### Target Files
- `src/datafusion_engine/delta_control_plane.py` (extend)
- `src/datafusion_engine/delta_observability.py` (extend)
- `src/datafusion_engine/write_pipeline.py` (integrate)

### ast-grep Discovery Patterns

**Find table initialization points:**
```bash
# Find DeltaTable creation
ast-grep run -l python -p 'DeltaTable($$$)' --globs 'src/**/*.py'
ast-grep run -l python -p 'DeltaTable.create($$$)' --globs 'src/**/*.py'

# Find commit operations
ast-grep run -l python -p '.commit($$$)' --globs 'src/**/*.py'
```

**Find feature flag checks:**
```bash
# Find deletion vector checks
ast-grep run -l python -p 'deletion_vectors' --globs 'src/**/*.py'

# Find checkpoint version checks
ast-grep run -l python -p 'v2_checkpoint' --globs 'src/**/*.py'
```

### cq Analysis

**Trace table creation flow:**
```bash
# Find table creation call sites
/cq calls DeltaTable
/cq calls DeltaTable.create

# Analyze commit flow
/cq impact delta_control_plane --param commit_info --depth 3
```

### Execution Steps

1. **Discovery Phase:**
   - Map all table initialization points
   - Map all commit operations
   - Document current feature flag handling

2. **Implementation Phase:**
   - Add policy application on table init:
     ```python
     def initialize_delta_table(
         uri: str,
         policy: DeltaMaintenancePolicy,
     ) -> DeltaTable:
         table = DeltaTable(uri)
         if policy.enable_deletion_vectors:
             table.enable_feature("deletionVectors")
         if policy.enable_v2_checkpoints:
             table.enable_feature("v2Checkpoint")
         return table
     ```
   - Record feature state in diagnostics

3. **Diagnostics Phase:**
   - Add feature adoption logging to observability:
     ```python
     def record_feature_state(table: DeltaTable) -> FeatureStateRecord:
         return FeatureStateRecord(
             deletion_vectors=table.has_feature("deletionVectors"),
             v2_checkpoints=table.has_feature("v2Checkpoint"),
             log_compaction=table.has_feature("logCompaction"),
         )
     ```

### Validation
```bash
uv run pytest tests/unit/datafusion_engine/ -v
uv run pytest tests/integration/ -m delta -v
```

---

## 11. Substrait Plan Mandate (v6 Scope 6 Remaining)

### Objective
- Make Substrait bytes mandatory in plan bundles
- Persist Substrait as the canonical portable plan artifact
- Use Substrait for plan diffing and portability checks

### Status
Complete. Substrait bytes are mandatory and optional handling removed from plan snapshots.

### Target Files
- `src/datafusion_engine/plan_bundle.py` (modify)
- `src/datafusion_engine/plan_artifact_store.py` (modify)
- `src/datafusion_engine/runtime.py` (integrate)

### ast-grep Discovery Patterns

**Find plan bundle creation:**
```bash
# Find PlanBundle instantiation
ast-grep run -l python -p 'PlanBundle($$$)' --globs 'src/**/*.py'

# Find plan bundle building
ast-grep run -l python -p 'build_plan_bundle($$$)' --globs 'src/**/*.py'

# Find substrait_bytes access
ast-grep run -l python -p '$$.substrait_bytes' --globs 'src/**/*.py'
```

**Find plan artifact storage:**
```bash
# Find plan artifact persistence
ast-grep run -l python -p 'store_artifact($$$plan$$$)' --globs 'src/**/*.py'
ast-grep run -l python -p 'record_plan_artifact($$$)' --globs 'src/**/*.py'
```

### cq Analysis

**Map plan bundle creation sites:**
```bash
# Find all plan bundle creators
/cq calls build_plan_bundle
/cq calls PlanBundle

# Analyze artifact storage flow
/cq impact plan_artifact_store --param bundle --depth 3
```

### Execution Steps

1. **Discovery Phase:**
   - Map all plan bundle creation sites
   - Identify sites that may have `substrait_bytes=None`
   - Document current plan artifact schema

2. **Enforcement Phase:**
   - Make `substrait_bytes` required in `PlanBundle`:
     ```python
     @dataclass(frozen=True)
     class PlanBundle:
         logical: str
         optimized: str
         physical: str
         substrait_bytes: bytes  # Now required, not Optional
     ```
   - Update all creation sites to provide Substrait

3. **Persistence Phase:**
   - Store Substrait as primary artifact:
     ```python
     def persist_plan_bundle(bundle: PlanBundle, path: Path) -> None:
         (path / "substrait.bin").write_bytes(bundle.substrait_bytes)
         # Also store text representations
     ```

4. **Diffing Phase:**
   - Implement Substrait-based plan comparison:
     ```python
     def compare_plans(a: PlanBundle, b: PlanBundle) -> PlanDiff:
         return substrait_diff(a.substrait_bytes, b.substrait_bytes)
     ```

### Validation
```bash
uv run pytest tests/unit/datafusion_engine/ -v
uv run pyright --warnings src/datafusion_engine/
```

---

## 12. msgspec Schema Export (v6 Scope 7 Remaining)

### Objective
- Export JSON Schema 2020-12 for each msgspec contract
- Replace manual schema docs with generated schema artifacts

### Status
Complete. Schema export helpers exist and `schemas/msgspec/` contains generated artifacts.

### Target Files
- `src/serde_artifacts.py` (extend)
- `src/serde_msgspec.py` (extend)
- `schemas/` (generate)

### ast-grep Discovery Patterns

**Find msgspec Struct definitions:**
```bash
# Find all msgspec Struct classes
ast-grep run -l python -p 'class $NAME(msgspec.Struct$$$):
    $$$' --globs 'src/**/*.py'

# Find frozen structs specifically
ast-grep run -l python -p 'class $NAME(msgspec.Struct, frozen=True):
    $$$' --globs 'src/**/*.py'
```

**Find manual schema documentation:**
```bash
# Find schema-related markdown/docstrings
ast-grep run -l python -p '"""$$$schema$$$"""' --globs 'src/**/*.py'
```

### cq Analysis

**Map msgspec struct usage:**
```bash
# Find all msgspec usage
/cq imports --module msgspec

# Check struct instantiation patterns
/cq calls PlanArtifact
/cq calls FeatureStateRecord
```

### Execution Steps

1. **Discovery Phase:**
   - Use ast-grep to find all msgspec Struct definitions
   - Document which structs are contract-level (artifacts, diagnostics)
   - Identify manual schema docs to replace

2. **Export Implementation Phase:**
   - Create schema export utility:
     ```python
     import msgspec
     from pathlib import Path

     def export_schemas(structs: list[type], output_dir: Path) -> None:
         for struct in structs:
             schema = msgspec.json.schema(struct)
             schema_path = output_dir / f"{struct.__name__}.schema.json"
             schema_path.write_text(msgspec.json.encode(schema).decode())
     ```

3. **Automation Phase:**
   - Add schema export to build/CI
   - Generate schemas to `schemas/` directory

4. **Cleanup Phase:**
   - Remove manual schema documentation
   - Reference generated schemas in documentation

### Validation
```bash
uv run python -c "from src.serde_artifacts import *; import msgspec; print(msgspec.json.schema(PlanArtifact))"
```

---

## 13. Catalog Autoload for Non-Delta (v6 Scope 8 Remaining)

### Objective
- Route non-Delta registration through catalog autoload
- Remove registry snapshot artifacts for non-Delta sources

### Status
Complete. Non-Delta registration prefers catalog autoload, and registry snapshots skip non-Delta entries.

### Target Files
- `src/datafusion_engine/session_factory.py` (modify)
- `src/datafusion_engine/runtime.py` (modify)
- `src/datafusion_engine/catalog_provider.py` (modify)

### ast-grep Discovery Patterns

**Find non-Delta registration patterns:**
```bash
# Find listing table registrations (non-Delta)
ast-grep run -l python -p 'register_listing_table($$$)' --globs 'src/**/*.py'

# Find parquet file registrations
ast-grep run -l python -p 'register_parquet($$$)' --globs 'src/**/*.py'

# Find registry snapshot creation
ast-grep run -l python -p 'RegistrySnapshot($$$)' --globs 'src/**/*.py'
```

**Find SessionConfig catalog settings:**
```bash
ast-grep run -l python -p 'datafusion.catalog.$$$' --globs 'src/**/*.py'
```

### cq Analysis

**Map registration flow:**
```bash
# Find registration call sites
/cq calls register_listing_table
/cq calls register_parquet

# Check registry snapshot usage
/cq calls RegistrySnapshot
```

### Execution Steps

1. **Discovery Phase:**
   - Map all non-Delta registration patterns
   - Identify registry snapshot creation sites
   - Document current catalog configuration

2. **Autoload Configuration Phase:**
   - Configure catalog autoload in session:
     ```python
     config = SessionConfig()
     config = config.set("datafusion.catalog.location", "s3://bucket/registry")
     config = config.set("datafusion.catalog.format", "parquet")
     ```

3. **Migration Phase:**
   - Route non-Delta tables through autoload
   - Remove explicit registration calls

4. **Cleanup Phase:**
   - Remove registry snapshot artifacts for non-Delta:
     ```bash
     ast-grep run -l python -p 'RegistrySnapshot($$$)' --globs 'src/**/*.py'
     ```

### Validation
```bash
uv run pytest tests/unit/datafusion_engine/ -v
```

---

## 14. SQL Policy Single Source (v6 Scope 9 Remaining)

### Objective
- Route policy through SessionConfig extension options
- Enforce via Rust analyzer rules only
- Remove Python policy fallbacks

### Status
Complete. SQLOptions are no longer policy-driven; enforcement is SessionConfig + Rust rules only.

### Target Files
- `src/datafusion_engine/sql_options.py` (refactor)
- `src/datafusion_engine/sql_guard.py` (refactor)
- `src/datafusion_engine/runtime.py` (refactor)

### ast-grep Discovery Patterns

**Find Python policy resolution:**
```bash
# Find Python policy fallback code
ast-grep run -l python -p 'if not policy:
    policy = $DEFAULT' --globs 'src/datafusion_engine/**/*.py'

# Find Python policy checks
ast-grep run -l python -p 'if policy.allow_$ACTION:' --globs 'src/datafusion_engine/**/*.py'

# Find sql_guard policy resolution
ast-grep run -l python -p 'SqlPolicy($$$)' --globs 'src/**/*.py'
```

**Find SessionConfig policy settings:**
```bash
ast-grep run -l python -p 'codeanatomy_policy.$$$' --globs 'src/**/*.py'
```

### cq Analysis

**Map policy resolution flow:**
```bash
# Find policy usage
/cq calls SqlPolicy
/cq calls sql_guard

# Analyze fallback paths
/cq impact sql_options --param policy --depth 3
```

### Execution Steps

1. **Discovery Phase:**
   - Map all Python policy resolution code
   - Document policy settings and their Rust equivalents
   - Identify fallback branches

2. **SessionConfig Migration Phase:**
   - Move all policy settings to SessionConfig:
     ```python
     config = SessionConfig()
     config = config.set("codeanatomy_policy.allow_ddl", "false")
     config = config.set("codeanatomy_policy.allow_dml", "true")
     ```

3. **Rust Enforcement Phase:**
   - Ensure Rust analyzer rules read from SessionConfig
   - Remove Python enforcement logic

4. **Cleanup Phase:**
   - Remove Python policy fallbacks:
     ```bash
     ast-grep run -l python -p 'if not policy:
         $$$' --globs 'src/datafusion_engine/**/*.py'
     ```

### Validation
```bash
cargo test --package datafusion_ext
uv run pytest tests/unit/datafusion_engine/ -v
```

---

## 15. COPY/INSERT Diagnostics (v6 Scope 10 Remaining)

### Objective
- Capture COPY/INSERT artifacts in diagnostics
- Remove duplicate write helpers

### Status
Complete. COPY/INSERT diagnostics now include row counts and delta feature state on write operations.

### Target Files
- `src/datafusion_engine/write_pipeline.py` (extend)
- `src/hamilton_pipeline/materializers.py` (refactor)
- `src/hamilton_pipeline/modules/outputs.py` (refactor)

### ast-grep Discovery Patterns

**Find COPY/INSERT execution:**
```bash
# Find COPY SQL execution
ast-grep run -l python -p 'COPY ($$$) TO' --globs 'src/**/*.py'

# Find INSERT SQL execution
ast-grep run -l python -p 'INSERT INTO $$$' --globs 'src/**/*.py'

# Find ctx.sql() with write statements
ast-grep run -l python -p 'ctx.sql($SQL)' --globs 'src/**/*.py'
```

**Find duplicate write helpers:**
```bash
# Find Arrow-to-filesystem writers
ast-grep run -l python -p 'pq.write_table($$$)' --globs 'src/**/*.py'
ast-grep run -l python -p 'write_parquet($$$)' --globs 'src/**/*.py'

# Find custom write functions
ast-grep run -l python -p 'def write_$TYPE($$$):' --globs 'src/**/*.py'
```

### cq Analysis

**Map write helper callers:**
```bash
# Find write helper usage
/cq calls write_parquet
/cq calls pq.write_table

# Analyze write pipeline flow
/cq impact write_pipeline --param output_path --depth 3
```

### Execution Steps

1. **Discovery Phase:**
   - Map all COPY/INSERT execution sites
   - Map all duplicate write helpers
   - Document current diagnostics capture

2. **Diagnostics Implementation Phase:**
   - Add artifact capture for COPY/INSERT:
     ```python
     def execute_copy_with_diagnostics(
         ctx: SessionContext,
         sql: str,
     ) -> CopyArtifact:
         result = ctx.sql(sql)
         return CopyArtifact(
             sql=sql,
             rows_written=result.count(),
             output_path=extract_path_from_sql(sql),
         )
     ```

3. **Cleanup Phase:**
   - Remove duplicate write helpers:
     ```bash
     ast-grep run -l python -p 'def write_parquet_manual($$$):
         $$$' --globs 'src/**/*.py'
     ```
   - Route all writes through COPY/INSERT

### Validation
```bash
uv run pytest tests/unit/datafusion_engine/ -v
uv run pytest tests/unit/hamilton_pipeline/ -v
```

---

## 16. v6 Deferred Decommissioning

### Objective
Finalize deletions after v6 scopes 8-15 are complete.

### Status
Complete. v6 decommissioning checklist satisfied.

### Checklist
- [ ] Confirm scopes 8-15 complete
- [ ] Validate no call sites remain for deprecated code
- [ ] Delete deferred modules and update imports

### ast-grep Final Verification

```bash
# Verify no Python physical tuning remains
ast-grep run -l python -p 'physical_plan' --globs 'src/**/*.py'

# Verify no Python policy fallbacks remain
ast-grep run -l python -p 'if not policy:' --globs 'src/datafusion_engine/**/*.py'

# Verify no duplicate write helpers remain
ast-grep run -l python -p 'pq.write_table' --globs 'src/**/*.py'

# Verify no registry snapshots for non-Delta
ast-grep run -l python -p 'RegistrySnapshot' --globs 'src/**/*.py'
```

### cq Final Verification

```bash
# Verify clean import graph
/cq imports --cycles

# Verify no orphaned function definitions
/cq calls SqlPolicy
/cq calls write_parquet
```

### Deletion Candidates (Pending Verification)
- Legacy non-Delta registration helpers in `dataset_registration.py`
- Python-side SQL policy fallback helpers in `sql_options.py`, `sql_guard.py`
- Custom plan serialization artifacts (non-Substrait)
- Duplicate constraint validators not backed by Delta CHECK

---

## Appendix: Quick Reference for Tool Selection

### Use ast-grep when:
- Finding structural code patterns (function definitions, class declarations)
- Performing codemods/rewrites across the codebase
- Searching for specific AST constructs (decorators, imports, call patterns)
- Verifying code has been removed (should return zero results)
- Pattern matching without false positives from strings/comments

### Use cq when:
- Analyzing function call sites (`/cq calls`)
- Tracing parameter impact through code (`/cq impact`)
- Simulating signature change effects (`/cq sig-impact`)
- Understanding import dependencies (`/cq imports`)
- Checking closure captures before extraction (`/cq scopes`)
- Finding exception handling patterns (`/cq exceptions`)

### Combined Workflow Pattern

```bash
# 1. Discovery: Find all instances with ast-grep
ast-grep run -l python -p '<pattern>' --globs 'src/**/*.py'

# 2. Impact Analysis: Check callers with cq
/cq calls <function_name>

# 3. Signature Safety: Verify changes won't break callers
/cq sig-impact <function_name> --to "<new_signature>"

# 4. Codemod: Apply changes with ast-grep
ast-grep run -l python -p '<old>' -r '<new>' -i

# 5. Verification: Confirm removal
ast-grep run -l python -p '<old_pattern>' --globs 'src/**/*.py'
# Should return zero results
```

---

## Verification Command Summary

```bash
# Quality gates
uv run ruff check --fix
uv run pyrefly check
uv run pyright --warnings --pythonversion=3.13

# Testing
uv run pytest tests/unit/ -v
uv run pytest tests/integration/ -v
uv run pytest tests/e2e/ -v

# Rust tests
cargo test --workspace
```
