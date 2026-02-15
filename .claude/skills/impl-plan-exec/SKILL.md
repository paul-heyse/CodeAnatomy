---
name: impl-plan-exec
description: Execute an implementation plan document end-to-end without stopping. Implements all scope items, tracks progress, verifies completeness against the plan, then runs quality gates in strict order.
allowed-tools: Read, Glob, Grep, Bash, Write, Edit, Task, TaskCreate, TaskUpdate, TaskList, TaskGet, Skill, EnterPlanMode, ExitPlanMode
user-invocable: true
---

# Implementation Plan Executor

Execute the entirety of an implementation plan document without stopping. This skill is for landing complete plan scope in a single session with tracked progress, completeness verification, and sequenced quality gates.

## When to Use

Invoke `/impl-plan-exec` when you have a finalized implementation plan document under `docs/plans/` and need to execute all scope items end-to-end.

## Arguments

- **plan-path** (required): Path to the plan document (absolute or relative to repo root).

Examples:
```
/impl-plan-exec docs/plans/cq_tree_sitter_capability_expansion_implementation_plan_v1_2026-02-15.md
/impl-plan-exec docs/plans/datafusion_engine_decomposition_implementation_plan_v1_2026-02-12.md
```

---

## Critical Operating Rules

### 1. Do not stop midway

Implement the **entirety** of the plan scope without pausing for confirmation between scope items. The only acceptable stopping points are:

- A scope item requires a design decision not covered by the plan (ask the user, then continue).
- An unrecoverable error blocks further progress (report it, then continue with remaining scope items).

### 2. Do not use the tool you are editing

When the plan modifies a tool or module, **do not use that tool for analysis during execution**. Specifically:

- If the plan modifies `tools/cq/`, do **not** invoke `/cq` commands. Use `Grep` (ripgrep) and `/ast-grep` instead for code search and structural matching.
- If the plan modifies `src/datafusion_engine/`, do not rely on DataFusion runtime probes for analysis of files being edited.

This prevents circular dependency where the tool under modification produces unreliable results about its own code.

### 3. Defer all quality checks until full scope is implemented

Do **not** run `ruff`, `pyrefly`, `pyright`, or `pytest` until every scope item in the plan is implemented. Running quality checks mid-implementation creates noise from incomplete intermediate states and wastes time on errors that will self-resolve as dependent scope items land.

### 4. Keep pytest scope focused

When running pytest, scope it to the subsystem being modified. Do not run the full test suite. Examples:
- Plan modifies `tools/cq/` → `uv run pytest tests/unit/cq/ tests/e2e/cq/ -v`
- Plan modifies `src/semantics/` → `uv run pytest tests/unit/test_semantic*.py -v`
- Plan modifies `src/datafusion_engine/` → `uv run pytest tests/unit/datafusion_engine/ -v`

### 5. Track progress with the task list

Before beginning implementation, create a task for each scope item (`S1`, `S2`, ...) and each decommission batch (`D1`, `D2`, ...) using `TaskCreate`. Set up dependency ordering with `addBlockedBy`/`addBlocks` where the plan specifies dependencies between scope items. Mark each task `in_progress` when starting and `completed` when done.

---

## Execution Procedure

### Phase 1: Plan Ingestion and Sequencing

1. **Read the plan document** end-to-end to understand full scope.
2. **Extract the implementation sequence** from the plan (the plan's "Implementation Sequence" section).
3. **Determine optimal execution order** considering:
   - Explicit dependency chains stated in the plan.
   - Shared-file batching (scope items touching the same files should be adjacent).
   - Foundation-first ordering (contracts and core modules before consumers).
4. **Create task list** with one task per scope item and one per decommission batch, in execution order. Set `addBlockedBy` relationships where the plan specifies dependency chains (e.g., `D1` blocked by `S1`, `S2`, `S3`, `S4`).

### Phase 2: Scope Implementation

For each scope item in execution order:

1. **Mark task `in_progress`.**
2. **Read the plan section** for this scope item (Goal, Representative Code Snippets, Files to Edit, New Files to Create, Legacy Decommission/Delete Scope).
3. **Implement the scope item:**
   - Create new files listed under "New Files to Create."
   - Edit existing files listed under "Files to Edit."
   - Follow the representative code snippets as architectural guidance (adapt to actual codebase state; snippets are illustrative, not copy-paste).
   - Execute the per-item "Legacy Decommission/Delete Scope" deletions.
4. **Write tests** for new modules. Every new file under `tools/` or `src/` must have a corresponding test file.
5. **Mark task `completed`.**
6. **Proceed immediately** to the next scope item. Do not pause.

For decommission batches:

1. **Verify all prerequisite scope items are completed** (check task list).
2. **Execute the batch deletions** specified in the plan.
3. **Mark task `completed`.**

### Phase 3: Completeness Verification

After all scope items and decommission batches are implemented:

1. **Re-read the plan document** from top to bottom.
2. **For each scope item**, verify against the codebase:
   - Are all "Files to Edit" actually modified?
   - Are all "New Files to Create" present?
   - Are all "Legacy Decommission/Delete Scope" items actually removed?
   - Do the implementations align with the representative code snippets (structurally, not literally)?
3. **For each decommission batch**, verify all listed deletions were executed.
4. **For the Implementation Checklist**, mentally check each box. If any item is not fully implemented, go back and complete it before proceeding.
5. **Report any gaps** found. If gaps exist, fix them now.

### Phase 4: Quality Gates (Strict Sequence)

Run quality gates in this **exact order**. Resolve all errors at each stage before proceeding to the next.

#### Step 1: Format
```bash
uv run ruff format
```

#### Step 2: Lint
```bash
uv run ruff check --fix
```
Fix any remaining errors that `--fix` cannot auto-resolve. Re-run until clean.

#### Step 3: Type checking (strict gate)
```bash
uv run pyrefly check
```
Fix all errors. This is the strict gate; do not skip or suppress errors.

#### Step 4: Type checking (IDE mode)
```bash
uv run pyright
```
Fix errors. Pyright runs in basic mode; focus on genuine type errors, not stylistic warnings.

#### Step 5: Pytest (scoped)
```bash
uv run pytest {scoped_test_dirs} -v
```
Run only the tests relevant to the plan's scope. Fix failures. Re-run until green.

### Phase 5: Completion Report

After all quality gates pass:

1. **List all files created or modified** (summary).
2. **List all files deleted** (summary).
3. **Confirm all plan checklist items are complete.**
4. **Report the final task list status** (all tasks should be `completed`).

---

## Analysis Tool Selection During Execution

| Need | When plan edits `tools/cq/` | Otherwise |
|------|-----------------------------|-----------|
| Find symbol usage | `Grep` (ripgrep) | `/cq search` |
| Structural pattern match | `/ast-grep` | `/cq q "pattern=..."` |
| Find file by name | `Glob` | `Glob` |
| Read file contents | `Read` | `Read` |
| Find callers/callsites | `Grep` + `/ast-grep` | `/cq calls` |
| Understand imports | `Grep` for `^from\|^import` | `/cq q "entity=import"` |

---

## Error Handling

- **Scope item blocked by missing dependency**: Skip it, continue with non-blocked items, come back after the blocker is resolved.
- **File listed in plan does not exist**: The plan may be stale. Create the file if appropriate, or note the discrepancy and continue.
- **Representative snippet uses an API that doesn't exist**: Probe the local environment (`uv run python -c "..."`) to find the correct API. Use `/cq-lib-ref` if the library is covered. Adapt the implementation.
- **Quality gate failure**: Fix the errors at that stage. Do not proceed to the next stage until the current one is clean. Do not go back and re-run earlier stages unless fixes introduced new issues.
