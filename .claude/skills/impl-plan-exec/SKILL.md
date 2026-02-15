---
name: impl-plan-exec
description: Execute an implementation plan document end-to-end without stopping. Implements all scope items, tracks progress, verifies completeness against the plan, then runs quality gates in strict order.
allowed-tools: Read, Glob, Grep, Ripgrep, AST-grep, Bash, Write, Edit, Task, TaskCreate, TaskUpdate, TaskList, TaskGet, Skill, EnterPlanMode, ExitPlanMode
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

### 5. Use the task list to focus execution, not just track it

The task list exists so you can **focus on one work unit at a time** without re-reading and re-analyzing the entire plan between tasks. Without this discipline, you will cycle through reviewing every affected file repeatedly and never land on actually writing code. The task list is your mechanism for breaking the plan into digestible, independently-actionable work units.

**Task granularity — group by interdependency, not 1:1 with scope items:**

Tasks should NOT be a literal mirror of the plan's scope item list. Instead, group and sequence scope items based on a high-level assessment of their implementation interdependencies:

- **Interdependent scope items go in the same task.** If `S1` creates a contract that `S2` and `S3` immediately consume, those three form one task (e.g., "Implement X contract and wire its consumers").
- **Independent scope items are separate tasks.** If `S4` touches entirely different files with no dependency on `S1`–`S3`, it is its own task.
- **Decommission work attaches to the task whose scope items it depends on**, not as a standalone task — unless the decommission batch is truly independent of all scope items in the plan.

**Task descriptions must be self-contained.** Each task description should capture enough context — which files to read/edit/create, what the changes accomplish, and any relevant snippet guidance from the plan — so that you can execute it by reading only that task description and the relevant source files. You should **not** need to re-read the full plan document to work on an individual task.

**Workflow per task:**
1. Mark the task `in_progress`.
2. Read the task description, then read the **applicable sections** of the plan for this task's scope items. Do not re-read the entire plan — only the sections relevant to the scope items grouped into this task.
3. Read the source files relevant to this task.
4. Implement the changes. Do not review or analyze files outside this task's scope.
5. Mark the task `completed`.
6. Move to the next task immediately. Do not re-read the entire plan between tasks.

---

## Execution Procedure

### Phase 1: Plan Ingestion and Task Decomposition

1. **Read the plan document** end-to-end to understand full scope. This is the first of **two** full reads — the second is the completeness audit in Phase 3.
2. **Extract the implementation sequence** from the plan (the plan's "Implementation Sequence" section).
3. **Assess interdependencies** across scope items, considering:
   - Explicit dependency chains stated in the plan.
   - Implicit coupling: scope items that create and consume the same contracts, touch the same files, or modify the same module interfaces.
   - Foundation-first ordering: contracts and core modules before their consumers.
4. **Create the task list** by grouping interdependent scope items into work units:
   - Each task groups one or more scope items (and any associated decommission work) that should land together.
   - Write a **self-contained description** for each task that includes: the goal, the files to read/edit/create, the key changes, and any relevant code snippet guidance from the plan. The description should be sufficient to execute the task without re-reading the plan.
   - Set `addBlockedBy` relationships between tasks where one task's output is required by another.
   - Order tasks foundation-first: tasks that produce contracts or core interfaces come before tasks that consume them.

### Phase 2: Scope Implementation

For each task in execution order:

1. **Mark task `in_progress`.**
2. **Read the task description** (via `TaskGet`), then read the **applicable sections** of the plan for this task's scope items. Focus only on those sections — do not re-read the entire plan.
3. **Read only the source files relevant to this task.**
4. **Implement the changes:**
   - Create new files as needed.
   - Edit existing files as specified.
   - Follow representative code snippets as architectural guidance (adapt to actual codebase state; snippets are illustrative, not copy-paste).
   - Execute any decommission deletions grouped into this task.
5. **Write tests** for new modules. Every new file under `tools/` or `src/` must have a corresponding test file.
6. **Mark task `completed`.**
7. **Proceed immediately** to the next task. Do not pause. Do not re-read the entire plan.

### Phase 3: Completeness Verification

After all tasks are completed:

1. **Re-read the plan document** from top to bottom (this is the second and final full read).
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
