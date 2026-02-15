---
name: impl-plan
description: Generate a comprehensive implementation plan document from proposed scope items, with code snippets, file manifests, deprecation scope, and checklists
allowed-tools: Read, Glob, Grep, Bash, Write, Edit, Ripgrep, AST-grep
user-invocable: true
---

# Implementation Plan Generator

Use this skill to produce a comprehensive implementation plan from scope items proposed or discussed in the current conversation. The plan is saved as a markdown document under `docs/plans/`.

## When to Use

Invoke `/impl-plan` after scope items have been proposed, discussed, and agreed upon in the conversation. The skill transforms those scope items into a structured, actionable implementation plan document.

## Arguments

- **topic** (optional): Short kebab-case topic slug for the filename. If omitted, infer from conversation context.
- **version** (optional): Version number (default: `v1`).

Examples:
```
/impl-plan
/impl-plan tree-sitter-incremental-parse
/impl-plan tree-sitter-incremental-parse v2
```

## Output

A markdown file at:
```
docs/plans/{topic}_implementation_plan_{version}_{date}.md
```

Where `{date}` is today's date in `YYYY-MM-DD` format.

## Plan Document Structure

The generated plan **must** follow this structure exactly:

### 1. Title and Metadata
```markdown
# {Topic} Implementation Plan {version} ({date})
```

### 2. Scope Summary
A concise paragraph or bullet list summarizing the overall scope and goals. State design stance explicitly (e.g., "no compatibility shims", "hard cutover", "incremental migration").

### 3. Design Principles (if applicable)
Numbered list of non-negotiable constraints and design decisions that govern all scope items.

### 4. Current Baseline (if applicable)
Bullet list of observed current-state facts relevant to the scope. Ground each observation in a specific file or code pattern. This section prevents the plan from making incorrect assumptions.

### 5. Per-Scope-Item Sections

For **each** scope item, create a section with the following subsections:

#### `## S{N}. {Scope Item Title}`

##### `### Goal`
One to three sentences stating the concrete outcome.

##### `### Representative Code Snippets`
Working code examples that document:
- Key architectural elements and patterns to implement
- Library functions, APIs, and primitives to use
- Contract/struct definitions
- Integration points with existing code

Code snippets must be realistic and grounded in actual library APIs. Include file path comments showing where each snippet belongs.

##### `### Files to Edit`
Bullet list of existing files that require modification.

##### `### New Files to Create`
Bullet list of new files (implementation + tests). Every new module must have a corresponding test file.

##### `### Legacy Decommission/Delete Scope`
Bullet list of code, functions, modules, or patterns within this scope item that become legacy and should be deleted. Be specific: name the file, the function/class/variable, and why it is superseded.

Each scope item section ends with a `---` separator.

### 6. Cross-Scope Legacy Decommission and Deletion Plan
A dedicated section at the end of the plan for deprecations that depend on **multiple** scope items landing. Organized as numbered batches:

```markdown
### Batch D{N} (after S{x}, S{y}, S{z})
- Delete {specific target} from {specific file} because {reason}.
```

Each batch lists its prerequisite scope items and the specific deletions that become safe only after all prerequisites are complete.

### 7. Implementation Sequence
Numbered list specifying the recommended implementation order, with brief rationale for ordering choices (dependency chains, risk reduction, etc.).

### 8. Implementation Checklist
A checkbox list (`- [ ]`) with one entry per scope item and one entry per decommission batch. This is the final section.

## Quality Requirements

1. **Code snippets must be grounded**: Reference actual library APIs (e.g., tree-sitter, DataFusion, diskcache, msgspec). Do not invent APIs. When uncertain, probe the environment or search the codebase first.

2. **File lists must be verified**: Use Glob/Grep to confirm that files listed under "Files to Edit" actually exist. Flag any files that could not be located.

3. **Deprecation scope must be specific**: Name the exact function, class, variable, or code pattern to delete. Vague statements like "delete old code" are not acceptable.

4. **Tests are mandatory**: Every new module in "New Files to Create" must have a corresponding test file under `tests/unit/` or `tests/e2e/`.

5. **No orphaned scope items**: Every scope item discussed in conversation must appear in the plan. If a scope item is deliberately excluded, state why in the Scope Summary.

6. **Consistent naming**: Use `S{N}` numbering for scope items and `D{N}` for decommission batches. Cross-reference by these identifiers.

## Procedure

When invoked, follow these steps:

1. **Gather scope items** from the preceding conversation. Identify each distinct scope item, its goal, and any code/architecture details discussed.

2. **Research the codebase** to ground the plan:
   - Use `/cq search` to find relevant existing code patterns.
   - Use Glob to verify file paths.
   - Use Grep to confirm current implementations that will be modified or deprecated.
   - Read key files to understand integration points.

3. **Draft the plan** following the structure above. For each scope item:
   - Write representative code snippets showing the target implementation pattern.
   - List files to edit (verified to exist).
   - List new files to create (with test files).
   - List specific legacy code to delete.

4. **Identify cross-scope deprecations** that require multiple scope items to land before deletion is safe. Organize into batches.

5. **Determine implementation sequence** based on dependency order and risk.

6. **Write the checklist** covering all scope items and decommission batches.

7. **Write the file** to `docs/plans/` using the naming convention.

8. **Report the file path** to the user.
