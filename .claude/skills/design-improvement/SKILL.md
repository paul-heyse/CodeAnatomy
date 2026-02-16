---
name: design-improvement
description: Review code for alignment with design principles (information hiding, SRP, DRY, CQS, etc.) and produce actionable improvement findings
allowed-tools: Read, Glob, Grep, Bash, Write, Edit, Task, TaskCreate, TaskUpdate, TaskList, TaskGet, Skill, EnterPlanMode, ExitPlanMode
user-invocable: true
---

# Design Improvement Review

Review a codebase scope for alignment with the project's design principles and produce actionable improvement findings. The design principles are defined in `docs/python_library_reference/design_principles.md`.

## When to Use

Invoke `/design-improvement` when you want to systematically audit a module, directory, or subsystem for design quality and identify concrete opportunities to better align the code with established design principles.

## Arguments

- **scope** (required): The directory, module, or file path to review. Use relative paths from repo root.
- **focus** (optional): Comma-separated list of principle numbers (1-24) or category slugs to prioritize. If omitted, all 24 principles are evaluated.
- **depth** (optional): `surface` (default), `moderate`, or `deep`. Controls how many files are sampled and how thoroughly each is analyzed.

Category slugs:
- `boundaries` — Principles 1-6 (system decomposition and boundaries)
- `knowledge` — Principles 7-11 (shared types, schemas, and knowledge)
- `composition` — Principles 12-15 (dependencies, composition, and construction)
- `correctness` — Principles 16-18 (effects, determinism, and pipeline correctness)
- `simplicity` — Principles 19-22 (simplicity, evolution, and predictability)
- `quality` — Principles 23-24 (testability and observability)

Examples:
```
/design-improvement src/semantics
/design-improvement src/relspec --focus boundaries,composition
/design-improvement src/datafusion_engine/sessions.py --focus 4,12,14 --depth deep
/design-improvement tools/cq/search --focus knowledge,quality --depth moderate
```

## Output

A markdown document saved to:
```
docs/reviews/design_review_{scope_slug}_{date}.md
```

Where `{scope_slug}` is the reviewed scope path with `/` replaced by `_`, and `{date}` is today's date in `YYYY-MM-DD` format.

---

## Design Principles Reference

The 24 principles are organized into six categories. Read `docs/python_library_reference/design_principles.md` for full descriptions. The summary below is for quick reference during review.

### Boundaries (1-6)
1. **Information hiding** — Modules own internal decisions; callers use stable surfaces
2. **Separation of concerns** — Policy/domain rules readable without IO/plumbing
3. **SRP (one reason to change)** — Each component changes for one reason
4. **High cohesion, low coupling** — Related concepts together; narrow interfaces between
5. **Dependency direction** — Core logic depends on nothing; details depend on core
6. **Ports & Adapters** — Core expresses needs via ports; adapters implement for specific tech

### Knowledge (7-11)
7. **DRY (knowledge, not lines)** — Single authoritative place for each invariant/schema/policy
8. **Design by contract** — Explicit preconditions, postconditions, invariants
9. **Parse, don't validate** — Convert messy inputs to structured representation once at boundary
10. **Make illegal states unrepresentable** — Data model prevents impossible combinations
11. **CQS** — Functions either return information or change state, not both

### Composition (12-15)
12. **Dependency inversion + explicit composition** — High-level depends on abstractions; DI over hidden creation
13. **Prefer composition over inheritance** — Build behavior by combining components
14. **Law of Demeter** — Talk to direct collaborators, not collaborator's collaborator
15. **Tell, don't ask** — Objects encapsulate rules; don't expose raw data for external logic

### Correctness (16-18)
16. **Functional core, imperative shell** — Deterministic transforms at center; IO at edges
17. **Idempotency** — Re-running with same inputs doesn't corrupt state
18. **Determinism / reproducibility** — Same inputs, same outputs; controlled nondeterminism

### Simplicity (19-22)
19. **KISS** — As simple as possible without sacrificing boundaries/clarity
20. **YAGNI** — No speculative generality; design seams, don't build unused extensions
21. **Least astonishment** — APIs behave as competent readers expect
22. **Declare and version public contracts** — Explicit stable surface vs internal detail

### Quality (23-24)
23. **Design for testability** — Units testable without heavyweight setup; DI, pure logic separated
24. **Observability** — Structured, consistent logging/metrics/tracing aligned to boundaries

---

## Review Procedure

### Phase 1: Scope Discovery

1. **Read the design principles** document (`docs/python_library_reference/design_principles.md`) to ground the review.
2. **Enumerate the scope.** Use `Glob` to list all Python files in the target scope. Record the file count.
3. **Sample files based on depth:**
   - `surface`: Up to 8 representative files (entry points, largest files, most-imported files).
   - `moderate`: Up to 20 files, prioritizing high-fan-in modules and public interfaces.
   - `deep`: All files in scope.
4. **If focus is specified**, narrow the review lens to only the specified principles. Still read the full files, but only flag findings for the focused principles.

### Phase 2: Per-Principle Analysis

For each principle in scope (or all 24 if no focus), systematically look for violations and improvement opportunities across the sampled files.

**Use CQ skills for evidence gathering** (unless the scope is `tools/cq/` itself):

| Analysis Need | Tool |
|--------------|------|
| Find coupling patterns | `/cq search` + `/cq q "entity=import"` |
| Identify God classes/modules | `/cq q "entity=class"` + `/cq q "entity=function"` with file counts |
| Find Law of Demeter violations | `/cq q "pattern='$X.$Y.$Z'"` (chained attribute access) |
| Find mixed query/command functions | `/cq q "pattern='return $X'"` cross-referenced with mutation patterns |
| Find raw data exposure | `/cq q "pattern='.$ATTR'"` for public attribute access patterns |
| Detect inheritance depth | `/cq q "entity=class expand=callers"` + `--format mermaid-class` |
| Find duplicated knowledge | `/cq search <invariant>` across modules |
| Assess testability | Check for constructor injection patterns, pure functions, IO separation |

**For each principle, evaluate:**

1. **Current alignment (0-3 scale):**
   - `0` = Principle actively violated; immediate risk
   - `1` = Significant gaps; improvement would reduce complexity or risk
   - `2` = Mostly aligned; minor improvements possible
   - `3` = Well aligned; no action needed

2. **Concrete findings:** Specific files, classes, or functions that demonstrate the gap, with line references.

3. **Suggested improvement:** A concrete, actionable suggestion (not vague advice). Reference specific code elements and describe the target state.

4. **Effort estimate:** `small` (< 1 hour), `medium` (1-4 hours), `large` (> 4 hours).

5. **Risk if unaddressed:** `low`, `medium`, `high`. What goes wrong if this stays as-is?

### Phase 3: Cross-Cutting Themes

After per-principle analysis, identify cross-cutting themes:

1. **Systemic patterns** — Are certain principle violations concentrated in specific modules? Do they share a root cause?
2. **Quick wins** — Which improvements have the highest alignment-gain-to-effort ratio?
3. **Structural blockers** — Are there design decisions that prevent multiple principles from being satisfied simultaneously? What architectural change would unlock them?

### Phase 4: Write the Review Document

Structure the output document as follows:

```markdown
# Design Review: {scope}

**Date:** {date}
**Scope:** {scope_path}
**Focus:** {focus or "All principles (1-24)"}
**Depth:** {depth}
**Files reviewed:** {count}

## Executive Summary

{2-4 sentence summary of overall alignment quality, top themes, and highest-priority improvements.}

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | Config internals exposed via public attrs |
| ... | ... | ... | ... | ... | ... |

## Detailed Findings

### Category: {category name}

#### P{N}. {Principle name} — Alignment: {score}/3

**Current state:**
{Description of how the code currently relates to this principle, with file:line references.}

**Findings:**
- {Finding 1 with specific file:line reference}
- {Finding 2 with specific file:line reference}

**Suggested improvement:**
{Concrete, actionable improvement. Reference specific code elements.}

**Effort:** {small|medium|large}
**Risk if unaddressed:** {low|medium|high}

---

{Repeat for each principle with alignment < 3}

## Cross-Cutting Themes

### {Theme 1 title}
{Description, root cause, affected principles, suggested approach.}

### {Theme 2 title}
...

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | ... | ... | small | ... |
| ... | ... | ... | ... | ... |

## Recommended Action Sequence

{Numbered list of recommended improvements in dependency order, grouped into phases if appropriate. Each item references the principle number and specific findings.}
```

### Phase 5: Save and Report

1. **Save the document** to `docs/reviews/`.
2. **Report the file path** and a one-paragraph summary to the user.

---

## Review Quality Requirements

1. **Every finding must reference specific code.** No vague statements like "coupling is high." Instead: "`src/semantics/compiler.py:234` directly accesses `_internal_cache` on `PlanCatalog`, bypassing its public interface."

2. **Suggestions must be actionable.** Not "reduce coupling" but "Extract `CompilationContext` protocol from `SemanticCompiler` and have `PlanCatalog` depend on the protocol instead of the concrete class."

3. **Don't invent problems.** If a principle is well-satisfied, score it 3 and move on. The review is useful only when it identifies real gaps.

4. **Respect YAGNI in the review itself.** Don't suggest improvements that introduce speculative generality. Improvements should reduce complexity or risk, not add abstraction for its own sake.

5. **Respect the existing architecture.** Suggestions should work within the project's established patterns (Hamilton DAG, DataFusion, rustworkx, msgspec contracts). Don't propose replacing foundational choices.

6. **Use the project's own tools.** Use `/cq` skills for evidence gathering. Use `/ast-grep` for structural pattern detection. Ground findings in tool output, not guesswork.

7. **Principles that are N/A get score 3.** If a principle doesn't meaningfully apply to the reviewed scope (e.g., idempotency for a pure utility module), score it 3 with a brief note.

---

## Error Handling

- **Scope path does not exist**: Report the error and stop.
- **Scope is too large for depth level**: Warn the user and suggest narrowing scope or reducing depth.
- **CQ tool unavailable**: Fall back to `Grep`, `Glob`, and `Read` for evidence gathering.
- **Focus specifies invalid principle numbers**: Report the invalid numbers, proceed with valid ones.
