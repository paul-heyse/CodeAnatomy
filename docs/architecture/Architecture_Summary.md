Below is an **architecture briefing** intended for an **LLM AI programming agent** who will make design changes to this codebase. It focuses on: what the system is meant to do, how the modules link together, the “inference‑driven” intent, and the technical invariants that must be preserved to keep the system robust, scalable, and easy to extend.

---

# Architecture briefing: Inference‑driven Python CPG builder (Hamilton + PyArrow Acero)

## 1) Functional intent in one paragraph

This system builds a **rich, best‑in‑class Code Property Graph (CPG) for Python** by extracting multiple evidence layers (LibCST, Python AST, symtable, bytecode/dis, SCIP, optional tree‑sitter, optional inspect), normalizing everything onto a **canonical coordinate system (byte spans)** and **stable IDs**, then compiling a set of **relationship rules** into **deterministic, contract‑validated PyArrow Acero plans**. The Hamilton DAG provides **inference‑driven orchestration**: you specify desired outputs (e.g., final CPG edges/nodes/props, debug datasets, run bundle), and Hamilton executes only the required upstream steps while enabling caching/materialization control.

---

## 2) The “big idea”: inference‑driven schemas + ambiguity‑tolerant joins

### What “inference‑driven” means here

* The pipeline is designed so you **do not have to fully specify intermediate schemas**.
* Intermediate datasets are treated as **evidence tables** whose schemas can evolve naturally as you add new extractors or enrichment columns.
* **Only key “contract boundaries” are strict**:

  1. relationship outputs (e.g., `rel_name_symbol`, `rel_callsite_symbol`)
  2. final CPG outputs (`cpg_nodes`, `cpg_edges`, `cpg_props`)
* Contracts enforce the minimum required columns and semantics. Everything else is optional enrichment.

### Ambiguity‑tolerance is a feature, not a bug

Many code intelligence problems are inherently ambiguous:

* multiple qualified-name candidates for the same name reference
* multiple potential call targets
* partial/incomplete indexes (SCIP missing, parse errors, etc.)

The architecture **never forces early resolution**. It:

* emits candidate relationships with scores/confidence
* stores `ambiguity_group_id` for grouping candidates
* applies deterministic winner selection only at the boundary where you need a “final” edge set (or chooses to keep multi‑edges, depending on output contract)

---

## 3) High-level pipeline overview

Think of it as four conceptual stages:

```
Extraction (tables) 
   -> Normalization (byte spans, stable IDs, join-ready shape)
      -> Relationship compilation (rules -> Acero plans -> relationship tables)
         -> CPG build (edge emission rules + node/prop building)
            -> Outputs + storage + observability
```

### Stage A — Extraction: “raw evidence”

Produces Arrow tables for each lens:

* `repo_files`: file inventory
* `cst_*`: LibCST-derived tables (callsites, imports, name refs, defs, etc.)
* `ast_*`: python ast-derived tables (optional but useful)
* `symtable_*`: compiler scope/binding facts
* `bytecode_*`: instructions, code units, raw def/use events (optional → best-in-class)
* `scip_*`: symbols, occurrences, docs, diagnostics

### Stage B — Normalization: “make tables joinable”

Normalizes disparate coordinate systems and establishes stable joins:

* convert line/col → byte offsets (`bstart`, `bend`)
* compute stable IDs (`*_id`) from anchor spans + kind
* infer/standardize schemas where useful (but keep it flexible)
* build dims (`dim_qualified_names`) and candidate datasets (`callsite_qname_candidates`)

### Stage C — Relationship rules → compiled Acero plans

A **RelationshipRegistry** contains rule objects like:

* interval-align joins (span containment / best match)
* hash joins (key-based)
* projections, filters, kernel-enrichment steps
  Each rule compiles into a PyArrow Acero plan (or a small chain of plans) and outputs a contract-validated dataset.

### Stage D — CPG build: deterministic emission

Relationship tables become final graph tables:

* **node emission**: create node rows with stable IDs and properties
* **edge emission**: emit semantic edges from relationship tables, apply preference rules:

  * prefer SCIP-resolved call edges, fallback to qualified-name candidates
  * derive reads/writes/imports from `symbol_roles` bits
* **props emission**: build property tables or embed props, depending on storage strategy

---

## 4) Directory / module map and responsibilities

### `src/codeintel_cpg/arrowdsl/`

**Purpose:** a small internal DSL for expressing relational transformations that compile to PyArrow Acero.

Key ideas:

* Represent queries as a small IR: scan → filter → project → join → group/aggregate → finalize
* Separate *what* from *how*: rules and kernels describe intent; runner executes them via Acero.
* Contracts constrain outputs without requiring intermediate schemas.

What to preserve:

* A clear separation between:

  * plan construction (pure, deterministic)
  * resolver selection (memory vs filesystem)
  * execution (side-effectful; materializes tables/record batches)
* Support “graceful empty table” behavior: if optional inputs missing, emit empty output with correct schema.

### `src/codeintel_cpg/extract/`

**Purpose:** source-specific extractors that turn code + indices into Arrow tables.

Important design constraints:

* Extractors should be **side-effect-free** (except reading files).
* They should emit **minimal required columns**, plus optional enrichment.
* They should not “solve” cross-source linking; that belongs in relationships.

### `src/codeintel_cpg/normalize/`

**Purpose:** the critical “join-readiness” layer.

Three invariants to protect:

1. **Byte spans are canonical.**
2. **Stable IDs are deterministic.**
3. Normalization should be **total**: if something fails (bad encoding, missing end offsets), produce a diagnostic row + best-effort outputs, not exceptions that kill the run.

### `src/codeintel_cpg/relspec/`

**Purpose:** the rule registry + compiler that produces relationship datasets.

The `relspec` package is the “semantic heart” of the system because it defines:

* what joins exist
* how ambiguity is preserved
* how dedupe/winner selection works
* what contracts must hold

Key objects:

* `RelationshipRule`: declarative description of a join/alignment and output columns
* `RelationshipRegistry`: holds all rules
* `RelationshipRuleCompiler`: turns rules into executable plans

### `src/codeintel_cpg/cpg/`

**Purpose:** final graph model emission.

It should be boring and deterministic:

* no complex join logic here (that belongs in relspec)
* it consumes relationship outputs and emits nodes/edges/props in a predictable way

Also contains:

* `kind_catalog.py` / `prop_catalog.py` — the **single source of truth** for CPG kind IDs and property typing.
* Symtable-derived view builders now live in `datafusion_engine/symtable_views.py` and are
  registered as DataFusion views; CPG consumes them via view references.

### `src/codeintel_cpg/hamilton/`

**Purpose:** inference-driven orchestration.

Hamilton is the “dependency planner”:

* a user asks for outputs, Hamilton pulls required upstream nodes automatically.
* caching decorators can prevent recomputation.
* “materializer nodes” are side-effects you request explicitly.

Hamilton modules should remain thin:

* they should mostly wire functions from packages above
* they should not embed business logic that belongs in relspec/cpg/normalize

### `src/codeintel_cpg/storage/`

**Purpose:** durable I/O for datasets and debug artifacts.

Two primary storage patterns:

* Parquet dataset dirs (preferred for filesystem resolver)
* IPC file/stream (fast for debug snapshots)

### `src/codeintel_cpg/obs/`

**Purpose:** reproducibility + provenance.

Key outputs:

* `manifest.json` (what was produced, schemas/fingerprints, dataset locations)
* `run bundle` directory snapshot containing:

  * manifest
  * config
  * registry/contracts snapshot
  * schema snapshots
  * environment/git info

This is crucial for LLM agents: it gives a stable view of “what happened” even after code changes.

### `src/codeintel_cpg/graph/`

**Purpose:** optional adapters (e.g., rustworkx) for graph operations downstream.

---

## 5) The two resolver modes (memory vs filesystem) and why it matters

### In-memory resolver

* Relationship plans execute against in-memory `pyarrow.Table` inputs.
* Best for small repos or rapid iteration.
* Risk: memory blowups for large repos.

### Filesystem resolver

* Normalized relationship input datasets are **persisted to Parquet dataset directories**.
* Relationship compilation/execution uses `pyarrow.dataset` scanning and Acero filtering/projection pushdown.
* Benefits:

  * lowers peak memory
  * improves reproducibility (inputs become stable artifacts)
  * enables caching/reuse and partial recomputation
  * makes “debug by inspecting datasets” easy

**Design callout:** Filesystem mode shifts the architecture toward an “analytical query engine” posture. It becomes feasible to run very large codebases if you avoid materializing huge join intermediates.

---

## 6) Contracts: where strictness belongs

### Contract boundaries

Contracts should be strict at:

* relationship outputs (`rel_*`)
* final graph outputs (`cpg_*`)
* any dataset written for external consumers

Everything else can be inferred/relaxed.

### Why contracts exist

* keep inference-driven flexibility while preventing silent breakage
* make LLM-driven refactors safe: contracts catch drift early

### Virtual fields (downstream-injected props)

A contract can declare “virtual fields” when a downstream stage injects required props.
Example: edge kinds may require `origin` or `resolution_method`, but you might not store them physically in the relationship output table.

**Important engineering constraint:** virtual fields must be explicit, not implicit.
This avoids the “it worked before, now it silently produces incomplete edges” failure mode.

---

## 7) Kind + property catalogs as the single source of truth

The catalog modules provide:

* stable node/edge kind identifiers (`cpg/kind_catalog.py`)
* required relation-output props per edge kind (for validation)
* canonical property keys and value types (`cpg/prop_catalog.py`)
* validation utilities tied to dataset contracts

**Functional intent:** every design change should be expressible as:

* adding/modifying edge kind requirements, and/or
* adding/modifying property catalog entries, and/or
* adding/modifying relationship rules and their output contracts.

That means an LLM agent can:

* reason about impact systematically
* update the registry first, then implement changes until validation passes

---

## 8) The “enforcement link”: relspec compiler validates contracts against edge kinds

We added a validator hook so that:

* whenever a relationship output dataset is mapped to edge kinds (e.g., `rel_name_symbol → PY_READS_SYMBOL/PY_WRITES_SYMBOL/...`)
* the relspec compiler checks that the dataset’s output contract contains all required props (either as columns or declared virtual fields)

**Why this matters:**

* It turns the kind catalog into an enforced interface contract.
* It prevents “relationship dataset drift” from quietly breaking edge emission.

**Key callout for refactors:**
If you add a new edge kind or change required props, you must update:

* `kind_catalog.py` edge requirements
* relationship output contracts (schema or virtual_fields)
* the dataset→edge mapping used by the validator (if new dataset feeds edges)

---

## 9) Key technical design callouts an LLM agent must respect

### 9.1 Canonical identity and joins

* Every join should be expressible using a small number of stable keys:

  * `(path, bstart, bend)` for interval alignment
  * IDs derived from those anchors (`*_id`)
  * `symbol` for SCIP identity
  * `qname` for qualified-name dims

**Anti-pattern:** joining on stringified AST dumps, reprs, or “best effort” text blobs.

### 9.2 Determinism

* The system must be deterministic given the same inputs:

  * stable IDs
  * stable sort before dedupe
  * stable winner selection policies
  * stable dataset naming and output layout

**Anti-pattern:** relying on hash iteration order, unordered dict traversal, or nondeterministic parallel traversal without stable ordering.

### 9.3 Ambiguity preservation

* Candidate edges must not be thrown away early.
* Prefer: candidate dataset + dedupe policy at finalize/emission.

**Anti-pattern:** “just pick the first match” inside an extractor.

### 9.4 Scalability boundaries

* In-memory tables are for small/medium repos.
* For large repos:

  * persist normalized datasets
  * scan from Parquet datasets
  * push filters/projections into scans
  * avoid materializing huge intermediates

### 9.5 Safety: runtime overlay is optional and must be sandboxed

* `inspect` overlay can be powerful but also dangerous in untrusted repos.
* Treat runtime overlay as:

  * off by default
  * safe introspection only (no eval/import execution unless explicitly enabled and sandboxed)
  * evidence-tagged, never treated as authoritative over symtable/SCIP

### 9.6 “Best-in-class” doesn’t mean “everything is required”

A maximally complete CPG is achieved by **layering**, not by forcing all sources to exist:

* SCIP may be missing → fall back to qualified names
* tree-sitter diagnostics may be missing → still build CPG
* bytecode DFG may be disabled → still build syntax/symbol graph

This is why contracts + empty-table defaults are important.

---

## 10) How an LLM agent should approach design changes

### A) Find the surface area impacted

Ask:

1. Is this a new fact source? (extract)
2. Is it a new normalization step? (normalize)
3. Is it a new relationship/join? (relspec)
4. Is it a new CPG representation? (cpg kinds + emission)
5. Is it operational? (storage/obs/hamilton)

### B) Update the registry first

For semantic changes:

* update kind requirements in `kind_catalog.py`
* update property catalog entries in `prop_catalog.py`
* run edge requirement validation against relation output schema

### C) Make the smallest schema changes possible

Prefer:

* adding optional columns (enrichment)
* adding a new relationship output dataset
* adding virtual fields to a contract if injected downstream

Avoid:

* breaking existing contract schemas unless versioned
* renaming columns without aliasing/migration path

### D) Preserve output determinism

* ensure new candidate datasets have a stable sort + dedupe policy
* ensure winner selection remains consistent across modes (memory/filesystem)

### E) Use observability to validate changes

After change:

* generate a run bundle
* check schema fingerprints and rule outputs
* diff manifests across runs

---

## 11) Quick “mental model” for debugging problems

When the graph output looks wrong, triage by stage:

1. **Extraction**: is the evidence present?

   * are callsites/name refs/imports extracted with correct spans?
   * is SCIP loaded and ranges normalized?

2. **Normalization**: do spans align?

   * do byte spans match across CST/SCIP?
   * are IDs stable and consistent?

3. **Relationship rules**:

   * are joins too strict/too loose?
   * are tie-breakers correct?
   * is ambiguity preserved?

4. **Edge emission**:

   * are symbol_roles interpreted correctly?
   * are fallback edges emitted only when intended?
   * are winners chosen consistently?

5. **Mode differences**:

   * in-memory vs filesystem resolver should produce the same results (modulo ordering/caching)
   * if not, check dataset write/read schema coercions and projection pushdown behavior

---

## 12) Summary: what to design around

If an LLM agent remembers only a few things:

1. **Byte spans + stable IDs are the backbone.**
2. **Relspec rules are the semantic engine; CPG build should be deterministic and thin.**
3. **Contracts are strict only at boundaries; intermediates are flexible.**
4. **Ambiguity is preserved; winner selection is explicit and deterministic.**
5. **Filesystem resolver mode is the scaling strategy; obs/run bundles make runs replayable and refactors safe.**
6. **The Ultimate kind registry is the source of truth; relspec compiler validation enforces it.**

---

If you want, I can turn this briefing into a **repo-level `ARCHITECTURE.md`** and a shorter **`LLM_AGENT_GUIDE.md`** that includes:

* “Where to make changes” map
* a checklist for adding new relationships/edge kinds
* common failure modes and how to debug them using run bundles and intermediate materializers
