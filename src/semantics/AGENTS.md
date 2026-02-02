# src/semantics/AGENTS.md

Semantic compiler and span canonicalization rules.

## Canonical Span Policy

**`bstart` and `bend` are byte offsets**, not line/column pairs.

All normalization layers must preserve byte-span semantics:
- Join keys based on spans use exact byte ranges
- No line/column conversion without explicit policy

## Data Flow

```
Extraction → Input Registry → SemanticCompiler → View Graph → Hamilton DAG → CPG Outputs
```

## Key Modules

| Module | Purpose |
|--------|---------|
| `compiler.py` | Core transformation rules (10 compiler rules) |
| `pipeline.py` | `build_cpg()` entry point |
| `input_registry.py` | Maps extraction → semantic inputs |
| `spec_registry.py` | Normalization + relationship specs |
| `naming.py` | Output naming (`_v1` suffix convention) |
| `catalog/` | View metadata and fingerprints |
| `joins/` | Schema-driven join inference |

## Naming Convention

All semantic outputs use `_v1` suffix for versioning:

```python
# naming.py controls this
output_name = f"{base_name}_v1"
```

## Compiler Rules

The `SemanticCompiler` in `compiler.py` enforces 10 transformation rules:
1. Normalize → relate → union ordering
2. Schema validation at boundaries
3. Byte-span preservation
4. Deterministic plan generation
5. ... (see `compiler.py` for full list)

## Configuration

- `SemanticConfig` in `config.py` - Pipeline behavior
- `CpgBuildOptions` in `pipeline.py` - Build-time options

## Reference Docs

- `docs/architecture/semantic_pipeline_graph.md` (auto-generated)
