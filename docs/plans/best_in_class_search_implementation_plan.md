# Best-in-class Python search: implementation plan + minimal ast-grep pack

## Scope and decisions
- Language: Python only.
- Baseline tooling: ripgrep for candidate narrowing, ast-grep for structural facts.
- Optional enrichment: system Python (ast, symtable, dis) for incremental and higher-confidence joins.
- Goal: low latency (fast warm queries, acceptable cold starts) and deterministic output.

## Target UX (single entry point)
- Command shape: `cq q "<query string>"` (same as existing plan).
- Output: Markdown summary plus a stable JSON artifact for caching and debugging.
- Explain mode: emit the exact tool plan (rg/ast-grep/python) and why.

## Architecture overview (tool-native, Python-only)
1) Parse query -> normalized IR.
2) Narrow file set with ripgrep (fast file list, no JSON for this step).
3) Extract facts with ast-grep scan (JSON stream, on-disk ruleset, include metadata).
4) Build tables and resolve symbols (fully qualified names, import aliasing, class scopes).
5) Optional enrichment via system Python (symtable and bytecode) for the small result set.
6) Render Markdown + JSON artifact.

Key requirement: avoid inline rules on the command line for production use. Use a versioned ruleset on disk and pass `--config` so results are deterministic and testable.

## Implementation plan (phased)

### Phase 0: Scaffolding and ruleset pack
- Add a dedicated ast-grep project under `tools/cq/astgrep/` to avoid repo-root coupling.
- Files:
  - `tools/cq/astgrep/sgconfig.yml`
  - `tools/cq/astgrep/rules/python_facts/*.yml`
  - `tools/cq/astgrep/rule-tests/python_facts/*.yml`
- Ensure tests are runnable with:
  - `ast-grep test -c tools/cq/astgrep/sgconfig.yml`

### Phase 1: Baseline query pipeline (fast path)
- Implement a query planner that produces:
  - `rg` command for file narrowing
  - `ast-grep scan` command for structural facts
- Requirements:
  - Chunk large file lists to avoid argv limits.
  - Always include `--json=stream --include-metadata` in ast-grep scan.
  - Normalize ast-grep 0-based ranges to 1-based for human output.

### Phase 2: Symbol resolution and hazards
- Build symbol keys as `module_path:qualname`.
- Resolve imports (relative, alias, from-import) and mark uncertainty.
- Track hazards with explicit reasons:
  - dynamic dispatch
  - getattr or __getattr__
  - *args/**kwargs forwarding
  - unresolved import aliasing
- Assign confidence per edge (e.g., exact name + local def = high, alias chain = medium).

### Phase 3: Incremental indexing (latency optimization)
- Add a small SQLite index under `.cq/index.sqlite` or `.cq/index/`.
- Store per-file hash + mtime + rule version.
- Update only changed files, reuse cached facts for unchanged files.
- Add a `cq index` subcommand to pre-warm or update the index.

### Phase 4: System Python enrichment (optional)
- Use system Python for:
  - symtable scope signals (free/global/nonlocal)
  - bytecode surfaces (load globals/attrs, opcode stats)
- Scope enrichment to the final result set to keep latency low.
- Allow override via `CQ_SYSTEM_PYTHON=/path/to/python`.

## Minimal ruleset pack (Python facts)

### sgconfig.yml (scoped to tools/cq/astgrep)
```yaml
ruleDirs:
  - tools/cq/astgrep/rules/python_facts

testConfigs:
  - testDir: tools/cq/astgrep/rule-tests/python_facts

utilDirs:
  - tools/cq/astgrep/utils
```

### Rule files
All rules emit `metadata.record` so the JSON stream can be treated as a fact table.

1) `tools/cq/astgrep/rules/python_facts/py_def_function.yml`
```yaml
id: py_def_function
message: "python def"
severity: info
language: Python
metadata: { record: def, kind: function }
rule:
  pattern: def $F($$$ARGS): $$$BODY
```

2) `tools/cq/astgrep/rules/python_facts/py_def_async_function.yml`
```yaml
id: py_def_async_function
message: "python async def"
severity: info
language: Python
metadata: { record: def, kind: async_function }
rule:
  pattern: async def $F($$$ARGS): $$$BODY
```

3) `tools/cq/astgrep/rules/python_facts/py_def_function_typeparams.yml`
```yaml
id: py_def_function_typeparams
message: "python def with type parameters"
severity: info
language: Python
metadata: { record: def, kind: function_typeparams }
rule:
  pattern: def $F[$$$TP]($$$ARGS): $$$BODY
```

4) `tools/cq/astgrep/rules/python_facts/py_def_class.yml`
```yaml
id: py_def_class
message: "python class"
severity: info
language: Python
metadata: { record: def, kind: class }
rule:
  pattern: class $C: $$$BODY
```

5) `tools/cq/astgrep/rules/python_facts/py_def_class_bases.yml`
```yaml
id: py_def_class_bases
message: "python class with bases"
severity: info
language: Python
metadata: { record: def, kind: class_bases }
rule:
  pattern: class $C($$$BASES): $$$BODY
```

6) `tools/cq/astgrep/rules/python_facts/py_def_class_typeparams.yml`
```yaml
id: py_def_class_typeparams
message: "python class with type parameters"
severity: info
language: Python
metadata: { record: def, kind: class_typeparams }
rule:
  pattern: class $C[$$$TP]: $$$BODY
```

7) `tools/cq/astgrep/rules/python_facts/py_def_class_typeparams_bases.yml`
```yaml
id: py_def_class_typeparams_bases
message: "python class with type parameters and bases"
severity: info
language: Python
metadata: { record: def, kind: class_typeparams_bases }
rule:
  pattern: class $C[$$$TP]($$$BASES): $$$BODY
```

8) `tools/cq/astgrep/rules/python_facts/py_call_name.yml`
```yaml
id: py_call_name
message: "python name call"
severity: info
language: Python
metadata: { record: call, kind: name_call }
rule:
  pattern: $F($$$ARGS)
```

9) `tools/cq/astgrep/rules/python_facts/py_call_attr.yml`
```yaml
id: py_call_attr
message: "python attribute call"
severity: info
language: Python
metadata: { record: call, kind: attr_call }
rule:
  pattern: $OBJ.$M($$$ARGS)
```

10) `tools/cq/astgrep/rules/python_facts/py_import.yml`
```yaml
id: py_import
message: "python import"
severity: info
language: Python
metadata: { record: import, kind: import }
rule:
  pattern: import $MOD
```

11) `tools/cq/astgrep/rules/python_facts/py_import_as.yml`
```yaml
id: py_import_as
message: "python import as"
severity: info
language: Python
metadata: { record: import, kind: import_as }
rule:
  pattern: import $MOD as $ALIAS
```

12) `tools/cq/astgrep/rules/python_facts/py_from_import.yml`
```yaml
id: py_from_import
message: "python from import"
severity: info
language: Python
metadata: { record: import, kind: from_import }
rule:
  pattern: from $MOD import $NAME
```

13) `tools/cq/astgrep/rules/python_facts/py_from_import_as.yml`
```yaml
id: py_from_import_as
message: "python from import as"
severity: info
language: Python
metadata: { record: import, kind: from_import_as }
rule:
  pattern: from $MOD import $NAME as $ALIAS
```

14) `tools/cq/astgrep/rules/python_facts/py_from_import_multi.yml`
```yaml
id: py_from_import_multi
message: "python from import multi"
severity: info
language: Python
metadata: { record: import, kind: from_import_multi }
rule:
  pattern: from $MOD import $NAME, $$$NAMES
```

15) `tools/cq/astgrep/rules/python_facts/py_from_import_paren.yml`
```yaml
id: py_from_import_paren
message: "python from import paren"
severity: info
language: Python
metadata: { record: import, kind: from_import_paren }
rule:
  pattern: from $MOD import ($$$NAMES)
```

16) `tools/cq/astgrep/rules/python_facts/py_raise.yml`
```yaml
id: py_raise
message: "python raise"
severity: info
language: Python
metadata: { record: raise, kind: raise_expr }
rule:
  pattern: raise $E
```

17) `tools/cq/astgrep/rules/python_facts/py_raise_from.yml`
```yaml
id: py_raise_from
message: "python raise from"
severity: info
language: Python
metadata: { record: raise, kind: raise_from }
rule:
  pattern: raise $E from $CAUSE
```

18) `tools/cq/astgrep/rules/python_facts/py_raise_bare.yml`
```yaml
id: py_raise_bare
message: "python raise bare"
severity: info
language: Python
metadata: { record: raise, kind: raise_bare }
rule:
  pattern: raise
```

19) `tools/cq/astgrep/rules/python_facts/py_except.yml`
```yaml
id: py_except
message: "python except"
severity: info
language: Python
metadata: { record: except, kind: except_expr }
rule:
  pattern: except $E: $$$BODY
```

20) `tools/cq/astgrep/rules/python_facts/py_except_as.yml`
```yaml
id: py_except_as
message: "python except as"
severity: info
language: Python
metadata: { record: except, kind: except_as }
rule:
  pattern: except $E as $AS: $$$BODY
```

21) `tools/cq/astgrep/rules/python_facts/py_except_bare.yml`
```yaml
id: py_except_bare
message: "python except bare"
severity: info
language: Python
metadata: { record: except, kind: except_bare }
rule:
  pattern: except: $$$BODY
```

22) `tools/cq/astgrep/rules/python_facts/py_ctor_assign_name.yml`
```yaml
id: py_ctor_assign_name
message: "python ctor assign name"
severity: info
language: Python
metadata: { record: assign_ctor, kind: name_ctor }
rule:
  pattern: $V = $C($$$ARGS)
```

23) `tools/cq/astgrep/rules/python_facts/py_ctor_assign_attr.yml`
```yaml
id: py_ctor_assign_attr
message: "python ctor assign attr"
severity: info
language: Python
metadata: { record: assign_ctor, kind: attr_ctor }
rule:
  pattern: $V = $MOD.$C($$$ARGS)
```

## Minimal test pack (ast-grep tests)
Format is the standard `id/valid/invalid` YAML. One test file per rule.

Example tests:

1) `tools/cq/astgrep/rule-tests/python_facts/py_def_function_test.yml`
```yaml
id: py_def_function
valid:
  - "def foo(x):\n    return x"
invalid:
  - "class Foo:\n    pass"
```

2) `tools/cq/astgrep/rule-tests/python_facts/py_def_async_function_test.yml`
```yaml
id: py_def_async_function
valid:
  - "async def foo(x):\n    return x"
invalid:
  - "def foo(x):\n    return x"
```

3) `tools/cq/astgrep/rule-tests/python_facts/py_def_function_typeparams_test.yml`
```yaml
id: py_def_function_typeparams
valid:
  - "def foo[T](x: T) -> T:\n    return x"
invalid:
  - "def foo(x):\n    return x"
```

4) `tools/cq/astgrep/rule-tests/python_facts/py_def_class_test.yml`
```yaml
id: py_def_class
valid:
  - "class Foo:\n    pass"
invalid:
  - "def foo():\n    pass"
```

5) `tools/cq/astgrep/rule-tests/python_facts/py_def_class_bases_test.yml`
```yaml
id: py_def_class_bases
valid:
  - "class Foo(Bar, Baz):\n    pass"
invalid:
  - "class Foo:\n    pass"
```

6) `tools/cq/astgrep/rule-tests/python_facts/py_def_class_typeparams_test.yml`
```yaml
id: py_def_class_typeparams
valid:
  - "class Box[T]:\n    pass"
invalid:
  - "class Box:\n    pass"
```

7) `tools/cq/astgrep/rule-tests/python_facts/py_def_class_typeparams_bases_test.yml`
```yaml
id: py_def_class_typeparams_bases
valid:
  - "class Box[T](Base):\n    pass"
invalid:
  - "class Box[T]:\n    pass"
```

8) `tools/cq/astgrep/rule-tests/python_facts/py_call_name_test.yml`
```yaml
id: py_call_name
valid:
  - "foo(1, 2)"
invalid:
  - "obj.foo"
```

9) `tools/cq/astgrep/rule-tests/python_facts/py_call_attr_test.yml`
```yaml
id: py_call_attr
valid:
  - "obj.foo(1)"
invalid:
  - "foo(1)"
```

10) `tools/cq/astgrep/rule-tests/python_facts/py_import_test.yml`
```yaml
id: py_import
valid:
  - "import os"
invalid:
  - "from os import path"
```

11) `tools/cq/astgrep/rule-tests/python_facts/py_import_as_test.yml`
```yaml
id: py_import_as
valid:
  - "import os as os_mod"
invalid:
  - "import os"
```

12) `tools/cq/astgrep/rule-tests/python_facts/py_from_import_test.yml`
```yaml
id: py_from_import
valid:
  - "from os import path"
invalid:
  - "import os"
```

13) `tools/cq/astgrep/rule-tests/python_facts/py_from_import_as_test.yml`
```yaml
id: py_from_import_as
valid:
  - "from os import path as osp"
invalid:
  - "from os import path"
```

14) `tools/cq/astgrep/rule-tests/python_facts/py_from_import_multi_test.yml`
```yaml
id: py_from_import_multi
valid:
  - "from os import path, sep"
invalid:
  - "from os import path"
```

15) `tools/cq/astgrep/rule-tests/python_facts/py_from_import_paren_test.yml`
```yaml
id: py_from_import_paren
valid:
  - "from os import (path, sep)"
invalid:
  - "from os import path"
```

16) `tools/cq/astgrep/rule-tests/python_facts/py_raise_test.yml`
```yaml
id: py_raise
valid:
  - "raise ValueError(\"bad\")"
invalid:
  - "raise"
```

17) `tools/cq/astgrep/rule-tests/python_facts/py_raise_from_test.yml`
```yaml
id: py_raise_from
valid:
  - "raise ValueError(\"bad\") from err"
invalid:
  - "raise ValueError(\"bad\")"
```

18) `tools/cq/astgrep/rule-tests/python_facts/py_raise_bare_test.yml`
```yaml
id: py_raise_bare
valid:
  - "raise"
invalid:
  - "raise ValueError(\"bad\")"
```

19) `tools/cq/astgrep/rule-tests/python_facts/py_except_test.yml`
```yaml
id: py_except
valid:
  - "try:\n    pass\nexcept ValueError:\n    pass"
invalid:
  - "try:\n    pass\nexcept:\n    pass"
```

20) `tools/cq/astgrep/rule-tests/python_facts/py_except_as_test.yml`
```yaml
id: py_except_as
valid:
  - "try:\n    pass\nexcept ValueError as err:\n    pass"
invalid:
  - "try:\n    pass\nexcept ValueError:\n    pass"
```

21) `tools/cq/astgrep/rule-tests/python_facts/py_except_bare_test.yml`
```yaml
id: py_except_bare
valid:
  - "try:\n    pass\nexcept:\n    pass"
invalid:
  - "try:\n    pass\nexcept ValueError:\n    pass"
```

22) `tools/cq/astgrep/rule-tests/python_facts/py_ctor_assign_name_test.yml`
```yaml
id: py_ctor_assign_name
valid:
  - "x = Foo(1)"
invalid:
  - "x = foo"
```

23) `tools/cq/astgrep/rule-tests/python_facts/py_ctor_assign_attr_test.yml`
```yaml
id: py_ctor_assign_attr
valid:
  - "x = mod.Foo(1)"
invalid:
  - "x = Foo(1)"
```

## Notes and safeguards
- ast-grep JSON ranges are 0-based; normalize to 1-based for output.
- Always keep the ruleset versioned; include a `ruleset_version` in your JSON artifact.
- Run ast-grep tests in CI for these rules before relying on them in production.
- Add a small compatibility check at startup: verify `ast-grep --version` and that `--json=stream` is supported.

