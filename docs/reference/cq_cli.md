# cq

```console
cq COMMAND
```

Code Query - High-signal code analysis macros

## Table of Contents

- [`impact`](#cq-impact)
- [`calls`](#cq-calls)
- [`imports`](#cq-imports)
- [`exceptions`](#cq-exceptions)
- [`sig-impact`](#cq-sig-impact)
- [`side-effects`](#cq-side-effects)
- [`scopes`](#cq-scopes)
- [`bytecode-surface`](#cq-bytecode-surface)
- [`q`](#cq-q)
- [`search`](#cq-search)
- [`report`](#cq-report)
- [`run`](#cq-run)
- [`chain`](#cq-chain)
- [`neighborhood`](#cq-neighborhood)
- [`index`](#cq-index)
- [`cache`](#cq-cache)
- [`schema`](#cq-schema)
- [`ldmd`](#cq-ldmd)
    - [`index`](#cq-ldmd-index)
    - [`get`](#cq-ldmd-get)
    - [`search`](#cq-ldmd-search)
    - [`neighbors`](#cq-ldmd-neighbors)
- [`artifact`](#cq-artifact)
    - [`list`](#cq-artifact-list)
    - [`get`](#cq-artifact-get)
- [`repl`](#cq-repl)
- [`--install-completion`](#cq-install-completion)

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level; repeat flag for higher verbosity  *[env: CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG]*  *[default: True]*

**Analysis**:

* [`bytecode-surface`](#cq-bytecode-surface): Analyze bytecode for hidden dependencies.
* [`calls`](#cq-calls): Census all call sites for a function.
* [`chain`](#cq-chain): Execute chained CQ commands via the shared run engine.
* [`exceptions`](#cq-exceptions): Analyze exception handling patterns.
* [`impact`](#cq-impact): Trace data flow from a function parameter.
* [`imports`](#cq-imports): Analyze import structure and cycles.
* [`neighborhood`](#cq-neighborhood): Analyze semantic neighborhood of a target symbol or location.
* [`q`](#cq-q): Run a declarative code query using ast-grep.
* [`report`](#cq-report): Run target-scoped report bundles.
* [`run`](#cq-run): Execute a multi-step CQ run plan.
* [`scopes`](#cq-scopes): Analyze scope capture (closures).
* [`search`](#cq-search): Search for code patterns with semantic enrichment.
* [`side-effects`](#cq-side-effects): Detect import-time side effects.
* [`sig-impact`](#cq-sig-impact): Analyze impact of a signature change.

**Administration**:

* [`cache`](#cq-cache): Show deprecation notice for removed cache management command.
* [`index`](#cq-index): Show deprecation notice for removed index management command.
* [`schema`](#cq-schema): Export JSON schema payloads for CQ contracts.

**Protocols**:

* [`artifact`](#cq-artifact): Retrieve cache-backed CQ artifacts
* [`ldmd`](#cq-ldmd): LDMD progressive disclosure protocol

**Setup**:

* [`repl`](#cq-repl): Launch an interactive CQ shell session.
* [`--install-completion`](#cq-install-completion): Install CQ shell completion scripts.

## cq impact

```console
cq impact --param STR [OPTIONS] FUNCTION
```

Trace data flow from a function parameter.

**Parameters**:

* `FUNCTION, --function`: Function name to analyze  **[required]**
* `--param`: Parameter name to trace  **[required]**
* `--depth`: Maximum call depth  *[default: 5]*

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level; repeat flag for higher verbosity  *[env: CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG]*  *[default: True]*

**Filters**:

* `--include`: Include files matching pattern (glob or ~regex, repeatable)  *[default: []]*
* `--exclude`: Exclude files matching pattern (glob or ~regex, repeatable)  *[default: []]*
* `--impact`: Filter by impact bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[default: []]*
* `--confidence`: Filter by confidence bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[default: []]*
* `--severity`: Filter by severity (comma-separated: error,warning,info)  *[choices: info, warning, error]*  *[default: []]*
* `--limit`: Maximum number of findings  *[default: None]*

## cq calls

```console
cq calls [OPTIONS] FUNCTION
```

Census all call sites for a function.

**Parameters**:

* `FUNCTION, --function`: Function name to find calls for  **[required]**

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level; repeat flag for higher verbosity  *[env: CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG]*  *[default: True]*

**Filters**:

* `--include`: Include files matching pattern (glob or ~regex, repeatable)  *[default: []]*
* `--exclude`: Exclude files matching pattern (glob or ~regex, repeatable)  *[default: []]*
* `--impact`: Filter by impact bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[default: []]*
* `--confidence`: Filter by confidence bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[default: []]*
* `--severity`: Filter by severity (comma-separated: error,warning,info)  *[choices: info, warning, error]*  *[default: []]*
* `--limit`: Maximum number of findings  *[default: None]*

## cq imports

```console
cq imports [OPTIONS]
```

Analyze import structure and cycles.

**Parameters**:

* `--cycles, --no-cycles`: Run cycle detection  *[default: False]*
* `--module`: Focus on specific module  *[default: None]*

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level; repeat flag for higher verbosity  *[env: CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG]*  *[default: True]*

**Filters**:

* `--include`: Include files matching pattern (glob or ~regex, repeatable)  *[default: []]*
* `--exclude`: Exclude files matching pattern (glob or ~regex, repeatable)  *[default: []]*
* `--impact`: Filter by impact bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[default: []]*
* `--confidence`: Filter by confidence bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[default: []]*
* `--severity`: Filter by severity (comma-separated: error,warning,info)  *[choices: info, warning, error]*  *[default: []]*
* `--limit`: Maximum number of findings  *[default: None]*

## cq exceptions

```console
cq exceptions [OPTIONS]
```

Analyze exception handling patterns.

**Parameters**:

* `--function`: Focus on specific function  *[default: None]*

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level; repeat flag for higher verbosity  *[env: CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG]*  *[default: True]*

**Filters**:

* `--include`: Include files matching pattern (glob or ~regex, repeatable)  *[default: []]*
* `--exclude`: Exclude files matching pattern (glob or ~regex, repeatable)  *[default: []]*
* `--impact`: Filter by impact bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[default: []]*
* `--confidence`: Filter by confidence bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[default: []]*
* `--severity`: Filter by severity (comma-separated: error,warning,info)  *[choices: info, warning, error]*  *[default: []]*
* `--limit`: Maximum number of findings  *[default: None]*

## cq sig-impact

```console
cq sig-impact --to STR [OPTIONS] SYMBOL
```

Analyze impact of a signature change.

**Parameters**:

* `SYMBOL, --symbol`: Function name to analyze  **[required]**
* `--to`: New signature (e.g., "foo(a, b, *, c=None)")  **[required]**

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level; repeat flag for higher verbosity  *[env: CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG]*  *[default: True]*

**Filters**:

* `--include`: Include files matching pattern (glob or ~regex, repeatable)  *[default: []]*
* `--exclude`: Exclude files matching pattern (glob or ~regex, repeatable)  *[default: []]*
* `--impact`: Filter by impact bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[default: []]*
* `--confidence`: Filter by confidence bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[default: []]*
* `--severity`: Filter by severity (comma-separated: error,warning,info)  *[choices: info, warning, error]*  *[default: []]*
* `--limit`: Maximum number of findings  *[default: None]*

## cq side-effects

```console
cq side-effects [OPTIONS]
```

Detect import-time side effects.

**Parameters**:

* `--max-files`: Maximum files to scan  *[default: 2000]*

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level; repeat flag for higher verbosity  *[env: CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG]*  *[default: True]*

**Filters**:

* `--include`: Include files matching pattern (glob or ~regex, repeatable)  *[default: []]*
* `--exclude`: Exclude files matching pattern (glob or ~regex, repeatable)  *[default: []]*
* `--impact`: Filter by impact bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[default: []]*
* `--confidence`: Filter by confidence bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[default: []]*
* `--severity`: Filter by severity (comma-separated: error,warning,info)  *[choices: info, warning, error]*  *[default: []]*
* `--limit`: Maximum number of findings  *[default: None]*

## cq scopes

```console
cq scopes [OPTIONS] TARGET
```

Analyze scope capture (closures).

**Parameters**:

* `TARGET, --target`: File path or symbol name to analyze  **[required]**

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level; repeat flag for higher verbosity  *[env: CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG]*  *[default: True]*

**Filters**:

* `--include`: Include files matching pattern (glob or ~regex, repeatable)  *[default: []]*
* `--exclude`: Exclude files matching pattern (glob or ~regex, repeatable)  *[default: []]*
* `--impact`: Filter by impact bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[default: []]*
* `--confidence`: Filter by confidence bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[default: []]*
* `--severity`: Filter by severity (comma-separated: error,warning,info)  *[choices: info, warning, error]*  *[default: []]*
* `--limit`: Maximum number of findings  *[default: None]*

## cq bytecode-surface

```console
cq bytecode-surface [OPTIONS] TARGET
```

Analyze bytecode for hidden dependencies.

**Parameters**:

* `TARGET, --target`: File path or symbol name to analyze  **[required]**
* `--show`: What to show: globals,attrs,constants,opcodes  *[default: globals,attrs,constants]*

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level; repeat flag for higher verbosity  *[env: CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG]*  *[default: True]*

**Filters**:

* `--include`: Include files matching pattern (glob or ~regex, repeatable)  *[default: []]*
* `--exclude`: Exclude files matching pattern (glob or ~regex, repeatable)  *[default: []]*
* `--impact`: Filter by impact bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[default: []]*
* `--confidence`: Filter by confidence bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[default: []]*
* `--severity`: Filter by severity (comma-separated: error,warning,info)  *[choices: info, warning, error]*  *[default: []]*
* `--limit`: Maximum number of findings  *[default: None]*

## cq q

```console
cq q [OPTIONS] QUERY-STRING
```

Run a declarative code query using ast-grep.

**Parameters**:

* `QUERY-STRING, --query-string`: Query string (e.g., "entity=function name=foo")  **[required]**
* `--explain-files, --no-explain-files`: Include file filtering diagnostics  *[default: False]*

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level; repeat flag for higher verbosity  *[env: CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG]*  *[default: True]*

**Filters**:

* `--include`: Include files matching pattern (glob or ~regex, repeatable)  *[default: []]*
* `--exclude`: Exclude files matching pattern (glob or ~regex, repeatable)  *[default: []]*
* `--impact`: Filter by impact bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[default: []]*
* `--confidence`: Filter by confidence bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[default: []]*
* `--severity`: Filter by severity (comma-separated: error,warning,info)  *[choices: info, warning, error]*  *[default: []]*
* `--limit`: Maximum number of findings  *[default: None]*

## cq search

```console
cq search [OPTIONS] QUERY
```

Search for code patterns with semantic enrichment.

**Parameters**:

* `QUERY, --query`: Search query  **[required]**
* `--include-strings, --no-include-strings`: Include matches in strings/comments/docstrings  *[default: False]*
* `--with-neighborhood, --no-with-neighborhood`: Include structural neighborhood preview (slower)  *[default: False]*
* `--in`: Restrict to directory  *[default: None]*
* `--lang`: Search language scope (auto, python, rust)  *[choices: auto, python, rust]*  *[default: auto]*

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level; repeat flag for higher verbosity  *[env: CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG]*  *[default: True]*

**Filters**:

* `--include`: Include files matching pattern (glob or ~regex, repeatable)  *[default: []]*
* `--exclude`: Exclude files matching pattern (glob or ~regex, repeatable)  *[default: []]*
* `--impact`: Filter by impact bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[default: []]*
* `--confidence`: Filter by confidence bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[default: []]*
* `--severity`: Filter by severity (comma-separated: error,warning,info)  *[choices: info, warning, error]*  *[default: []]*
* `--limit`: Maximum number of findings  *[default: None]*

**Search Mode**:

* `--regex, --no-regex`: Treat query as regex  *[default: False]*
* `--literal, --no-literal`: Treat query as literal  *[default: False]*

## cq report

```console
cq report --target STR [OPTIONS] PRESET
```

Run target-scoped report bundles.

**Parameters**:

* `PRESET, --preset`: Report preset (refactor-impact, safety-reliability, change-propagation, dependency-health)  **[required]**  *[choices: refactor-impact, safety-reliability, change-propagation, dependency-health]*
* `--target`: Target spec (function:foo, class:Bar, module:pkg.mod, path:src/...)  **[required]**
* `--in`: Restrict analysis to a directory  *[default: None]*
* `--param`: Parameter name for impact analysis  *[default: None]*
* `--to`: Proposed signature for sig-impact analysis  *[default: None]*
* `--bytecode-show`: Bytecode surface fields  *[default: None]*

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level; repeat flag for higher verbosity  *[env: CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG]*  *[default: True]*

**Filters**:

* `--include`: Include files matching pattern (glob or ~regex, repeatable)  *[default: []]*
* `--exclude`: Exclude files matching pattern (glob or ~regex, repeatable)  *[default: []]*
* `--impact`: Filter by impact bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[default: []]*
* `--confidence`: Filter by confidence bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[default: []]*
* `--severity`: Filter by severity (comma-separated: error,warning,info)  *[choices: info, warning, error]*  *[default: []]*
* `--limit`: Maximum number of findings  *[default: None]*

## cq run

```console
cq run [OPTIONS]
```

Execute a multi-step CQ run plan.

**Parameters**:

* `--stop-on-error, --no-stop-on-error`: Stop execution on the first step error  *[default: False]*

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level; repeat flag for higher verbosity  *[env: CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG]*  *[default: True]*

**Filters**:

* `--include`: Include files matching pattern (glob or ~regex, repeatable)  *[default: []]*
* `--exclude`: Exclude files matching pattern (glob or ~regex, repeatable)  *[default: []]*
* `--impact`: Filter by impact bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[default: []]*
* `--confidence`: Filter by confidence bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[default: []]*
* `--severity`: Filter by severity (comma-separated: error,warning,info)  *[choices: info, warning, error]*  *[default: []]*
* `--limit`: Maximum number of findings  *[default: None]*

**Run Input**:

* `--plan`: Path to a run plan TOML file  *[default: None]*
* `--step`: Repeatable JSON step object (e.g., '{"type":"q","query":"..."}')  *[default: []]*
* `--steps`: JSON array of steps (e.g., '[{"type":"q",...},{"type":"calls",...}]')  *[default: []]*

## cq chain

```console
cq chain [OPTIONS]
```

Execute chained CQ commands via the shared run engine.

**Parameters**:

* `--delimiter`: Token used to split command segments.  *[default: AND]*

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level; repeat flag for higher verbosity  *[env: CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG]*  *[default: True]*

## cq neighborhood

```console
cq neighborhood [OPTIONS] TARGET
```

Analyze semantic neighborhood of a target symbol or location.

**Parameters**:

* `TARGET, --target`: Target location (file:line[:col] or symbol)  **[required]**
* `--lang`: Query language (python, rust)  *[choices: python, rust]*  *[default: python]*
* `--top-k`: Max items per slice  *[default: 10]*
* `--semantic-enrichment, --no-semantic-enrichment`: Enable semantic enrichment  *[default: True]*

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level; repeat flag for higher verbosity  *[env: CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG]*  *[default: True]*

## cq index

```console
cq index
```

Show deprecation notice for removed index management command.

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level; repeat flag for higher verbosity  *[env: CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG]*  *[default: True]*

## cq cache

```console
cq cache
```

Show deprecation notice for removed cache management command.

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level; repeat flag for higher verbosity  *[env: CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG]*  *[default: True]*

## cq schema

```console
cq schema [OPTIONS]
```

Export JSON schema payloads for CQ contracts.

**Parameters**:

* `--kind`: Schema export kind  *[choices: result, query, components]*  *[default: result]*

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level; repeat flag for higher verbosity  *[env: CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG]*  *[default: True]*

## cq ldmd

LDMD progressive disclosure protocol

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level; repeat flag for higher verbosity  *[env: CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG]*  *[default: True]*

### cq ldmd index

```console
cq ldmd index PATH
```

Index an LDMD document and return section metadata.

**Parameters**:

* `PATH, --path`: Path to LDMD document  **[required]**

### cq ldmd get

```console
cq ldmd get --id STR [OPTIONS] PATH
```

Extract content from a section by ID.

**Parameters**:

* `PATH, --path`: Path to LDMD document  **[required]**
* `--id`: Section ID to extract.  **[required]**
* `--mode`: Extraction mode: full, preview, or tldr  *[choices: full, preview, tldr]*  *[default: full]*
* `--depth`: Include nested sections up to this depth (0 = no nesting).  *[default: 0]*
* `--limit-bytes`: Max bytes to return (0 = unlimited).  *[default: 0]*

### cq ldmd search

```console
cq ldmd search --query STR PATH
```

Search within LDMD sections.

**Parameters**:

* `PATH, --path`: Path to LDMD document  **[required]**
* `--query`: Search query string.  **[required]**

### cq ldmd neighbors

```console
cq ldmd neighbors --id STR PATH
```

Get neighboring sections for navigation.

**Parameters**:

* `PATH, --path`: Path to LDMD document  **[required]**
* `--id`: Section ID to get neighbors for.  **[required]**

## cq artifact

Retrieve cache-backed CQ artifacts

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level; repeat flag for higher verbosity  *[env: CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG]*  *[default: True]*

### cq artifact list

```console
cq artifact list [OPTIONS]
```

List cached search artifact bundles.

**Parameters**:

* `--run-id`: Filter entries to a run id
* `--limit`: Max rows to return  *[default: 20]*

### cq artifact get

```console
cq artifact get --run-id STR [OPTIONS]
```

Fetch a cached search artifact payload by run id.

**Parameters**:

* `--run-id`: Run id to retrieve  **[required]**
* `--kind`: Artifact payload kind  *[choices: search_bundle, summary, object_summaries, occurrences, snippets, diagnostics]*  *[default: search_bundle]*

## cq repl

```console
cq repl
```

Launch an interactive CQ shell session.

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level; repeat flag for higher verbosity  *[env: CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG]*  *[default: True]*

## cq --install-completion

```console
cq --install-completion [OPTIONS]
```

Install CQ shell completion scripts.

**Parameters**:

* `--shell`: Shell type for completion. If not specified, attempts to auto-detect current shell.  *[choices: zsh, bash, fish]*  *[default: None]*
* `--output, -o`: Output path for the completion script. If not specified, uses shell-specific default.  *[default: None]*

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level; repeat flag for higher verbosity  *[env: CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG]*  *[default: True]*
