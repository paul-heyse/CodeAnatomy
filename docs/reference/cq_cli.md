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

* `--root`: Repository root  *[env: CQ_ROOT, CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level  *[env: CQ_VERBOSE, CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT, CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR, CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT, CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG, CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG, CQ_USE_CONFIG]*  *[default: True]*

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

* [`cache`](#cq-cache): Handle deprecated cache management flags.
* [`index`](#cq-index): Handle deprecated index management flags.
* [`schema`](#cq-schema): Emit msgspec JSON Schema for CQ types.

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

* `FUNCTION, --function`: Function name to analyze  **[required]**  *[env: CQ_FUNCTION]*
* `--param`: Parameter name to trace  **[required]**  *[env: CQ_PARAM]*
* `--depth`: Maximum call depth  *[env: CQ_DEPTH]*  *[default: 5]*

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT, CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level  *[env: CQ_VERBOSE, CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT, CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR, CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT, CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG, CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG, CQ_USE_CONFIG]*  *[default: True]*

**Filters**:

* `--include, --empty-include`: Include files matching pattern (glob or ~regex, repeatable)  *[env: CQ_INCLUDE]*  *[default: []]*
* `--exclude, --empty-exclude`: Exclude files matching pattern (glob or ~regex, repeatable)  *[env: CQ_EXCLUDE]*  *[default: []]*
* `--impact, --empty-impact`: Filter by impact bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[env: CQ_IMPACT]*  *[default: []]*
* `--confidence, --empty-confidence`: Filter by confidence bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[env: CQ_CONFIDENCE]*  *[default: []]*
* `--severity, --empty-severity`: Filter by severity (comma-separated: error,warning,info)  *[choices: info, warning, error]*  *[env: CQ_SEVERITY]*  *[default: []]*
* `--limit`: Maximum number of findings  *[env: CQ_LIMIT]*  *[default: None]*

## cq calls

```console
cq calls [OPTIONS] FUNCTION
```

Census all call sites for a function.

**Parameters**:

* `FUNCTION, --function`: Function name to find calls for  **[required]**  *[env: CQ_FUNCTION]*

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT, CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level  *[env: CQ_VERBOSE, CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT, CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR, CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT, CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG, CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG, CQ_USE_CONFIG]*  *[default: True]*

**Filters**:

* `--include, --empty-include`: Include files matching pattern (glob or ~regex, repeatable)  *[env: CQ_INCLUDE]*  *[default: []]*
* `--exclude, --empty-exclude`: Exclude files matching pattern (glob or ~regex, repeatable)  *[env: CQ_EXCLUDE]*  *[default: []]*
* `--impact, --empty-impact`: Filter by impact bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[env: CQ_IMPACT]*  *[default: []]*
* `--confidence, --empty-confidence`: Filter by confidence bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[env: CQ_CONFIDENCE]*  *[default: []]*
* `--severity, --empty-severity`: Filter by severity (comma-separated: error,warning,info)  *[choices: info, warning, error]*  *[env: CQ_SEVERITY]*  *[default: []]*
* `--limit`: Maximum number of findings  *[env: CQ_LIMIT]*  *[default: None]*

## cq imports

```console
cq imports [OPTIONS]
```

Analyze import structure and cycles.

**Parameters**:

* `--cycles, --no-cycles`: Run cycle detection  *[env: CQ_CYCLES]*  *[default: False]*
* `--module`: Focus on specific module  *[env: CQ_MODULE]*  *[default: None]*

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT, CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level  *[env: CQ_VERBOSE, CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT, CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR, CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT, CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG, CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG, CQ_USE_CONFIG]*  *[default: True]*

**Filters**:

* `--include, --empty-include`: Include files matching pattern (glob or ~regex, repeatable)  *[env: CQ_INCLUDE]*  *[default: []]*
* `--exclude, --empty-exclude`: Exclude files matching pattern (glob or ~regex, repeatable)  *[env: CQ_EXCLUDE]*  *[default: []]*
* `--impact, --empty-impact`: Filter by impact bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[env: CQ_IMPACT]*  *[default: []]*
* `--confidence, --empty-confidence`: Filter by confidence bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[env: CQ_CONFIDENCE]*  *[default: []]*
* `--severity, --empty-severity`: Filter by severity (comma-separated: error,warning,info)  *[choices: info, warning, error]*  *[env: CQ_SEVERITY]*  *[default: []]*
* `--limit`: Maximum number of findings  *[env: CQ_LIMIT]*  *[default: None]*

## cq exceptions

```console
cq exceptions [OPTIONS]
```

Analyze exception handling patterns.

**Parameters**:

* `--function`: Focus on specific function  *[env: CQ_FUNCTION]*  *[default: None]*

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT, CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level  *[env: CQ_VERBOSE, CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT, CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR, CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT, CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG, CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG, CQ_USE_CONFIG]*  *[default: True]*

**Filters**:

* `--include, --empty-include`: Include files matching pattern (glob or ~regex, repeatable)  *[env: CQ_INCLUDE]*  *[default: []]*
* `--exclude, --empty-exclude`: Exclude files matching pattern (glob or ~regex, repeatable)  *[env: CQ_EXCLUDE]*  *[default: []]*
* `--impact, --empty-impact`: Filter by impact bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[env: CQ_IMPACT]*  *[default: []]*
* `--confidence, --empty-confidence`: Filter by confidence bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[env: CQ_CONFIDENCE]*  *[default: []]*
* `--severity, --empty-severity`: Filter by severity (comma-separated: error,warning,info)  *[choices: info, warning, error]*  *[env: CQ_SEVERITY]*  *[default: []]*
* `--limit`: Maximum number of findings  *[env: CQ_LIMIT]*  *[default: None]*

## cq sig-impact

```console
cq sig-impact --to STR [OPTIONS] SYMBOL
```

Analyze impact of a signature change.

**Parameters**:

* `SYMBOL, --symbol`: Function name to analyze  **[required]**  *[env: CQ_SYMBOL]*
* `--to`: New signature (e.g., "foo(a, b, *, c=None)")  **[required]**  *[env: CQ_TO]*

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT, CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level  *[env: CQ_VERBOSE, CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT, CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR, CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT, CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG, CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG, CQ_USE_CONFIG]*  *[default: True]*

**Filters**:

* `--include, --empty-include`: Include files matching pattern (glob or ~regex, repeatable)  *[env: CQ_INCLUDE]*  *[default: []]*
* `--exclude, --empty-exclude`: Exclude files matching pattern (glob or ~regex, repeatable)  *[env: CQ_EXCLUDE]*  *[default: []]*
* `--impact, --empty-impact`: Filter by impact bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[env: CQ_IMPACT]*  *[default: []]*
* `--confidence, --empty-confidence`: Filter by confidence bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[env: CQ_CONFIDENCE]*  *[default: []]*
* `--severity, --empty-severity`: Filter by severity (comma-separated: error,warning,info)  *[choices: info, warning, error]*  *[env: CQ_SEVERITY]*  *[default: []]*
* `--limit`: Maximum number of findings  *[env: CQ_LIMIT]*  *[default: None]*

## cq side-effects

```console
cq side-effects [OPTIONS]
```

Detect import-time side effects.

**Parameters**:

* `--max-files`: Maximum files to scan  *[env: CQ_MAX_FILES]*  *[default: 2000]*

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT, CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level  *[env: CQ_VERBOSE, CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT, CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR, CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT, CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG, CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG, CQ_USE_CONFIG]*  *[default: True]*

**Filters**:

* `--include, --empty-include`: Include files matching pattern (glob or ~regex, repeatable)  *[env: CQ_INCLUDE]*  *[default: []]*
* `--exclude, --empty-exclude`: Exclude files matching pattern (glob or ~regex, repeatable)  *[env: CQ_EXCLUDE]*  *[default: []]*
* `--impact, --empty-impact`: Filter by impact bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[env: CQ_IMPACT]*  *[default: []]*
* `--confidence, --empty-confidence`: Filter by confidence bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[env: CQ_CONFIDENCE]*  *[default: []]*
* `--severity, --empty-severity`: Filter by severity (comma-separated: error,warning,info)  *[choices: info, warning, error]*  *[env: CQ_SEVERITY]*  *[default: []]*
* `--limit`: Maximum number of findings  *[env: CQ_LIMIT]*  *[default: None]*

## cq scopes

```console
cq scopes [OPTIONS] TARGET
```

Analyze scope capture (closures).

**Parameters**:

* `TARGET, --target`: File path or symbol name to analyze  **[required]**  *[env: CQ_TARGET]*

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT, CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level  *[env: CQ_VERBOSE, CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT, CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR, CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT, CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG, CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG, CQ_USE_CONFIG]*  *[default: True]*

**Filters**:

* `--include, --empty-include`: Include files matching pattern (glob or ~regex, repeatable)  *[env: CQ_INCLUDE]*  *[default: []]*
* `--exclude, --empty-exclude`: Exclude files matching pattern (glob or ~regex, repeatable)  *[env: CQ_EXCLUDE]*  *[default: []]*
* `--impact, --empty-impact`: Filter by impact bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[env: CQ_IMPACT]*  *[default: []]*
* `--confidence, --empty-confidence`: Filter by confidence bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[env: CQ_CONFIDENCE]*  *[default: []]*
* `--severity, --empty-severity`: Filter by severity (comma-separated: error,warning,info)  *[choices: info, warning, error]*  *[env: CQ_SEVERITY]*  *[default: []]*
* `--limit`: Maximum number of findings  *[env: CQ_LIMIT]*  *[default: None]*

## cq bytecode-surface

```console
cq bytecode-surface [OPTIONS] TARGET
```

Analyze bytecode for hidden dependencies.

**Parameters**:

* `TARGET, --target`: File path or symbol name to analyze  **[required]**  *[env: CQ_TARGET]*
* `--show`: What to show: globals,attrs,constants,opcodes  *[env: CQ_SHOW]*  *[default: globals,attrs,constants]*

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT, CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level  *[env: CQ_VERBOSE, CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT, CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR, CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT, CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG, CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG, CQ_USE_CONFIG]*  *[default: True]*

**Filters**:

* `--include, --empty-include`: Include files matching pattern (glob or ~regex, repeatable)  *[env: CQ_INCLUDE]*  *[default: []]*
* `--exclude, --empty-exclude`: Exclude files matching pattern (glob or ~regex, repeatable)  *[env: CQ_EXCLUDE]*  *[default: []]*
* `--impact, --empty-impact`: Filter by impact bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[env: CQ_IMPACT]*  *[default: []]*
* `--confidence, --empty-confidence`: Filter by confidence bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[env: CQ_CONFIDENCE]*  *[default: []]*
* `--severity, --empty-severity`: Filter by severity (comma-separated: error,warning,info)  *[choices: info, warning, error]*  *[env: CQ_SEVERITY]*  *[default: []]*
* `--limit`: Maximum number of findings  *[env: CQ_LIMIT]*  *[default: None]*

## cq q

```console
cq q [OPTIONS] QUERY-STRING
```

Run a declarative code query using ast-grep.

**Parameters**:

* `QUERY-STRING, --query-string`: Query string (e.g., "entity=function name=foo")  **[required]**  *[env: CQ_QUERY_STRING]*
* `--explain-files, --no-explain-files`: Include file filtering diagnostics  *[env: CQ_EXPLAIN_FILES]*  *[default: False]*

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT, CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level  *[env: CQ_VERBOSE, CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT, CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR, CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT, CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG, CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG, CQ_USE_CONFIG]*  *[default: True]*

**Filters**:

* `--include, --empty-include`: Include files matching pattern (glob or ~regex, repeatable)  *[env: CQ_INCLUDE]*  *[default: []]*
* `--exclude, --empty-exclude`: Exclude files matching pattern (glob or ~regex, repeatable)  *[env: CQ_EXCLUDE]*  *[default: []]*
* `--impact, --empty-impact`: Filter by impact bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[env: CQ_IMPACT]*  *[default: []]*
* `--confidence, --empty-confidence`: Filter by confidence bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[env: CQ_CONFIDENCE]*  *[default: []]*
* `--severity, --empty-severity`: Filter by severity (comma-separated: error,warning,info)  *[choices: info, warning, error]*  *[env: CQ_SEVERITY]*  *[default: []]*
* `--limit`: Maximum number of findings  *[env: CQ_LIMIT]*  *[default: None]*

## cq search

```console
cq search [OPTIONS] QUERY
```

Search for code patterns with semantic enrichment.

**Parameters**:

* `QUERY, --query`: Search query  **[required]**  *[env: CQ_QUERY]*
* `--include-strings, --no-include-strings`: Include matches in strings/comments/docstrings  *[env: CQ_INCLUDE_STRINGS]*  *[default: False]*
* `--with-neighborhood, --no-with-neighborhood`: Include structural neighborhood preview (slower)  *[env: CQ_WITH_NEIGHBORHOOD]*  *[default: False]*
* `--in`: Restrict to directory  *[env: CQ_IN]*  *[default: None]*
* `--lang`: Search language scope (auto, python, rust)  *[choices: auto, python, rust]*  *[env: CQ_LANG]*  *[default: auto]*

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT, CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level  *[env: CQ_VERBOSE, CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT, CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR, CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT, CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG, CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG, CQ_USE_CONFIG]*  *[default: True]*

**Filters**:

* `--include, --empty-include`: Include files matching pattern (glob or ~regex, repeatable)  *[env: CQ_INCLUDE]*  *[default: []]*
* `--exclude, --empty-exclude`: Exclude files matching pattern (glob or ~regex, repeatable)  *[env: CQ_EXCLUDE]*  *[default: []]*
* `--impact, --empty-impact`: Filter by impact bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[env: CQ_IMPACT]*  *[default: []]*
* `--confidence, --empty-confidence`: Filter by confidence bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[env: CQ_CONFIDENCE]*  *[default: []]*
* `--severity, --empty-severity`: Filter by severity (comma-separated: error,warning,info)  *[choices: info, warning, error]*  *[env: CQ_SEVERITY]*  *[default: []]*
* `--limit`: Maximum number of findings  *[env: CQ_LIMIT]*  *[default: None]*

**Search Mode**:

* `--regex, --no-regex`: Treat query as regex  *[env: CQ_REGEX]*  *[default: False]*
* `--literal, --no-literal`: Treat query as literal  *[env: CQ_LITERAL]*  *[default: False]*

## cq report

```console
cq report --target STR [OPTIONS] PRESET
```

Run target-scoped report bundles.

**Parameters**:

* `PRESET, --preset`: Report preset (refactor-impact, safety-reliability, change-propagation, dependency-health)  **[required]**  *[choices: refactor-impact, safety-reliability, change-propagation, dependency-health]*  *[env: CQ_PRESET]*
* `--target`: Target spec (function:foo, class:Bar, module:pkg.mod, path:src/...)  **[required]**  *[env: CQ_TARGET]*
* `--in`: Restrict analysis to a directory  *[env: CQ_IN]*  *[default: None]*
* `--param`: Parameter name for impact analysis  *[env: CQ_PARAM]*  *[default: None]*
* `--to`: Proposed signature for sig-impact analysis  *[env: CQ_TO]*  *[default: None]*
* `--bytecode-show`: Bytecode surface fields  *[env: CQ_BYTECODE_SHOW]*  *[default: None]*

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT, CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level  *[env: CQ_VERBOSE, CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT, CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR, CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT, CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG, CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG, CQ_USE_CONFIG]*  *[default: True]*

**Filters**:

* `--include, --empty-include`: Include files matching pattern (glob or ~regex, repeatable)  *[env: CQ_INCLUDE]*  *[default: []]*
* `--exclude, --empty-exclude`: Exclude files matching pattern (glob or ~regex, repeatable)  *[env: CQ_EXCLUDE]*  *[default: []]*
* `--impact, --empty-impact`: Filter by impact bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[env: CQ_IMPACT]*  *[default: []]*
* `--confidence, --empty-confidence`: Filter by confidence bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[env: CQ_CONFIDENCE]*  *[default: []]*
* `--severity, --empty-severity`: Filter by severity (comma-separated: error,warning,info)  *[choices: info, warning, error]*  *[env: CQ_SEVERITY]*  *[default: []]*
* `--limit`: Maximum number of findings  *[env: CQ_LIMIT]*  *[default: None]*

## cq run

```console
cq run [OPTIONS]
```

Execute a multi-step CQ run plan.

**Parameters**:

* `--stop-on-error, --no-stop-on-error`: Stop execution on the first step error  *[env: CQ_STOP_ON_ERROR]*  *[default: False]*

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT, CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level  *[env: CQ_VERBOSE, CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT, CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR, CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT, CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG, CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG, CQ_USE_CONFIG]*  *[default: True]*

**Filters**:

* `--include, --empty-include`: Include files matching pattern (glob or ~regex, repeatable)  *[env: CQ_INCLUDE]*  *[default: []]*
* `--exclude, --empty-exclude`: Exclude files matching pattern (glob or ~regex, repeatable)  *[env: CQ_EXCLUDE]*  *[default: []]*
* `--impact, --empty-impact`: Filter by impact bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[env: CQ_IMPACT]*  *[default: []]*
* `--confidence, --empty-confidence`: Filter by confidence bucket (comma-separated: low,med,high)  *[choices: low, med, high]*  *[env: CQ_CONFIDENCE]*  *[default: []]*
* `--severity, --empty-severity`: Filter by severity (comma-separated: error,warning,info)  *[choices: info, warning, error]*  *[env: CQ_SEVERITY]*  *[default: []]*
* `--limit`: Maximum number of findings  *[env: CQ_LIMIT]*  *[default: None]*

**Run Input**:

* `--plan`: Path to a run plan TOML file  *[env: CQ_PLAN]*  *[default: None]*
* `--step, --empty-step`: Repeatable JSON step object (e.g., '{"type":"q","query":"..."}')  *[env: CQ_STEP]*  *[default: []]*
* `--steps, --empty-steps`: JSON array of steps (e.g., '[{"type":"q",...},{"type":"calls",...}]')  *[env: CQ_STEPS]*  *[default: []]*

## cq chain

```console
cq chain [OPTIONS]
```

Execute chained CQ commands via the shared run engine.

**Parameters**:

* `--delimiter`: Token used to split command segments.  *[env: CQ_DELIMITER]*  *[default: AND]*

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT, CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level  *[env: CQ_VERBOSE, CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT, CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR, CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT, CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG, CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG, CQ_USE_CONFIG]*  *[default: True]*

## cq neighborhood

```console
cq neighborhood [OPTIONS] TARGET
```

Analyze semantic neighborhood of a target symbol or location.

**Parameters**:

* `TARGET, --target`: Target location (file:line[:col] or symbol)  **[required]**  *[env: CQ_TARGET]*
* `--lang`: Query language (python, rust)  *[choices: python, rust]*  *[env: CQ_LANG]*  *[default: python]*
* `--top-k`: Max items per slice  *[env: CQ_TOP_K]*  *[default: 10]*
* `--semantic-enrichment, --no-semantic-enrichment`: Enable semantic enrichment  *[env: CQ_SEMANTIC_ENRICHMENT]*  *[default: True]*

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT, CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level  *[env: CQ_VERBOSE, CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT, CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR, CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT, CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG, CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG, CQ_USE_CONFIG]*  *[default: True]*

## cq index

```console
cq index [OPTIONS]
```

Handle deprecated index management flags.

**Parameters**:

* `--rebuild, --no-rebuild`: (Deprecated) Rebuild index  *[env: CQ_REBUILD]*  *[default: False]*
* `--status, --no-status`: (Deprecated) Show index status  *[env: CQ_STATUS]*  *[default: False]*

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT, CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level  *[env: CQ_VERBOSE, CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT, CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR, CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT, CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG, CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG, CQ_USE_CONFIG]*  *[default: True]*

## cq cache

```console
cq cache [OPTIONS]
```

Handle deprecated cache management flags.

**Parameters**:

* `--stats, --no-stats`: (Deprecated) Show cache statistics  *[env: CQ_STATS]*  *[default: False]*
* `--clear, --no-clear`: (Deprecated) Clear cache  *[env: CQ_CLEAR]*  *[default: False]*

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT, CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level  *[env: CQ_VERBOSE, CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT, CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR, CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT, CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG, CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG, CQ_USE_CONFIG]*  *[default: True]*

## cq schema

```console
cq schema [OPTIONS]
```

Emit msgspec JSON Schema for CQ types.

**Parameters**:

* `--kind`: Schema kind: result, query, or components  *[choices: result, query, components]*  *[env: CQ_KIND]*  *[default: result]*

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT, CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level  *[env: CQ_VERBOSE, CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT, CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR, CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT, CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG, CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG, CQ_USE_CONFIG]*  *[default: True]*

## cq ldmd

LDMD progressive disclosure protocol

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT, CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level  *[env: CQ_VERBOSE, CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT, CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR, CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT, CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG, CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG, CQ_USE_CONFIG]*  *[default: True]*

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

* `--root`: Repository root  *[env: CQ_ROOT, CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level  *[env: CQ_VERBOSE, CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT, CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR, CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT, CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG, CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG, CQ_USE_CONFIG]*  *[default: True]*

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

* `--root`: Repository root  *[env: CQ_ROOT, CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level  *[env: CQ_VERBOSE, CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT, CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR, CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT, CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG, CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG, CQ_USE_CONFIG]*  *[default: True]*

## cq --install-completion

```console
cq --install-completion [OPTIONS]
```

Install CQ shell completion scripts.

**Parameters**:

* `--shell`: Shell type for completion. If not specified, attempts to auto-detect current shell.  *[choices: zsh, bash, fish]*  *[env: CQ_SHELL]*  *[default: None]*
* `--output, -o`: Output path for the completion script. If not specified, uses shell-specific default.  *[env: CQ_O]*  *[default: None]*

**Global Options**:

Options applied to every CQ command.

* `--root`: Repository root  *[env: CQ_ROOT, CQ_ROOT]*  *[default: None]*
* `--verbose, -v`: Verbosity level  *[env: CQ_VERBOSE, CQ_VERBOSE]*  *[default: 0]*
* `--format`: Output format  *[choices: md, json, both, summary, mermaid, mermaid-class, dot, ldmd]*  *[env: CQ_FORMAT, CQ_FORMAT]*  *[default: md]*
* `--artifact-dir`: Artifact directory  *[env: CQ_ARTIFACT_DIR, CQ_ARTIFACT_DIR]*  *[default: None]*
* `--save-artifact, --no-save-artifact`: Persist output artifacts  *[env: CQ_SAVE_ARTIFACT, CQ_SAVE_ARTIFACT]*  *[default: True]*
* `--config`: Config file path  *[env: CQ_CONFIG, CQ_CONFIG]*  *[default: None]*
* `--use-config, --no-config`: Enable config file loading  *[env: CQ_USE_CONFIG, CQ_USE_CONFIG]*  *[default: True]*
