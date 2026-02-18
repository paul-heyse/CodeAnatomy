**datafusion_tracing**

# Module: datafusion_tracing

## Contents

**Macros**

- [`instrument_rules_with_debug_spans`](#instrument_rules_with_debug_spans) - Instruments a `SessionState` with DEBUG-level tracing spans.
- [`instrument_rules_with_error_spans`](#instrument_rules_with_error_spans) - Instruments a `SessionState` with ERROR-level tracing spans.
- [`instrument_rules_with_info_spans`](#instrument_rules_with_info_spans) - Instruments a `SessionState` with INFO-level tracing spans.
- [`instrument_rules_with_spans`](#instrument_rules_with_spans) - Instruments a `SessionState` with tracing spans for all rule phases.
- [`instrument_rules_with_trace_spans`](#instrument_rules_with_trace_spans) - Instruments a `SessionState` with TRACE-level tracing spans.
- [`instrument_rules_with_warn_spans`](#instrument_rules_with_warn_spans) - Instruments a `SessionState` with WARN-level tracing spans.
- [`instrument_with_debug_spans`](#instrument_with_debug_spans) - Constructs a new instrumentation `PhysicalOptimizerRule` for a DataFusion `ExecutionPlan` at the debug level.
- [`instrument_with_error_spans`](#instrument_with_error_spans) - Constructs a new instrumentation `PhysicalOptimizerRule` for a DataFusion `ExecutionPlan` at the error level.
- [`instrument_with_info_spans`](#instrument_with_info_spans) - Constructs a new instrumentation `PhysicalOptimizerRule` for a DataFusion `ExecutionPlan` at the info level.
- [`instrument_with_spans`](#instrument_with_spans) - Constructs a new instrumentation `PhysicalOptimizerRule` for a DataFusion `ExecutionPlan`.
- [`instrument_with_trace_spans`](#instrument_with_trace_spans) - Constructs a new instrumentation `PhysicalOptimizerRule` for a DataFusion `ExecutionPlan` at the trace level.
- [`instrument_with_warn_spans`](#instrument_with_warn_spans) - Constructs a new instrumentation `PhysicalOptimizerRule` for a DataFusion `ExecutionPlan` at the warn level.

---

## datafusion_tracing::instrument_rules_with_debug_spans

*Declarative Macro*

Instruments a `SessionState` with DEBUG-level tracing spans.

This is a convenience wrapper around [`instrument_rules_with_spans!`] that automatically
sets the tracing level to `DEBUG`.

See [`instrument_rules_with_spans!`] for detailed documentation.

# Example

```rust,ignore
use datafusion_tracing::{instrument_rules_with_debug_spans, RuleInstrumentationOptions};

let options = RuleInstrumentationOptions::full();
let session_state = instrument_rules_with_debug_spans!(
    options: options,
    state: session_state
);
```

```rust
macro_rules! instrument_rules_with_debug_spans {
    (target: $target:expr, options: $options:expr, state: $state:expr, $($field:tt)*) => { ... };
    (options: $options:expr, state: $state:expr, $($field:tt)*) => { ... };
    (target: $target:expr, options: $options:expr, state: $state:expr) => { ... };
    (options: $options:expr, state: $state:expr) => { ... };
}
```



## datafusion_tracing::instrument_rules_with_error_spans

*Declarative Macro*

Instruments a `SessionState` with ERROR-level tracing spans.

This is a convenience wrapper around [`instrument_rules_with_spans!`] that automatically
sets the tracing level to `ERROR`.

See [`instrument_rules_with_spans!`] for detailed documentation.

# Example

```rust,ignore
use datafusion_tracing::{instrument_rules_with_error_spans, RuleInstrumentationOptions};

let options = RuleInstrumentationOptions::full();
let session_state = instrument_rules_with_error_spans!(
    options: options,
    state: session_state
);
```

```rust
macro_rules! instrument_rules_with_error_spans {
    (target: $target:expr, options: $options:expr, state: $state:expr, $($field:tt)*) => { ... };
    (options: $options:expr, state: $state:expr, $($field:tt)*) => { ... };
    (target: $target:expr, options: $options:expr, state: $state:expr) => { ... };
    (options: $options:expr, state: $state:expr) => { ... };
}
```



## datafusion_tracing::instrument_rules_with_info_spans

*Declarative Macro*

Instruments a `SessionState` with INFO-level tracing spans.

This is a convenience wrapper around [`instrument_rules_with_spans!`] that automatically
sets the tracing level to `INFO`.

See [`instrument_rules_with_spans!`] for detailed documentation.

# Example

```rust,ignore
use datafusion_tracing::{instrument_rules_with_info_spans, RuleInstrumentationOptions};

let options = RuleInstrumentationOptions::full().with_plan_diff();
let session_state = instrument_rules_with_info_spans!(
    options: options,
    state: session_state
);
```

```rust
macro_rules! instrument_rules_with_info_spans {
    (target: $target:expr, options: $options:expr, state: $state:expr, $($field:tt)*) => { ... };
    (options: $options:expr, state: $state:expr, $($field:tt)*) => { ... };
    (target: $target:expr, options: $options:expr, state: $state:expr) => { ... };
    (options: $options:expr, state: $state:expr) => { ... };
}
```



## datafusion_tracing::instrument_rules_with_spans

*Declarative Macro*

Instruments a `SessionState` with tracing spans for all rule phases.

This macro wraps analyzer, optimizer, and physical optimizer rules with tracing spans
at the specified level. Rule spans are grouped under phase spans
("analyze_logical_plan", "optimize_logical_plan", "optimize_physical_plan").

When physical optimizer instrumentation is enabled, the query planner is also
automatically instrumented to trace physical plan creation.

# Syntax

```ignore
instrument_rules_with_spans!(
    $level,
    options: $options,
    state: $session_state,
    $($fields)*
)
```

# Arguments

* `$level` - The tracing level (e.g., `tracing::Level::INFO`)
* `options` - A `RuleInstrumentationOptions` instance
* `state` - A `SessionState` to instrument
* `$fields` - Optional additional span fields

# Example

```rust,ignore
use datafusion_tracing::{instrument_rules_with_info_spans, RuleInstrumentationOptions};

let options = RuleInstrumentationOptions::full().with_plan_diff();
let session_state = instrument_rules_with_info_spans!(
    options: options,
    state: session_state
);
```

```rust
macro_rules! instrument_rules_with_spans {
    (target: $target:expr, $lvl:expr, options: $options:expr, state: $state:expr, $($fields:tt)*) => { ... };
    (target: $target:expr, $lvl:expr, options: $options:expr, state: $state:expr) => { ... };
    ($lvl:expr, options: $options:expr, state: $state:expr, $($fields:tt)*) => { ... };
    ($lvl:expr, options: $options:expr, state: $state:expr) => { ... };
}
```



## datafusion_tracing::instrument_rules_with_trace_spans

*Declarative Macro*

Instruments a `SessionState` with TRACE-level tracing spans.

This is a convenience wrapper around [`instrument_rules_with_spans!`] that automatically
sets the tracing level to `TRACE`.

See [`instrument_rules_with_spans!`] for detailed documentation.

# Example

```rust,ignore
use datafusion_tracing::{instrument_rules_with_trace_spans, RuleInstrumentationOptions};

let options = RuleInstrumentationOptions::full();
let session_state = instrument_rules_with_trace_spans!(
    options: options,
    state: session_state
);
```

```rust
macro_rules! instrument_rules_with_trace_spans {
    (target: $target:expr, options: $options:expr, state: $state:expr, $($field:tt)*) => { ... };
    (options: $options:expr, state: $state:expr, $($field:tt)*) => { ... };
    (target: $target:expr, options: $options:expr, state: $state:expr) => { ... };
    (options: $options:expr, state: $state:expr) => { ... };
}
```



## datafusion_tracing::instrument_rules_with_warn_spans

*Declarative Macro*

Instruments a `SessionState` with WARN-level tracing spans.

This is a convenience wrapper around [`instrument_rules_with_spans!`] that automatically
sets the tracing level to `WARN`.

See [`instrument_rules_with_spans!`] for detailed documentation.

# Example

```rust,ignore
use datafusion_tracing::{instrument_rules_with_warn_spans, RuleInstrumentationOptions};

let options = RuleInstrumentationOptions::full();
let session_state = instrument_rules_with_warn_spans!(
    options: options,
    state: session_state
);
```

```rust
macro_rules! instrument_rules_with_warn_spans {
    (target: $target:expr, options: $options:expr, state: $state:expr, $($field:tt)*) => { ... };
    (options: $options:expr, state: $state:expr, $($field:tt)*) => { ... };
    (target: $target:expr, options: $options:expr, state: $state:expr) => { ... };
    (options: $options:expr, state: $state:expr) => { ... };
}
```



## datafusion_tracing::instrument_with_debug_spans

*Declarative Macro*

Constructs a new instrumentation `PhysicalOptimizerRule` for a DataFusion `ExecutionPlan` at the debug level.

This macro automatically sets the tracing level to `DEBUG` and is a convenience wrapper around
[`instrument_with_spans!`] with the appropriate log level preset.

See [`instrument_with_spans!`] for details on instrumentation options and span fields.

# Examples

Basic usage with default options:
```rust
# use datafusion_tracing::{instrument_with_debug_spans, InstrumentationOptions};
let instrument_rule = instrument_with_debug_spans!(options: InstrumentationOptions::default());
```

Adding custom fields:
```rust
# use datafusion_tracing::{instrument_with_debug_spans, InstrumentationOptions};
# use tracing::field;
# use std::collections::HashMap;
let custom_fields = HashMap::from([
    ("custom.key1".to_string(), "value1".to_string()),
]);
let options = InstrumentationOptions {
   custom_fields,
   ..Default::default()
};
let instrument_rule = instrument_with_debug_spans!(
    options: options,
    datafusion.additional_info = "some info",
    custom.key1 = field::Empty,
);
```

[tracing_debug_span]: https://docs.rs/tracing/latest/tracing/macro.debug_span.html
[`debug_span!`]: https://docs.rs/tracing/latest/tracing/macro.debug_span.html
[`instrument_with_spans!`]: crate::instrument_with_spans!

```rust
macro_rules! instrument_with_debug_spans {
    (target: $target:expr, options: $options:expr, $($field:tt)*) => { ... };
    (options: $options:expr, $($field:tt)*) => { ... };
    (target: $target:expr, options: $options:expr) => { ... };
    (options: $options:expr) => { ... };
}
```



## datafusion_tracing::instrument_with_error_spans

*Declarative Macro*

Constructs a new instrumentation `PhysicalOptimizerRule` for a DataFusion `ExecutionPlan` at the error level.

This macro automatically sets the tracing level to `ERROR` and is a convenience wrapper around
[`instrument_with_spans!`] with the appropriate log level preset.

See [`instrument_with_spans!`] for details on instrumentation options and span fields.

# Examples

Basic usage with default options:
```rust
# use datafusion_tracing::{instrument_with_error_spans, InstrumentationOptions};
let instrument_rule = instrument_with_error_spans!(options: InstrumentationOptions::default());
```

Adding custom fields:
```rust
# use datafusion_tracing::{instrument_with_error_spans, InstrumentationOptions};
# use tracing::field;
# use std::collections::HashMap;
let custom_fields = HashMap::from([
    ("custom.key1".to_string(), "value1".to_string()),
]);
let options = InstrumentationOptions {
   custom_fields,
   ..Default::default()
};
let instrument_rule = instrument_with_error_spans!(
    options: options,
    datafusion.additional_info = "some info",
    custom.key1 = field::Empty,
);
```

[tracing_error_span]: https://docs.rs/tracing/latest/tracing/macro.error_span.html
[`error_span!`]: https://docs.rs/tracing/latest/tracing/macro.error_span.html
[`instrument_with_spans!`]: crate::instrument_with_spans!

```rust
macro_rules! instrument_with_error_spans {
    (target: $target:expr, options: $options:expr, $($field:tt)*) => { ... };
    (options: $options:expr, $($field:tt)*) => { ... };
    (target: $target:expr, options: $options:expr) => { ... };
    (options: $options:expr) => { ... };
}
```



## datafusion_tracing::instrument_with_info_spans

*Declarative Macro*

Constructs a new instrumentation `PhysicalOptimizerRule` for a DataFusion `ExecutionPlan` at the info level.

This macro automatically sets the tracing level to `INFO` and is a convenience wrapper around
[`instrument_with_spans!`] with the appropriate log level preset.

See [`instrument_with_spans!`] for details on instrumentation options and span fields.

# Examples

Basic usage with default options:
```rust
# use datafusion_tracing::{instrument_with_info_spans, InstrumentationOptions};
let instrument_rule = instrument_with_info_spans!(options: InstrumentationOptions::default());
```

Adding custom fields:
```rust
# use datafusion_tracing::{instrument_with_info_spans, InstrumentationOptions};
# use tracing::field;
# use std::collections::HashMap;
let custom_fields = HashMap::from([
    ("custom.key1".to_string(), "value1".to_string()),
]);
let options = InstrumentationOptions {
   custom_fields,
   ..Default::default()
};
let instrument_rule = instrument_with_info_spans!(
    options: options,
    datafusion.additional_info = "some info",
    custom.key1 = field::Empty,
);
```

[tracing_info_span]: https://docs.rs/tracing/latest/tracing/macro.info_span.html
[`info_span!`]: https://docs.rs/tracing/latest/tracing/macro.info_span.html
[`instrument_with_spans!`]: crate::instrument_with_spans!

```rust
macro_rules! instrument_with_info_spans {
    (target: $target:expr, options: $options:expr, $($field:tt)*) => { ... };
    (options: $options:expr, $($field:tt)*) => { ... };
    (target: $target:expr, options: $options:expr) => { ... };
    (options: $options:expr) => { ... };
}
```



## datafusion_tracing::instrument_with_spans

*Declarative Macro*

Constructs a new instrumentation `PhysicalOptimizerRule` for a DataFusion `ExecutionPlan`.

This macro is modeled after the [`span!`] macro from the [tracing] crate but is specifically designed
to instrument DataFusion execution plans. It creates a new `PhysicalOptimizerRule` that will wrap
each node of the plan in a custom `InstrumentedExec` node.

By default, it includes all known metrics fields relevant to DataFusion execution.

# Instrumentation Options

The instrumentation options are specified via the [`crate::InstrumentationOptions`] struct, which includes:
- `record_metrics`: Enable or disable recording of DataFusion execution metrics.
- `preview_limit`: Set the number of rows to preview per span (set to `0` to disable).
- `preview_fn`: Provide an optional callback for formatting previewed batches.
  If unspecified, [`datafusion::arrow::util::pretty::pretty_format_batches`] will be used.
- `custom_fields`: Provide custom key-value pairs for additional span metadata.

# Span Fields

This macro supports the same field–value syntax used by tracing's span macros, allowing you to add additional
contextual information as needed.

Refer to the [tracing documentation][tracing_span] for more information on the syntax
accepted by these macros, as it closely follows `span!`.

# Examples

Creating a new `InstrumentRule` to wrap the plan with TRACE level spans:
```rust
# use datafusion_tracing::{instrument_with_spans, InstrumentationOptions};
# use tracing::Level;
let instrument_rule = instrument_with_spans!(Level::TRACE, options: InstrumentationOptions::default());
```

Adding additional fields to the instrumentation:
```rust
# use datafusion_tracing::{instrument_with_spans, InstrumentationOptions};
# use tracing::{field, Level};
# use std::collections::HashMap;
let custom_fields = HashMap::from([
    ("custom.key1".to_string(), "value1".to_string()),
    ("custom.key2".to_string(), "value2".to_string()),
]);
let options = InstrumentationOptions {
   record_metrics: true,
   preview_limit: 10,
   custom_fields,
   ..Default::default()
};
let instrument_rule = instrument_with_spans!(
    Level::INFO,
    options: options,
    datafusion.additional_info = "some info",
    datafusion.user_id = 42,
    custom.key1 = field::Empty,
    custom.key2 = field::Empty,
);
// The instrumentation now includes additional fields, and all spans will be tagged with:
// - "datafusion.additional_info": "some info"
// - "datafusion.user_id": 42
// - "custom.key1": "value1"
// - "custom.key2": "value2"
// as well as all DataFusion metrics fields, and a 10 line preview of the data.
```

[tracing_span]: https://docs.rs/tracing/latest/tracing/macro.span.html
[`span!`]: https://docs.rs/tracing/latest/tracing/macro.span.html

**Note for crate Developers:**

The list of native datafusion metrics can be re-generated by running the following bash command at the root of the [datafusion](https://github.com/apache/datafusion) repository:
```bash
(
  find . -type f -name '*.rs' ! -path '*/metrics/mod.rs' -exec grep -A2 'MetricBuilder::new' {} \; | grep -E '(counter|gauge|subset_time)'
  grep -E -o 'Self::.*=>.*"' datafusion/physical-plan/src/metrics/value.rs
) | cut -s -f 2 -d '"' | sort -u | sed 's/\(.*\)/datafusion.metrics.\1 = tracing::field::Empty,/g'
```

```rust
macro_rules! instrument_with_spans {
    (target: $target:expr, $lvl:expr, options: $options:expr, $($fields:tt)*) => { ... };
    (target: $target:expr, $lvl:expr, options: $options:expr) => { ... };
    ($lvl:expr, options: $options:expr, $($fields:tt)*) => { ... };
    ($lvl:expr, options: $options:expr) => { ... };
}
```



## datafusion_tracing::instrument_with_trace_spans

*Declarative Macro*

Constructs a new instrumentation `PhysicalOptimizerRule` for a DataFusion `ExecutionPlan` at the trace level.

This macro automatically sets the tracing level to `TRACE` and is a convenience wrapper around
[`instrument_with_spans!`] with the appropriate log level preset.

See [`instrument_with_spans!`] for details on instrumentation options and span fields.

# Examples

Basic usage with default options:
```rust
# use datafusion_tracing::{instrument_with_trace_spans, InstrumentationOptions};
let instrument_rule = instrument_with_trace_spans!(options: InstrumentationOptions::default());
```

Adding custom fields:
```rust
# use datafusion_tracing::{instrument_with_trace_spans, InstrumentationOptions};
# use tracing::field;
# use std::collections::HashMap;
let custom_fields = HashMap::from([
    ("custom.key1".to_string(), "value1".to_string()),
]);
let options = InstrumentationOptions {
   custom_fields,
   ..Default::default()
};
let instrument_rule = instrument_with_trace_spans!(
    options: options,
    datafusion.additional_info = "some info",
    custom.key1 = field::Empty,
);
```

[tracing_trace_span]: https://docs.rs/tracing/latest/tracing/macro.trace_span.html
[`trace_span!`]: https://docs.rs/tracing/latest/tracing/macro.trace_span.html
[`instrument_with_spans!`]: crate::instrument_with_spans!

```rust
macro_rules! instrument_with_trace_spans {
    (target: $target:expr, options: $options:expr, $($field:tt)*) => { ... };
    (options: $options:expr, $($field:tt)*) => { ... };
    (target: $target:expr, options: $options:expr) => { ... };
    (options: $options:expr) => { ... };
}
```



## datafusion_tracing::instrument_with_warn_spans

*Declarative Macro*

Constructs a new instrumentation `PhysicalOptimizerRule` for a DataFusion `ExecutionPlan` at the warn level.

This macro automatically sets the tracing level to `WARN` and is a convenience wrapper around
[`instrument_with_spans!`] with the appropriate log level preset.

See [`instrument_with_spans!`] for details on instrumentation options and span fields.

# Examples

Basic usage with default options:
```rust
# use datafusion_tracing::{instrument_with_warn_spans, InstrumentationOptions};
let instrument_rule = instrument_with_warn_spans!(options: InstrumentationOptions::default());
```

Adding custom fields:
```rust
# use datafusion_tracing::{instrument_with_warn_spans, InstrumentationOptions};
# use tracing::field;
# use std::collections::HashMap;
let custom_fields = HashMap::from([
    ("custom.key1".to_string(), "value1".to_string()),
]);
let options = InstrumentationOptions {
   custom_fields,
   ..Default::default()
};
let instrument_rule = instrument_with_warn_spans!(
    options: options,
    datafusion.additional_info = "some info",
    custom.key1 = field::Empty,
);
```

[tracing_warn_span]: https://docs.rs/tracing/latest/tracing/macro.warn_span.html
[`warn_span!`]: https://docs.rs/tracing/latest/tracing/macro.warn_span.html
[`instrument_with_spans!`]: crate::instrument_with_spans!

```rust
macro_rules! instrument_with_warn_spans {
    (target: $target:expr, options: $options:expr, $($field:tt)*) => { ... };
    (options: $options:expr, $($field:tt)*) => { ... };
    (target: $target:expr, options: $options:expr) => { ... };
    (options: $options:expr) => { ... };
}
```



