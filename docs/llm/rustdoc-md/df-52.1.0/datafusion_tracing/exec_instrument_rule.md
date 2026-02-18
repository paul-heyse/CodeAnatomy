**datafusion_tracing > exec_instrument_rule**

# Module: exec_instrument_rule

## Contents

**Functions**

- [`new_instrument_rule`](#new_instrument_rule)

---

## datafusion_tracing::exec_instrument_rule::new_instrument_rule

*Function*

```rust
fn new_instrument_rule(span_create_fn: std::sync::Arc<dyn Fn>, options: crate::options::InstrumentationOptions) -> std::sync::Arc<dyn PhysicalOptimizerRule>
```



