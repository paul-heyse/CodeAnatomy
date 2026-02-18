**datafusion_physical_expr_common > metrics > custom**

# Module: metrics::custom

## Contents

**Traits**

- [`CustomMetricValue`](#custommetricvalue) - A trait for implementing custom metric values.

---

## datafusion_physical_expr_common::metrics::custom::CustomMetricValue

*Trait*

A trait for implementing custom metric values.

This trait enables defining application- or operator-specific metric types
that can be aggregated and displayed alongside standard metrics. These
custom metrics integrate with [`MetricValue::Custom`] and support
aggregation logic, introspection, and optional numeric representation.

# Requirements
Implementations of `CustomMetricValue` must satisfy the following:

1. [`Self::aggregate`]: Defines how two metric values are combined
2. [`Self::new_empty`]: Returns a new, zero-value instance for accumulation
3. [`Self::as_any`]: Enables dynamic downcasting for type-specific operations
4. [`Self::as_usize`]: Optionally maps the value to a `usize` (for sorting, display, etc.)
5. [`Self::is_eq`]: Implements comparison between two values, this isn't reusing the std
   PartialEq trait because this trait is used dynamically in the context of
   [`MetricValue::Custom`]

# Examples
```
# use std::sync::Arc;
# use std::fmt::{Debug, Display};
# use std::any::Any;
# use std::sync::atomic::{AtomicUsize, Ordering};

# use datafusion_physical_expr_common::metrics::CustomMetricValue;

#[derive(Debug, Default)]
struct MyCounter {
    count: AtomicUsize,
}

impl Display for MyCounter {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "count: {}", self.count.load(Ordering::Relaxed))
    }
}

impl CustomMetricValue for MyCounter {
    fn new_empty(&self) -> Arc<dyn CustomMetricValue> {
        Arc::new(Self::default())
    }

    fn aggregate(&self, other: Arc<dyn CustomMetricValue>) {
        let other = other.as_any().downcast_ref::<Self>().unwrap();
        self.count
            .fetch_add(other.count.load(Ordering::Relaxed), Ordering::Relaxed);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_usize(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    fn is_eq(&self, other: &Arc<dyn CustomMetricValue>) -> bool {
        let Some(other) = other.as_any().downcast_ref::<Self>() else {
            return false;
        };

        self.count.load(Ordering::Relaxed) == other.count.load(Ordering::Relaxed)
    }
}
```

[`MetricValue::Custom`]: super::MetricValue::Custom

**Methods:**

- `new_empty`: Returns a new, zero-initialized version of this metric value.
- `aggregate`: Merges another metric value into this one.
- `as_any`: Returns this value as a [`Any`] to support dynamic downcasting.
- `as_usize`: Optionally returns a numeric representation of the value, if meaningful.
- `is_eq`: Compares this value with another custom value.



