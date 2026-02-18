**datafusion_common > display > human_readable**

# Module: display::human_readable

## Contents

**Modules**

- [`units`](#units) - Common data size units

**Functions**

- [`human_readable_count`](#human_readable_count) - Present count in human-readable form with K, M, B, T suffixes
- [`human_readable_duration`](#human_readable_duration) - Present duration in human-readable form with 2 decimal places
- [`human_readable_size`](#human_readable_size) - Present size in human-readable form

---

## datafusion_common::display::human_readable::human_readable_count

*Function*

Present count in human-readable form with K, M, B, T suffixes

```rust
fn human_readable_count(count: usize) -> String
```



## datafusion_common::display::human_readable::human_readable_duration

*Function*

Present duration in human-readable form with 2 decimal places

```rust
fn human_readable_duration(nanos: u64) -> String
```



## datafusion_common::display::human_readable::human_readable_size

*Function*

Present size in human-readable form

```rust
fn human_readable_size(size: usize) -> String
```



## Module: units

Common data size units



