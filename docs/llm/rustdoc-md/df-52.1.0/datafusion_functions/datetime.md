**datafusion_functions > datetime**

# Module: datetime

## Contents

**Modules**

- [`common`](#common)
- [`current_date`](#current_date)
- [`current_time`](#current_time)
- [`date_bin`](#date_bin)
- [`date_part`](#date_part)
- [`date_trunc`](#date_trunc)
- [`expr_fn`](#expr_fn)
- [`from_unixtime`](#from_unixtime)
- [`make_date`](#make_date)
- [`make_time`](#make_time)
- [`now`](#now)
- [`planner`](#planner) - SQL planning extensions like [`DatetimeFunctionPlanner`]
- [`to_char`](#to_char)
- [`to_date`](#to_date)
- [`to_local_time`](#to_local_time)
- [`to_time`](#to_time)
- [`to_timestamp`](#to_timestamp)
- [`to_unixtime`](#to_unixtime)

**Functions**

- [`current_date`](#current_date) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of current_date
- [`current_time`](#current_time) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of current_time
- [`date_bin`](#date_bin) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of date_bin
- [`date_part`](#date_part) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of date_part
- [`date_trunc`](#date_trunc) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of date_trunc
- [`from_unixtime`](#from_unixtime) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of from_unixtime
- [`functions`](#functions) - Returns all DataFusion functions defined in this package
- [`make_date`](#make_date) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of make_date
- [`make_time`](#make_time) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of make_time
- [`now`](#now) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of now
- [`to_char`](#to_char) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of to_char
- [`to_date`](#to_date) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of to_date
- [`to_local_time`](#to_local_time) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of to_local_time
- [`to_time`](#to_time) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of to_time
- [`to_timestamp`](#to_timestamp) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of to_timestamp
- [`to_timestamp_micros`](#to_timestamp_micros) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of to_timestamp_micros
- [`to_timestamp_millis`](#to_timestamp_millis) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of to_timestamp_millis
- [`to_timestamp_nanos`](#to_timestamp_nanos) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of to_timestamp_nanos
- [`to_timestamp_seconds`](#to_timestamp_seconds) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of to_timestamp_seconds
- [`to_unixtime`](#to_unixtime) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of to_unixtime

---

## Module: common



## datafusion_functions::datetime::current_date

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of current_date

```rust
fn current_date() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: current_date



## Module: current_time



## datafusion_functions::datetime::current_time

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of current_time

```rust
fn current_time() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::datetime::date_bin

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of date_bin

```rust
fn date_bin() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: date_bin



## Module: date_part



## datafusion_functions::datetime::date_part

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of date_part

```rust
fn date_part() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: date_trunc



## datafusion_functions::datetime::date_trunc

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of date_trunc

```rust
fn date_trunc() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: expr_fn



## datafusion_functions::datetime::from_unixtime

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of from_unixtime

```rust
fn from_unixtime() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: from_unixtime



## datafusion_functions::datetime::functions

*Function*

Returns all DataFusion functions defined in this package

```rust
fn functions() -> Vec<std::sync::Arc<datafusion_expr::ScalarUDF>>
```



## Module: make_date



## datafusion_functions::datetime::make_date

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of make_date

```rust
fn make_date() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: make_time



## datafusion_functions::datetime::make_time

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of make_time

```rust
fn make_time() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::datetime::now

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of now

```rust
fn now(config: &datafusion_common::config::ConfigOptions) -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: now



## Module: planner

SQL planning extensions like [`DatetimeFunctionPlanner`]



## datafusion_functions::datetime::to_char

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of to_char

```rust
fn to_char() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: to_char



## Module: to_date



## datafusion_functions::datetime::to_date

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of to_date

```rust
fn to_date() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::datetime::to_local_time

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of to_local_time

```rust
fn to_local_time() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: to_local_time



## Module: to_time



## datafusion_functions::datetime::to_time

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of to_time

```rust
fn to_time() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::datetime::to_timestamp

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of to_timestamp

```rust
fn to_timestamp(config: &datafusion_common::config::ConfigOptions) -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: to_timestamp



## datafusion_functions::datetime::to_timestamp_micros

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of to_timestamp_micros

```rust
fn to_timestamp_micros(config: &datafusion_common::config::ConfigOptions) -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::datetime::to_timestamp_millis

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of to_timestamp_millis

```rust
fn to_timestamp_millis(config: &datafusion_common::config::ConfigOptions) -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::datetime::to_timestamp_nanos

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of to_timestamp_nanos

```rust
fn to_timestamp_nanos(config: &datafusion_common::config::ConfigOptions) -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::datetime::to_timestamp_seconds

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of to_timestamp_seconds

```rust
fn to_timestamp_seconds(config: &datafusion_common::config::ConfigOptions) -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::datetime::to_unixtime

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of to_unixtime

```rust
fn to_unixtime() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: to_unixtime



