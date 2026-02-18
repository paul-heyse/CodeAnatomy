**datafusion_functions_aggregate > bit_and_or_xor**

# Module: bit_and_or_xor

## Contents

**Functions**

- [`bit_and`](#bit_and) - Returns the bitwiseBitwiseOperationType::Andof a group of values
- [`bit_and_udaf`](#bit_and_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`bit_and`]
- [`bit_or`](#bit_or) - Returns the bitwiseBitwiseOperationType::Orof a group of values
- [`bit_or_udaf`](#bit_or_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`bit_or`]
- [`bit_xor`](#bit_xor) - Returns the bitwiseBitwiseOperationType::Xorof a group of values
- [`bit_xor_udaf`](#bit_xor_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`bit_xor`]

---

## datafusion_functions_aggregate::bit_and_or_xor::bit_and

*Function*

Returns the bitwiseBitwiseOperationType::Andof a group of values

```rust
fn bit_and(expr_x: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::bit_and_or_xor::bit_and_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`bit_and`]

```rust
fn bit_and_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



## datafusion_functions_aggregate::bit_and_or_xor::bit_or

*Function*

Returns the bitwiseBitwiseOperationType::Orof a group of values

```rust
fn bit_or(expr_x: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::bit_and_or_xor::bit_or_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`bit_or`]

```rust
fn bit_or_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



## datafusion_functions_aggregate::bit_and_or_xor::bit_xor

*Function*

Returns the bitwiseBitwiseOperationType::Xorof a group of values

```rust
fn bit_xor(expr_x: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::bit_and_or_xor::bit_xor_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`bit_xor`]

```rust
fn bit_xor_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



