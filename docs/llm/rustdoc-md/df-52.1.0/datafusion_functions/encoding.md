**datafusion_functions > encoding**

# Module: encoding

## Contents

**Modules**

- [`expr_fn`](#expr_fn)
- [`inner`](#inner) - Encoding expressions

**Functions**

- [`decode`](#decode) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of decode
- [`encode`](#encode) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of encode
- [`functions`](#functions) - Returns all DataFusion functions defined in this package

---

## datafusion_functions::encoding::decode

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of decode

```rust
fn decode() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::encoding::encode

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of encode

```rust
fn encode() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: expr_fn



## datafusion_functions::encoding::functions

*Function*

Returns all DataFusion functions defined in this package

```rust
fn functions() -> Vec<std::sync::Arc<datafusion_expr::ScalarUDF>>
```



## Module: inner

Encoding expressions



