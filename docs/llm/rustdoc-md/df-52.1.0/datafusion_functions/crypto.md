**datafusion_functions > crypto**

# Module: crypto

## Contents

**Modules**

- [`basic`](#basic) - "crypto" DataFusion functions
- [`digest`](#digest)
- [`expr_fn`](#expr_fn)
- [`md5`](#md5)
- [`sha`](#sha)

**Functions**

- [`digest`](#digest) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of digest
- [`functions`](#functions) - Returns all DataFusion functions defined in this package
- [`md5`](#md5) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of md5
- [`sha224`](#sha224) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of sha224
- [`sha256`](#sha256) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of sha256
- [`sha384`](#sha384) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of sha384
- [`sha512`](#sha512) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of sha512

---

## Module: basic

"crypto" DataFusion functions



## datafusion_functions::crypto::digest

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of digest

```rust
fn digest() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: digest



## Module: expr_fn



## datafusion_functions::crypto::functions

*Function*

Returns all DataFusion functions defined in this package

```rust
fn functions() -> Vec<std::sync::Arc<datafusion_expr::ScalarUDF>>
```



## datafusion_functions::crypto::md5

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of md5

```rust
fn md5() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: md5



## Module: sha



## datafusion_functions::crypto::sha224

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of sha224

```rust
fn sha224() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::crypto::sha256

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of sha256

```rust
fn sha256() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::crypto::sha384

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of sha384

```rust
fn sha384() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::crypto::sha512

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of sha512

```rust
fn sha512() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



