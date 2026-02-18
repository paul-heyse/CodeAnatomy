**datafusion_functions > crypto > expr_fn**

# Module: crypto::expr_fn

## Contents

**Functions**

- [`digest`](#digest) - Computes the binary hash of an expression using the specified algorithm.
- [`md5`](#md5) - Computes an MD5 128-bit checksum for a string expression.
- [`sha224`](#sha224) - Computes the SHA-224 hash of a binary string.
- [`sha256`](#sha256) - Computes the SHA-256 hash of a binary string.
- [`sha384`](#sha384) - Computes the SHA-384 hash of a binary string.
- [`sha512`](#sha512) - Computes the SHA-512 hash of a binary string.

---

## datafusion_functions::crypto::expr_fn::digest

*Function*

Computes the binary hash of an expression using the specified algorithm.

```rust
fn digest(input_arg1: datafusion_expr::Expr, input_arg2: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::crypto::expr_fn::md5

*Function*

Computes an MD5 128-bit checksum for a string expression.

```rust
fn md5(input_arg: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::crypto::expr_fn::sha224

*Function*

Computes the SHA-224 hash of a binary string.

```rust
fn sha224(input_arg1: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::crypto::expr_fn::sha256

*Function*

Computes the SHA-256 hash of a binary string.

```rust
fn sha256(input_arg1: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::crypto::expr_fn::sha384

*Function*

Computes the SHA-384 hash of a binary string.

```rust
fn sha384(input_arg1: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::crypto::expr_fn::sha512

*Function*

Computes the SHA-512 hash of a binary string.

```rust
fn sha512(input_arg1: datafusion_expr::Expr) -> datafusion_expr::Expr
```



