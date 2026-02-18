**datafusion_functions > encoding > expr_fn**

# Module: encoding::expr_fn

## Contents

**Functions**

- [`decode`](#decode) - decode the `input`, using the `encoding`. encoding can be base64 or hex
- [`encode`](#encode) - encode the `input`, using the `encoding`. encoding can be base64 or hex

---

## datafusion_functions::encoding::expr_fn::decode

*Function*

decode the `input`, using the `encoding`. encoding can be base64 or hex

```rust
fn decode(input: datafusion_expr::Expr, encoding: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::encoding::expr_fn::encode

*Function*

encode the `input`, using the `encoding`. encoding can be base64 or hex

```rust
fn encode(input: datafusion_expr::Expr, encoding: datafusion_expr::Expr) -> datafusion_expr::Expr
```



