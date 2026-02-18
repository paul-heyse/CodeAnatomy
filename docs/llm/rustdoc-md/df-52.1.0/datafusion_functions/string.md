**datafusion_functions > string**

# Module: string

## Contents

**Modules**

- [`ascii`](#ascii)
- [`bit_length`](#bit_length)
- [`btrim`](#btrim)
- [`chr`](#chr)
- [`common`](#common) - Common utilities for implementing string functions
- [`concat`](#concat)
- [`concat_ws`](#concat_ws)
- [`contains`](#contains)
- [`ends_with`](#ends_with)
- [`expr_fn`](#expr_fn)
- [`levenshtein`](#levenshtein)
- [`lower`](#lower)
- [`ltrim`](#ltrim)
- [`octet_length`](#octet_length)
- [`overlay`](#overlay)
- [`repeat`](#repeat)
- [`replace`](#replace)
- [`rtrim`](#rtrim)
- [`split_part`](#split_part)
- [`starts_with`](#starts_with)
- [`to_hex`](#to_hex)
- [`upper`](#upper)
- [`uuid`](#uuid)

**Functions**

- [`ascii`](#ascii) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of ascii
- [`bit_length`](#bit_length) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of bit_length
- [`btrim`](#btrim) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of btrim
- [`chr`](#chr) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of chr
- [`concat`](#concat) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of concat
- [`concat_ws`](#concat_ws) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of concat_ws
- [`contains`](#contains) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of contains
- [`ends_with`](#ends_with) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of ends_with
- [`functions`](#functions) - Returns all DataFusion functions defined in this package
- [`levenshtein`](#levenshtein) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of levenshtein
- [`lower`](#lower) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of lower
- [`ltrim`](#ltrim) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of ltrim
- [`octet_length`](#octet_length) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of octet_length
- [`repeat`](#repeat) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of repeat
- [`replace`](#replace) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of replace
- [`rtrim`](#rtrim) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of rtrim
- [`split_part`](#split_part) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of split_part
- [`starts_with`](#starts_with) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of starts_with
- [`to_hex`](#to_hex) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of to_hex
- [`upper`](#upper) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of upper
- [`uuid`](#uuid) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of uuid

---

## Module: ascii



## datafusion_functions::string::ascii

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of ascii

```rust
fn ascii() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::string::bit_length

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of bit_length

```rust
fn bit_length() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: bit_length



## Module: btrim



## datafusion_functions::string::btrim

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of btrim

```rust
fn btrim() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: chr



## datafusion_functions::string::chr

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of chr

```rust
fn chr() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: common

Common utilities for implementing string functions



## datafusion_functions::string::concat

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of concat

```rust
fn concat() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: concat



## datafusion_functions::string::concat_ws

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of concat_ws

```rust
fn concat_ws() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: concat_ws



## Module: contains



## datafusion_functions::string::contains

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of contains

```rust
fn contains() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: ends_with



## datafusion_functions::string::ends_with

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of ends_with

```rust
fn ends_with() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: expr_fn



## datafusion_functions::string::functions

*Function*

Returns all DataFusion functions defined in this package

```rust
fn functions() -> Vec<std::sync::Arc<datafusion_expr::ScalarUDF>>
```



## Module: levenshtein



## datafusion_functions::string::levenshtein

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of levenshtein

```rust
fn levenshtein() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::string::lower

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of lower

```rust
fn lower() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: lower



## datafusion_functions::string::ltrim

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of ltrim

```rust
fn ltrim() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: ltrim



## datafusion_functions::string::octet_length

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of octet_length

```rust
fn octet_length() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: octet_length



## Module: overlay



## datafusion_functions::string::repeat

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of repeat

```rust
fn repeat() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: repeat



## Module: replace



## datafusion_functions::string::replace

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of replace

```rust
fn replace() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: rtrim



## datafusion_functions::string::rtrim

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of rtrim

```rust
fn rtrim() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: split_part



## datafusion_functions::string::split_part

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of split_part

```rust
fn split_part() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: starts_with



## datafusion_functions::string::starts_with

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of starts_with

```rust
fn starts_with() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: to_hex



## datafusion_functions::string::to_hex

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of to_hex

```rust
fn to_hex() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: upper



## datafusion_functions::string::upper

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of upper

```rust
fn upper() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::string::uuid

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of uuid

```rust
fn uuid() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: uuid



