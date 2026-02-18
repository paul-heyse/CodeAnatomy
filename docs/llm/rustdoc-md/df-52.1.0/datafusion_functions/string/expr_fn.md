**datafusion_functions > string > expr_fn**

# Module: string::expr_fn

## Contents

**Functions**

- [`ascii`](#ascii) - Returns the numeric code of the first character of the argument.
- [`bit_length`](#bit_length) - Returns the number of bits in the `string`
- [`btrim`](#btrim) - Removes all characters, spaces by default, from both sides of a string
- [`chr`](#chr) - Converts the Unicode code point to a UTF8 character
- [`concat`](#concat) - Concatenates the text representations of all the arguments. NULL arguments are ignored
- [`concat_ws`](#concat_ws) - Concatenates all but the first argument, with separators. The first argument is used as the separator string, and should not be NULL. Other NULL arguments are ignored.
- [`contains`](#contains) - Return true if `search_string` is found within `string`.
- [`ends_with`](#ends_with) - Returns true if the `string` ends with the `suffix`, false otherwise.
- [`levenshtein`](#levenshtein) - Returns the Levenshtein distance between the two given strings
- [`lower`](#lower) - Converts a string to lowercase.
- [`ltrim`](#ltrim) - Removes all characters, spaces by default, from the beginning of a string
- [`octet_length`](#octet_length) - returns the number of bytes of a string
- [`repeat`](#repeat) - Repeats the `string` to `n` times
- [`replace`](#replace) - Replaces all occurrences of `from` with `to` in the `string`
- [`rtrim`](#rtrim) - Removes all characters, spaces by default, from the end of a string
- [`split_part`](#split_part) - Splits a string based on a delimiter and picks out the desired field based on the index.
- [`starts_with`](#starts_with) - Returns true if string starts with prefix.
- [`to_hex`](#to_hex) - Converts an integer to a hexadecimal string.
- [`trim`](#trim) - Removes all characters, spaces by default, from both sides of a string
- [`upper`](#upper) - Converts a string to uppercase.
- [`uuid`](#uuid) - returns uuid v4 as a string value

---

## datafusion_functions::string::expr_fn::ascii

*Function*

Returns the numeric code of the first character of the argument.

```rust
fn ascii(arg1: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::string::expr_fn::bit_length

*Function*

Returns the number of bits in the `string`

```rust
fn bit_length(arg1: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::string::expr_fn::btrim

*Function*

Removes all characters, spaces by default, from both sides of a string

```rust
fn btrim(args: Vec<datafusion_expr::Expr>) -> datafusion_expr::Expr
```



## datafusion_functions::string::expr_fn::chr

*Function*

Converts the Unicode code point to a UTF8 character

```rust
fn chr(arg1: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::string::expr_fn::concat

*Function*

Concatenates the text representations of all the arguments. NULL arguments are ignored

```rust
fn concat(args: Vec<datafusion_expr::Expr>) -> datafusion_expr::Expr
```



## datafusion_functions::string::expr_fn::concat_ws

*Function*

Concatenates all but the first argument, with separators. The first argument is used as the separator string, and should not be NULL. Other NULL arguments are ignored.

```rust
fn concat_ws(delimiter: datafusion_expr::Expr, args: Vec<datafusion_expr::Expr>) -> datafusion_expr::Expr
```



## datafusion_functions::string::expr_fn::contains

*Function*

Return true if `search_string` is found within `string`.

```rust
fn contains(string: datafusion_expr::Expr, search_string: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::string::expr_fn::ends_with

*Function*

Returns true if the `string` ends with the `suffix`, false otherwise.

```rust
fn ends_with(string: datafusion_expr::Expr, suffix: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::string::expr_fn::levenshtein

*Function*

Returns the Levenshtein distance between the two given strings

```rust
fn levenshtein(arg1: datafusion_expr::Expr, arg2: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::string::expr_fn::lower

*Function*

Converts a string to lowercase.

```rust
fn lower(arg1: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::string::expr_fn::ltrim

*Function*

Removes all characters, spaces by default, from the beginning of a string

```rust
fn ltrim(args: Vec<datafusion_expr::Expr>) -> datafusion_expr::Expr
```



## datafusion_functions::string::expr_fn::octet_length

*Function*

returns the number of bytes of a string

```rust
fn octet_length(args: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::string::expr_fn::repeat

*Function*

Repeats the `string` to `n` times

```rust
fn repeat(string: datafusion_expr::Expr, n: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::string::expr_fn::replace

*Function*

Replaces all occurrences of `from` with `to` in the `string`

```rust
fn replace(string: datafusion_expr::Expr, from: datafusion_expr::Expr, to: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::string::expr_fn::rtrim

*Function*

Removes all characters, spaces by default, from the end of a string

```rust
fn rtrim(args: Vec<datafusion_expr::Expr>) -> datafusion_expr::Expr
```



## datafusion_functions::string::expr_fn::split_part

*Function*

Splits a string based on a delimiter and picks out the desired field based on the index.

```rust
fn split_part(string: datafusion_expr::Expr, delimiter: datafusion_expr::Expr, index: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::string::expr_fn::starts_with

*Function*

Returns true if string starts with prefix.

```rust
fn starts_with(arg1: datafusion_expr::Expr, arg2: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::string::expr_fn::to_hex

*Function*

Converts an integer to a hexadecimal string.

```rust
fn to_hex(arg1: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::string::expr_fn::trim

*Function*

Removes all characters, spaces by default, from both sides of a string

```rust
fn trim(args: Vec<datafusion_expr::Expr>) -> datafusion_expr::Expr
```



## datafusion_functions::string::expr_fn::upper

*Function*

Converts a string to uppercase.

```rust
fn upper(arg1: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::string::expr_fn::uuid

*Function*

returns uuid v4 as a string value

```rust
fn uuid() -> datafusion_expr::Expr
```



