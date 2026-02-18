**datafusion_functions > unicode > expr_fn**

# Module: unicode::expr_fn

## Contents

**Functions**

- [`char_length`](#char_length) - the number of characters in the `string`
- [`character_length`](#character_length) - the number of characters in the `string`
- [`find_in_set`](#find_in_set) - Returns a value in the range of 1 to N if the string `str` is in the string list `strlist` consisting of N substrings
- [`initcap`](#initcap) - converts the first letter of each word in `string` in uppercase and the remaining characters in lowercase
- [`instr`](#instr) - finds the position from where the `substring` matches the `string`
- [`left`](#left) - returns the first `n` characters in the `string`
- [`length`](#length) - the number of characters in the `string`
- [`lpad`](#lpad) - fill up a string to the length by prepending the characters
- [`position`](#position) - finds the position from where the `substring` matches the `string`
- [`reverse`](#reverse) - reverses the `string`
- [`right`](#right) - returns the last `n` characters in the `string`
- [`rpad`](#rpad) - fill up a string to the length by appending the characters
- [`strpos`](#strpos) - finds the position from where the `substring` matches the `string`
- [`substr`](#substr) - substring from the `position` to the end
- [`substr_index`](#substr_index) - Returns the substring from str before count occurrences of the delimiter
- [`substring`](#substring) - substring from the `position` with `length` characters
- [`translate`](#translate) - replaces the characters in `from` with the counterpart in `to`

---

## datafusion_functions::unicode::expr_fn::char_length

*Function*

the number of characters in the `string`

```rust
fn char_length(string: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::unicode::expr_fn::character_length

*Function*

the number of characters in the `string`

```rust
fn character_length(string: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::unicode::expr_fn::find_in_set

*Function*

Returns a value in the range of 1 to N if the string `str` is in the string list `strlist` consisting of N substrings

```rust
fn find_in_set(string: datafusion_expr::Expr, strlist: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::unicode::expr_fn::initcap

*Function*

converts the first letter of each word in `string` in uppercase and the remaining characters in lowercase

```rust
fn initcap(string: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::unicode::expr_fn::instr

*Function*

finds the position from where the `substring` matches the `string`

```rust
fn instr(string: datafusion_expr::Expr, substring: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::unicode::expr_fn::left

*Function*

returns the first `n` characters in the `string`

```rust
fn left(string: datafusion_expr::Expr, n: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::unicode::expr_fn::length

*Function*

the number of characters in the `string`

```rust
fn length(string: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::unicode::expr_fn::lpad

*Function*

fill up a string to the length by prepending the characters

```rust
fn lpad(args: Vec<datafusion_expr::Expr>) -> datafusion_expr::Expr
```



## datafusion_functions::unicode::expr_fn::position

*Function*

finds the position from where the `substring` matches the `string`

```rust
fn position(string: datafusion_expr::Expr, substring: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::unicode::expr_fn::reverse

*Function*

reverses the `string`

```rust
fn reverse(string: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::unicode::expr_fn::right

*Function*

returns the last `n` characters in the `string`

```rust
fn right(string: datafusion_expr::Expr, n: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::unicode::expr_fn::rpad

*Function*

fill up a string to the length by appending the characters

```rust
fn rpad(args: Vec<datafusion_expr::Expr>) -> datafusion_expr::Expr
```



## datafusion_functions::unicode::expr_fn::strpos

*Function*

finds the position from where the `substring` matches the `string`

```rust
fn strpos(string: datafusion_expr::Expr, substring: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::unicode::expr_fn::substr

*Function*

substring from the `position` to the end

```rust
fn substr(string: datafusion_expr::Expr, position: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::unicode::expr_fn::substr_index

*Function*

Returns the substring from str before count occurrences of the delimiter

```rust
fn substr_index(string: datafusion_expr::Expr, delimiter: datafusion_expr::Expr, count: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::unicode::expr_fn::substring

*Function*

substring from the `position` with `length` characters

```rust
fn substring(string: datafusion_expr::Expr, position: datafusion_expr::Expr, length: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::unicode::expr_fn::translate

*Function*

replaces the characters in `from` with the counterpart in `to`

```rust
fn translate(string: datafusion_expr::Expr, from: datafusion_expr::Expr, to: datafusion_expr::Expr) -> datafusion_expr::Expr
```



