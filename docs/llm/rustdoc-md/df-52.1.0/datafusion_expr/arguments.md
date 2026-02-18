**datafusion_expr > arguments**

# Module: arguments

## Contents

**Structs**

- [`ArgumentName`](#argumentname) - Represents a named function argument with its original case and quote information.

**Functions**

- [`resolve_function_arguments`](#resolve_function_arguments) - Resolves function arguments, handling named and positional notation.

---

## datafusion_expr::arguments::ArgumentName

*Struct*

Represents a named function argument with its original case and quote information.

This struct preserves whether an identifier was quoted in the SQL, which determines
whether case-sensitive or case-insensitive matching should be used per SQL standards.

**Fields:**
- `value: String` - The argument name in its original case as it appeared in the SQL
- `is_quoted: bool` - Whether the identifier was quoted (e.g., "STR" vs STR)

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> ArgumentName`
- **PartialEq**
  - `fn eq(self: &Self, other: &ArgumentName) -> bool`



## datafusion_expr::arguments::resolve_function_arguments

*Function*

Resolves function arguments, handling named and positional notation.

This function validates and reorders arguments to match the function's parameter names
when named arguments are used.

# Rules
- All positional arguments must come before named arguments
- Named arguments can be in any order after positional arguments
- Parameter names follow SQL identifier rules: unquoted names are case-insensitive
  (normalized to lowercase), quoted names are case-sensitive
- No duplicate parameter names allowed

# Arguments
* `param_names` - The function's parameter names in order
* `args` - The argument expressions
* `arg_names` - Optional parameter name for each argument

# Returns
A vector of expressions in the correct order matching the parameter names

# Examples
```text
Given parameters ["a", "b", "c"]
And call: func(10, c => 30, b => 20)
Returns: [Expr(10), Expr(20), Expr(30)]
```

```rust
fn resolve_function_arguments(param_names: &[String], args: Vec<crate::Expr>, arg_names: Vec<Option<ArgumentName>>) -> datafusion_common::Result<Vec<crate::Expr>>
```



