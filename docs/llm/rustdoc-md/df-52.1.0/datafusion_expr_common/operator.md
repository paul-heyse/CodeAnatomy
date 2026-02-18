**datafusion_expr_common > operator**

# Module: operator

## Contents

**Enums**

- [`Operator`](#operator) - Operators applied to expressions

---

## datafusion_expr_common::operator::Operator

*Enum*

Operators applied to expressions

**Variants:**
- `Eq` - Expressions are equal
- `NotEq` - Expressions are not equal
- `Lt` - Left side is smaller than right side
- `LtEq` - Left side is smaller or equal to right side
- `Gt` - Left side is greater than right side
- `GtEq` - Left side is greater or equal to right side
- `Plus` - Addition
- `Minus` - Subtraction
- `Multiply` - Multiplication operator, like `*`
- `Divide` - Division operator, like `/`
- `Modulo` - Remainder operator, like `%`
- `And` - Logical AND, like `&&`
- `Or` - Logical OR, like `||`
- `IsDistinctFrom` - `IS DISTINCT FROM` (see [`distinct`])
- `IsNotDistinctFrom` - `IS NOT DISTINCT FROM` (see [`not_distinct`])
- `RegexMatch` - Case sensitive regex match
- `RegexIMatch` - Case insensitive regex match
- `RegexNotMatch` - Case sensitive regex not match
- `RegexNotIMatch` - Case insensitive regex not match
- `LikeMatch` - Case sensitive pattern match
- `ILikeMatch` - Case insensitive pattern match
- `NotLikeMatch` - Case sensitive pattern not match
- `NotILikeMatch` - Case insensitive pattern not match
- `BitwiseAnd` - Bitwise and, like `&`
- `BitwiseOr` - Bitwise or, like `|`
- `BitwiseXor` - Bitwise xor, such as `^` in MySQL or `#` in PostgreSQL
- `BitwiseShiftRight` - Bitwise right, like `>>`
- `BitwiseShiftLeft` - Bitwise left, like `<<`
- `StringConcat` - String concat
- `AtArrow` - At arrow, like `@>`.
- `ArrowAt` - Arrow at, like `<@`.
- `Arrow` - Arrow, like `->`.
- `LongArrow` - Long arrow, like `->>`
- `HashArrow` - Hash arrow, like `#>`
- `HashLongArrow` - Hash long arrow, like `#>>`
- `AtAt` - At at, like `@@`
- `IntegerDivide` - Integer division operator, like `DIV` from MySQL or `//` from DuckDB
- `HashMinus` - Hash Minis, like `#-`
- `AtQuestion` - At question, like `@?`
- `Question` - Question, like `?`
- `QuestionAnd` - Question and, like `?&`
- `QuestionPipe` - Question pipe, like `?|`

**Methods:**

- `fn negate(self: &Self) -> Option<Operator>` - If the operator can be negated, return the negated operator
- `fn is_numerical_operators(self: &Self) -> bool` - Return true if the operator is a numerical operator.
- `fn supports_propagation(self: &Self) -> bool` - Return true if the comparison operator can be used in interval arithmetic and constraint
- `fn is_logic_operator(self: &Self) -> bool` - Return true if the operator is a logic operator.
- `fn swap(self: &Self) -> Option<Operator>` - Return the operator where swapping lhs and rhs wouldn't change the result.
- `fn precedence(self: &Self) -> u8` - Get the operator precedence
- `fn returns_null_on_null(self: &Self) -> bool` - Returns true if the `Expr::BinaryOperator` with this operator

**Traits:** Eq, Copy

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> Operator`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Operator) -> $crate::option::Option<$crate::cmp::Ordering>`
- **PartialEq**
  - `fn eq(self: &Self, other: &Operator) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Display**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`



