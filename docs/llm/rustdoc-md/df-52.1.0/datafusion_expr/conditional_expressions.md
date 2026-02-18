**datafusion_expr > conditional_expressions**

# Module: conditional_expressions

## Contents

**Structs**

- [`CaseBuilder`](#casebuilder) - Helper struct for building [Expr::Case]

---

## datafusion_expr::conditional_expressions::CaseBuilder

*Struct*

Helper struct for building [Expr::Case]

**Methods:**

- `fn new(expr: Option<Box<Expr>>, when_expr: Vec<Expr>, then_expr: Vec<Expr>, else_expr: Option<Box<Expr>>) -> Self`
- `fn when(self: & mut Self, when: Expr, then: Expr) -> CaseBuilder`
- `fn otherwise(self: & mut Self, else_expr: Expr) -> Result<Expr>`
- `fn end(self: &Self) -> Result<Expr>`

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> CaseBuilder`



