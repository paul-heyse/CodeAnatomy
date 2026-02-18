**datafusion_expr > select_expr**

# Module: select_expr

## Contents

**Enums**

- [`SelectExpr`](#selectexpr) - Represents a SELECT expression in a SQL query.

---

## datafusion_expr::select_expr::SelectExpr

*Enum*

Represents a SELECT expression in a SQL query.

`SelectExpr` supports three types of expressions commonly found in the SELECT clause:

* Wildcard (`*`) - Selects all columns
* Qualified wildcard (`table.*`) - Selects all columns from a specific table
* Regular expression - Any other expression like columns, functions, literals etc.

This enum is typically used when you need to handle wildcards. After expanding `*` in the query,
you can use `Expr` for all other expressions.

# Examples

```
use datafusion_expr::col;
use datafusion_expr::expr::WildcardOptions;
use datafusion_expr::select_expr::SelectExpr;

// SELECT *
let wildcard = SelectExpr::Wildcard(WildcardOptions::default());

// SELECT mytable.*
let qualified =
    SelectExpr::QualifiedWildcard("mytable".into(), WildcardOptions::default());

// SELECT col1
let expr = SelectExpr::Expression(col("col1").into());
```

**Variants:**
- `Wildcard(crate::expr::WildcardOptions)` - Represents a wildcard (`*`) that selects all columns from all tables.
- `QualifiedWildcard(datafusion_common::TableReference, crate::expr::WildcardOptions)` - Represents a qualified wildcard (`table.*`) that selects all columns from a specific table.
- `Expression(crate::Expr)` - Represents any other valid SELECT expression like column references,

**Trait Implementations:**

- **Display**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> SelectExpr`
- **From**
  - `fn from(expr: Expr) -> Self`
- **From**
  - `fn from(value: Column) -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **From**
  - `fn from(value: (Option<&'a TableReference>, &'a FieldRef)) -> Self`



