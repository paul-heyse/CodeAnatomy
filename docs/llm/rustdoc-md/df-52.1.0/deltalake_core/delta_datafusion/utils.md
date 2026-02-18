**deltalake_core > delta_datafusion > utils**

# Module: delta_datafusion::utils

## Contents

**Enums**

- [`Expression`](#expression) - Used to represent user input of either a Datafusion expression or string expression

---

## deltalake_core::delta_datafusion::utils::Expression

*Enum*

Used to represent user input of either a Datafusion expression or string expression

**Variants:**
- `DataFusion(datafusion::logical_expr::Expr)` - Datafusion Expression
- `String(String)` - String Expression



