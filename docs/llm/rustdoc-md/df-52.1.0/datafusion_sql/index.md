# datafusion_sql

This crate provides:

1. A SQL parser, [`DFParser`], that translates SQL query text into
   an abstract syntax tree (AST), [`Statement`].

2. A SQL query planner [`SqlToRel`] that creates [`LogicalPlan`]s
   from [`Statement`]s.

3. A SQL [`unparser`] that converts [`Expr`]s and [`LogicalPlan`]s
   into SQL query text.

[`DFParser`]: parser::DFParser
[`Statement`]: parser::Statement
[`SqlToRel`]: planner::SqlToRel
[`LogicalPlan`]: datafusion_expr::logical_plan::LogicalPlan
[`Expr`]: datafusion_expr::expr::Expr

## Modules

### [`datafusion_sql`](datafusion_sql.md)

*5 modules*

### [`parser`](parser.md)

*3 enums, 5 structs*

### [`planner`](planner.md)

*1 enum, 2 functions, 4 structs*

### [`resolve`](resolve.md)

*1 function*

### [`unparser`](unparser.md)

*1 struct, 3 modules*

### [`unparser::ast`](unparser/ast.md)

*1 enum, 8 structs*

### [`unparser::dialect`](unparser/dialect.md)

*1 trait, 1 type alias, 3 enums, 8 structs*

### [`unparser::expr`](unparser/expr.md)

*1 function*

### [`unparser::extension_unparser`](unparser/extension_unparser.md)

*1 trait, 2 enums*

### [`unparser::plan`](unparser/plan.md)

*1 function*

### [`utils`](utils.md)

*1 constant, 1 function*

