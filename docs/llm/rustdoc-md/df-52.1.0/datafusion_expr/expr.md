**datafusion_expr > expr**

# Module: expr

## Contents

**Structs**

- [`AggregateFunction`](#aggregatefunction) - Aggregate function
- [`AggregateFunctionParams`](#aggregatefunctionparams)
- [`Alias`](#alias) - Alias expression
- [`Between`](#between) - BETWEEN expression
- [`BinaryExpr`](#binaryexpr) - Binary expression
- [`Case`](#case) - CASE expression
- [`Cast`](#cast) - Cast expression
- [`Exists`](#exists) - EXISTS expression
- [`ExprListDisplay`](#exprlistdisplay) - Formats a list of `&Expr` with a custom separator using SQL display format
- [`InList`](#inlist) - InList expression
- [`InSubquery`](#insubquery) - IN subquery
- [`Like`](#like) - LIKE expression
- [`Placeholder`](#placeholder) - Placeholder, representing bind parameter values such as `$1` or `$name`.
- [`PlannedReplaceSelectItem`](#plannedreplaceselectitem) - The planned expressions for `REPLACE`
- [`ScalarFunction`](#scalarfunction) - Invoke a [`ScalarUDF`] with a set of arguments
- [`Sort`](#sort) - SORT expression
- [`TryCast`](#trycast) - TryCast Expression
- [`Unnest`](#unnest) - UNNEST expression.
- [`WildcardOptions`](#wildcardoptions) - Additional options for wildcards, e.g. Snowflake `EXCLUDE`/`RENAME` and Bigquery `EXCEPT`.
- [`WindowFunction`](#windowfunction) - Window function
- [`WindowFunctionParams`](#windowfunctionparams)

**Enums**

- [`Expr`](#expr) -  Represents logical expressions such as `A + 1`, or `CAST(c1 AS int)`.
- [`GetFieldAccess`](#getfieldaccess) - Access a sub field of a nested type, such as `Field` or `List`
- [`GroupingSet`](#groupingset) - Grouping sets
- [`NullTreatment`](#nulltreatment)
- [`WindowFunctionDefinition`](#windowfunctiondefinition) - A function used as a SQL window function

**Functions**

- [`intersect_metadata_for_union`](#intersect_metadata_for_union) - Intersects multiple metadata instances for UNION operations.
- [`physical_name`](#physical_name) - The name of the column (field) that this `Expr` will produce in the physical plan.
- [`schema_name_from_exprs`](#schema_name_from_exprs) - Get schema_name for Vector of expressions
- [`schema_name_from_sorts`](#schema_name_from_sorts)

**Constants**

- [`OUTER_REFERENCE_COLUMN_PREFIX`](#outer_reference_column_prefix)
- [`UNNEST_COLUMN_PREFIX`](#unnest_column_prefix)

**Type Aliases**

- [`SchemaFieldMetadata`](#schemafieldmetadata) - The metadata used in [`Field::metadata`].

---

## datafusion_expr::expr::AggregateFunction

*Struct*

Aggregate function

See also  [`ExprFunctionExt`] to set these fields on `Expr`

[`ExprFunctionExt`]: crate::expr_fn::ExprFunctionExt

**Fields:**
- `func: std::sync::Arc<crate::AggregateUDF>` - Name of the function
- `params: AggregateFunctionParams`

**Methods:**

- `fn new_udf(func: Arc<AggregateUDF>, args: Vec<Expr>, distinct: bool, filter: Option<Box<Expr>>, order_by: Vec<Sort>, null_treatment: Option<NullTreatment>) -> Self` - Create a new AggregateFunction expression with a user-defined function (UDF)

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> AggregateFunction`
- **PartialEq**
  - `fn eq(self: &Self, other: &AggregateFunction) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &AggregateFunction) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_expr::expr::AggregateFunctionParams

*Struct*

**Fields:**
- `args: Vec<Expr>`
- `distinct: bool` - Whether this is a DISTINCT aggregation or not
- `filter: Option<Box<Expr>>` - Optional filter
- `order_by: Vec<Sort>` - Optional ordering
- `null_treatment: Option<NullTreatment>`

**Traits:** Eq

**Trait Implementations:**

- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &AggregateFunctionParams) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> AggregateFunctionParams`
- **PartialEq**
  - `fn eq(self: &Self, other: &AggregateFunctionParams) -> bool`



## datafusion_expr::expr::Alias

*Struct*

Alias expression

**Fields:**
- `expr: Box<Expr>`
- `relation: Option<datafusion_common::TableReference>`
- `name: String`
- `metadata: Option<FieldMetadata>`

**Methods:**

- `fn new<impl Into<TableReference>, impl Into<String>>(expr: Expr, relation: Option<impl Trait>, name: impl Trait) -> Self` - Create an alias with an optional schema/field qualifier.
- `fn with_metadata(self: Self, metadata: Option<FieldMetadata>) -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Hash**
  - `fn hash<H>(self: &Self, state: & mut H)`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Self) -> Option<Ordering>`
- **Clone**
  - `fn clone(self: &Self) -> Alias`
- **PartialEq**
  - `fn eq(self: &Self, other: &Alias) -> bool`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr::expr::Between

*Struct*

BETWEEN expression

**Fields:**
- `expr: Box<Expr>` - The value to compare
- `negated: bool` - Whether the expression is negated
- `low: Box<Expr>` - The low end of the range
- `high: Box<Expr>` - The high end of the range

**Methods:**

- `fn new(expr: Box<Expr>, negated: bool, low: Box<Expr>, high: Box<Expr>) -> Self` - Create a new Between expression

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Between`
- **PartialEq**
  - `fn eq(self: &Self, other: &Between) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Between) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_expr::expr::BinaryExpr

*Struct*

Binary expression

**Fields:**
- `left: Box<Expr>` - Left-hand side of the expression
- `op: crate::Operator` - The comparison operator
- `right: Box<Expr>` - Right-hand side of the expression

**Methods:**

- `fn new(left: Box<Expr>, op: Operator, right: Box<Expr>) -> Self` - Create a new binary expression

**Traits:** Eq

**Trait Implementations:**

- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> BinaryExpr`
- **PartialEq**
  - `fn eq(self: &Self, other: &BinaryExpr) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &BinaryExpr) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Display**
  - `fn fmt(self: &Self, f: & mut Formatter) -> fmt::Result`



## datafusion_expr::expr::Case

*Struct*

CASE expression

The CASE expression is similar to a series of nested if/else and there are two forms that
can be used. The first form consists of a series of boolean "when" expressions with
corresponding "then" expressions, and an optional "else" expression.

```text
CASE WHEN condition THEN result
     [WHEN ...]
     [ELSE result]
END
```

The second form uses a base expression and then a series of "when" clauses that match on a
literal value.

```text
CASE expression
    WHEN value THEN result
    [WHEN ...]
    [ELSE result]
END
```

**Fields:**
- `expr: Option<Box<Expr>>` - Optional base expression that can be compared to literal values in the "when" expressions
- `when_then_expr: Vec<(Box<Expr>, Box<Expr>)>` - One or more when/then expressions
- `else_expr: Option<Box<Expr>>` - Optional "else" expression

**Methods:**

- `fn new(expr: Option<Box<Expr>>, when_then_expr: Vec<(Box<Expr>, Box<Expr>)>, else_expr: Option<Box<Expr>>) -> Self` - Create a new Case expression

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &Case) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Case) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Clone**
  - `fn clone(self: &Self) -> Case`



## datafusion_expr::expr::Cast

*Struct*

Cast expression

**Fields:**
- `expr: Box<Expr>` - The expression being cast
- `data_type: arrow::datatypes::DataType` - The `DataType` the expression will yield

**Methods:**

- `fn new(expr: Box<Expr>, data_type: DataType) -> Self` - Create a new Cast expression

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> Cast`
- **PartialEq**
  - `fn eq(self: &Self, other: &Cast) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Cast) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr::expr::Exists

*Struct*

EXISTS expression

**Fields:**
- `subquery: crate::logical_plan::Subquery` - Subquery that will produce a single column of data
- `negated: bool` - Whether the expression is negated

**Methods:**

- `fn new(subquery: Subquery, negated: bool) -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Exists`
- **PartialEq**
  - `fn eq(self: &Self, other: &Exists) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Exists) -> $crate::option::Option<$crate::cmp::Ordering>`



## datafusion_expr::expr::Expr

*Enum*

 Represents logical expressions such as `A + 1`, or `CAST(c1 AS int)`.

 For example the expression `A + 1` will be represented as

```text
  BinaryExpr {
    left: Expr::Column("A"),
    op: Operator::Plus,
    right: Expr::Literal(ScalarValue::Int32(Some(1)), None)
 }
 ```

 # Creating Expressions

 `Expr`s can be created directly, but it is often easier and less verbose to
 use the fluent APIs in [`crate::expr_fn`] such as [`col`] and [`lit`], or
 methods such as [`Expr::alias`], [`Expr::cast_to`], and [`Expr::Like`]).

 See also [`ExprFunctionExt`] for creating aggregate and window functions.

 [`ExprFunctionExt`]: crate::expr_fn::ExprFunctionExt

 # Printing Expressions

 You can print `Expr`s using the `Debug` trait, `Display` trait, or
 [`Self::human_display`]. See the [examples](#examples-displaying-exprs) below.

 If you need  SQL to pass to other systems, consider using [`Unparser`].

 [`Unparser`]: https://docs.rs/datafusion/latest/datafusion/sql/unparser/struct.Unparser.html

 # Schema Access

 See [`ExprSchemable::get_type`] to access the [`DataType`] and nullability
 of an `Expr`.

 # Visiting and Rewriting `Expr`s

 The `Expr` struct implements the [`TreeNode`] trait for walking and
 rewriting expressions. For example [`TreeNode::apply`] recursively visits an
 `Expr` and [`TreeNode::transform`] can be used to rewrite an expression. See
 the examples below and [`TreeNode`] for more information.

 # Examples: Creating and Using `Expr`s

 ## Column References and Literals

 [`Expr::Column`] refer to the values of columns and are often created with
 the [`col`] function. For example to create an expression `c1` referring to
 column named "c1":

 [`col`]: crate::expr_fn::col

 ```
 # use datafusion_common::Column;
 # use datafusion_expr::{lit, col, Expr};
 let expr = col("c1");
 assert_eq!(expr, Expr::Column(Column::from_name("c1")));
 ```

 [`Expr::Literal`] refer to literal, or constant, values. These are created
 with the [`lit`] function. For example to create an expression `42`:

 [`lit`]: crate::lit

 ```
 # use datafusion_common::{Column, ScalarValue};
 # use datafusion_expr::{lit, col, Expr};
 // All literals are strongly typed in DataFusion. To make an `i64` 42:
 let expr = lit(42i64);
 assert_eq!(expr, Expr::Literal(ScalarValue::Int64(Some(42)), None));
 assert_eq!(expr, Expr::Literal(ScalarValue::Int64(Some(42)), None));
 // To make a (typed) NULL:
 let expr = Expr::Literal(ScalarValue::Int64(None), None);
 // to make an (untyped) NULL (the optimizer will coerce this to the correct type):
 let expr = lit(ScalarValue::Null);
 ```

 ## Binary Expressions

 Exprs implement traits that allow easy to understand construction of more
 complex expressions. For example, to create `c1 + c2` to add columns "c1" and
 "c2" together

 ```
 # use datafusion_expr::{lit, col, Operator, Expr};
 // Use the `+` operator to add two columns together
 let expr = col("c1") + col("c2");
 assert!(matches!(expr, Expr::BinaryExpr { .. }));
 if let Expr::BinaryExpr(binary_expr) = expr {
     assert_eq!(*binary_expr.left, col("c1"));
     assert_eq!(*binary_expr.right, col("c2"));
     assert_eq!(binary_expr.op, Operator::Plus);
 }
 ```

 The expression `c1 = 42` to compares the value in column "c1" to the
 literal value `42`:

 ```
 # use datafusion_common::ScalarValue;
 # use datafusion_expr::{lit, col, Operator, Expr};
 let expr = col("c1").eq(lit(42_i32));
 assert!(matches!(expr, Expr::BinaryExpr { .. }));
 if let Expr::BinaryExpr(binary_expr) = expr {
     assert_eq!(*binary_expr.left, col("c1"));
     let scalar = ScalarValue::Int32(Some(42));
     assert_eq!(*binary_expr.right, Expr::Literal(scalar, None));
     assert_eq!(binary_expr.op, Operator::Eq);
 }
 ```

 Here is how to implement the equivalent of `SELECT *` to select all
 [`Expr::Column`] from a [`DFSchema`]'s columns:

 ```
 # use arrow::datatypes::{DataType, Field, Schema};
 # use datafusion_common::{DFSchema, Column};
 # use datafusion_expr::Expr;
 // Create a schema c1(int, c2 float)
 let arrow_schema = Schema::new(vec![
     Field::new("c1", DataType::Int32, false),
     Field::new("c2", DataType::Float64, false),
 ]);
 // DFSchema is a an Arrow schema with optional relation name
 let df_schema = DFSchema::try_from_qualified_schema("t1", &arrow_schema).unwrap();

 // Form Vec<Expr> with an expression for each column in the schema
 let exprs: Vec<_> = df_schema.iter().map(Expr::from).collect();

 assert_eq!(
     exprs,
     vec![
         Expr::from(Column::from_qualified_name("t1.c1")),
         Expr::from(Column::from_qualified_name("t1.c2")),
     ]
 );
 ```

 # Examples: Displaying `Exprs`

 There are three ways to print an `Expr` depending on the usecase.

 ## Use `Debug` trait

 Following Rust conventions, the `Debug` implementation prints out the
 internal structure of the expression, which is useful for debugging.

 ```
 # use datafusion_expr::{lit, col};
 let expr = col("c1") + lit(42);
 assert_eq!(format!("{expr:?}"), "BinaryExpr(BinaryExpr { left: Column(Column { relation: None, name: \"c1\" }), op: Plus, right: Literal(Int32(42), None) })");
 ```

 ## Use the `Display` trait  (detailed expression)

 The `Display` implementation prints out the expression in a SQL-like form,
 but has additional details such as the data type of literals. This is useful
 for understanding the expression in more detail and is used for the low level
 [`ExplainFormat::Indent`] explain plan format.

 [`ExplainFormat::Indent`]: crate::logical_plan::ExplainFormat::Indent

 ```
 # use datafusion_expr::{lit, col};
 let expr = col("c1") + lit(42);
 assert_eq!(format!("{expr}"), "c1 + Int32(42)");
 ```

 ## Use [`Self::human_display`] (human readable)

 [`Self::human_display`]  prints out the expression in a SQL-like form, optimized
 for human consumption by end users. It is used for the
 [`ExplainFormat::Tree`] explain plan format.

 [`ExplainFormat::Tree`]: crate::logical_plan::ExplainFormat::Tree

```
 # use datafusion_expr::{lit, col};
 let expr = col("c1") + lit(42);
 assert_eq!(format!("{}", expr.human_display()), "c1 + 42");
 ```

 # Examples: Visiting and Rewriting `Expr`s

 Here is an example that finds all literals in an `Expr` tree:
 ```
 # use std::collections::{HashSet};
 use datafusion_common::ScalarValue;
 # use datafusion_expr::{col, Expr, lit};
 use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
 // Expression a = 5 AND b = 6
 let expr = col("a").eq(lit(5)) & col("b").eq(lit(6));
 // find all literals in a HashMap
 let mut scalars = HashSet::new();
 // apply recursively visits all nodes in the expression tree
 expr.apply(|e| {
     if let Expr::Literal(scalar, _) = e {
         scalars.insert(scalar);
     }
     // The return value controls whether to continue visiting the tree
     Ok(TreeNodeRecursion::Continue)
 })
 .unwrap();
 // All subtrees have been visited and literals found
 assert_eq!(scalars.len(), 2);
 assert!(scalars.contains(&ScalarValue::Int32(Some(5))));
 assert!(scalars.contains(&ScalarValue::Int32(Some(6))));
 ```

 Rewrite an expression, replacing references to column "a" in an
 to the literal `42`:

  ```
 # use datafusion_common::tree_node::{Transformed, TreeNode};
 # use datafusion_expr::{col, Expr, lit};
 // expression a = 5 AND b = 6
 let expr = col("a").eq(lit(5)).and(col("b").eq(lit(6)));
 // rewrite all references to column "a" to the literal 42
 let rewritten = expr.transform(|e| {
   if let Expr::Column(c) = &e {
     if &c.name == "a" {
       // return Transformed::yes to indicate the node was changed
       return Ok(Transformed::yes(lit(42)))
     }
   }
   // return Transformed::no to indicate the node was not changed
   Ok(Transformed::no(e))
 }).unwrap();
 // The expression has been rewritten
 assert!(rewritten.transformed);
 // to 42 = 5 AND b = 6
 assert_eq!(rewritten.data, lit(42).eq(lit(5)).and(col("b").eq(lit(6))));

**Variants:**
- `Alias(Alias)` - An expression with a specific name.
- `Column(datafusion_common::Column)` - A named reference to a qualified field in a schema.
- `ScalarVariable(arrow::datatypes::FieldRef, Vec<String>)` - A named reference to a variable in a registry.
- `Literal(datafusion_common::ScalarValue, Option<FieldMetadata>)` - A constant value along with associated [`FieldMetadata`].
- `BinaryExpr(BinaryExpr)` - A binary expression such as "age > 21"
- `Like(Like)` - LIKE expression
- `SimilarTo(Like)` - LIKE expression that uses regular expressions
- `Not(Box<Expr>)` - Negation of an expression. The expression's type must be a boolean to make sense.
- `IsNotNull(Box<Expr>)` - True if argument is not NULL, false otherwise. This expression itself is never NULL.
- `IsNull(Box<Expr>)` - True if argument is NULL, false otherwise. This expression itself is never NULL.
- `IsTrue(Box<Expr>)` - True if argument is true, false otherwise. This expression itself is never NULL.
- `IsFalse(Box<Expr>)` - True if argument is  false, false otherwise. This expression itself is never NULL.
- `IsUnknown(Box<Expr>)` - True if argument is NULL, false otherwise. This expression itself is never NULL.
- `IsNotTrue(Box<Expr>)` - True if argument is FALSE or NULL, false otherwise. This expression itself is never NULL.
- `IsNotFalse(Box<Expr>)` - True if argument is TRUE OR NULL, false otherwise. This expression itself is never NULL.
- `IsNotUnknown(Box<Expr>)` - True if argument is TRUE or FALSE, false otherwise. This expression itself is never NULL.
- `Negative(Box<Expr>)` - arithmetic negation of an expression, the operand must be of a signed numeric data type
- `Between(Between)` - Whether an expression is between a given range.
- `Case(Case)` - A CASE expression (see docs on [`Case`])
- `Cast(Cast)` - Casts the expression to a given type and will return a runtime error if the expression cannot be cast.
- `TryCast(TryCast)` - Casts the expression to a given type and will return a null value if the expression cannot be cast.
- `ScalarFunction(ScalarFunction)` - Call a scalar function with a set of arguments.
- `AggregateFunction(AggregateFunction)` - Calls an aggregate function with arguments, and optional
- `WindowFunction(Box<WindowFunction>)` - Call a window function with a set of arguments.
- `InList(InList)` - Returns whether the list contains the expr value.
- `Exists(Exists)` - EXISTS subquery
- `InSubquery(InSubquery)` - IN subquery
- `ScalarSubquery(crate::logical_plan::Subquery)` - Scalar subquery
- `Wildcard{ qualifier: Option<datafusion_common::TableReference>, options: Box<WildcardOptions> }` - Represents a reference to all available fields in a specific schema,
- `GroupingSet(GroupingSet)` - List of grouping set expressions. Only valid in the context of an aggregate
- `Placeholder(Placeholder)` - A place holder for parameters in a prepared statement
- `OuterReferenceColumn(arrow::datatypes::FieldRef, datafusion_common::Column)` - A placeholder which holds a reference to a qualified field
- `Unnest(Unnest)` - Unnest expression

**Methods:**

- `fn schema_name(self: &Self) -> impl Trait` - The name of the column (field) that this `Expr` will produce.
- `fn human_display(self: &Self) -> impl Trait` - Human readable display formatting for this expression.
- `fn qualified_name(self: &Self) -> (Option<TableReference>, String)` - Returns the qualifier and the schema name of this expression.
- `fn variant_name(self: &Self) -> &str` - Return String representation of the variant represented by `self`
- `fn eq(self: Self, other: Expr) -> Expr` - Return `self == other`
- `fn not_eq(self: Self, other: Expr) -> Expr` - Return `self != other`
- `fn gt(self: Self, other: Expr) -> Expr` - Return `self > other`
- `fn gt_eq(self: Self, other: Expr) -> Expr` - Return `self >= other`
- `fn lt(self: Self, other: Expr) -> Expr` - Return `self < other`
- `fn lt_eq(self: Self, other: Expr) -> Expr` - Return `self <= other`
- `fn and(self: Self, other: Expr) -> Expr` - Return `self && other`
- `fn or(self: Self, other: Expr) -> Expr` - Return `self || other`
- `fn like(self: Self, other: Expr) -> Expr` - Return `self LIKE other`
- `fn not_like(self: Self, other: Expr) -> Expr` - Return `self NOT LIKE other`
- `fn ilike(self: Self, other: Expr) -> Expr` - Return `self ILIKE other`
- `fn not_ilike(self: Self, other: Expr) -> Expr` - Return `self NOT ILIKE other`
- `fn name_for_alias(self: &Self) -> Result<String>` - Return the name to use for the specific Expr
- `fn alias_if_changed(self: Self, original_name: String) -> Result<Expr>` - Ensure `expr` has the name as `original_name` by adding an
- `fn alias<impl Into<String>>(self: Self, name: impl Trait) -> Expr` - Return `self AS name` alias expression
- `fn alias_with_metadata<impl Into<String>>(self: Self, name: impl Trait, metadata: Option<FieldMetadata>) -> Expr` - Return `self AS name` alias expression with metadata
- `fn alias_qualified<impl Into<TableReference>, impl Into<String>>(self: Self, relation: Option<impl Trait>, name: impl Trait) -> Expr` - Return `self AS name` alias expression with a specific qualifier
- `fn alias_qualified_with_metadata<impl Into<TableReference>, impl Into<String>>(self: Self, relation: Option<impl Trait>, name: impl Trait, metadata: Option<FieldMetadata>) -> Expr` - Return `self AS name` alias expression with a specific qualifier and metadata
- `fn unalias(self: Self) -> Expr` - Remove an alias from an expression if one exists.
- `fn unalias_nested(self: Self) -> Transformed<Expr>` - Recursively removed potentially multiple aliases from an expression.
- `fn in_list(self: Self, list: Vec<Expr>, negated: bool) -> Expr` - Return `self IN <list>` if `negated` is false, otherwise
- `fn is_null(self: Self) -> Expr` - Return `IsNull(Box(self))
- `fn is_not_null(self: Self) -> Expr` - Return `IsNotNull(Box(self))
- `fn sort(self: Self, asc: bool, nulls_first: bool) -> Sort` - Create a sort configuration from an existing expression.
- `fn is_true(self: Self) -> Expr` - Return `IsTrue(Box(self))`
- `fn is_not_true(self: Self) -> Expr` - Return `IsNotTrue(Box(self))`
- `fn is_false(self: Self) -> Expr` - Return `IsFalse(Box(self))`
- `fn is_not_false(self: Self) -> Expr` - Return `IsNotFalse(Box(self))`
- `fn is_unknown(self: Self) -> Expr` - Return `IsUnknown(Box(self))`
- `fn is_not_unknown(self: Self) -> Expr` - Return `IsNotUnknown(Box(self))`
- `fn between(self: Self, low: Expr, high: Expr) -> Expr` - return `self BETWEEN low AND high`
- `fn not_between(self: Self, low: Expr, high: Expr) -> Expr` - Return `self NOT BETWEEN low AND high`
- `fn try_as_col(self: &Self) -> Option<&Column>` - Return a reference to the inner `Column` if any
- `fn get_as_join_column(self: &Self) -> Option<&Column>` - Returns the inner `Column` if any. This is a specialized version of
- `fn column_refs(self: &Self) -> HashSet<&Column>` - Return all references to columns in this expression.
- `fn add_column_refs<'a>(self: &'a Self, set: & mut HashSet<&'a Column>)` - Adds references to all columns in this expression to the set
- `fn column_refs_counts(self: &Self) -> HashMap<&Column, usize>` - Return all references to columns and their occurrence counts in the expression.
- `fn add_column_ref_counts<'a>(self: &'a Self, map: & mut HashMap<&'a Column, usize>)` - Adds references to all columns and their occurrence counts in the expression to
- `fn any_column_refs(self: &Self) -> bool` - Returns true if there are any column references in this Expr
- `fn contains_outer(self: &Self) -> bool` - Return true if the expression contains out reference(correlated) expressions.
- `fn is_volatile_node(self: &Self) -> bool` - Returns true if the expression node is volatile, i.e. whether it can return
- `fn is_volatile(self: &Self) -> bool` - Returns true if the expression is volatile, i.e. whether it can return different
- `fn infer_placeholder_types(self: Self, schema: &DFSchema) -> Result<(Expr, bool)>` - Recursively find all [`Expr::Placeholder`] expressions, and
- `fn short_circuits(self: &Self) -> bool` - Returns true if some of this `exprs` subexpressions may not be evaluated
- `fn spans(self: &Self) -> Option<&Spans>` - Returns a reference to the set of locations in the SQL query where this
- `fn as_literal(self: &Self) -> Option<&ScalarValue>` - Check if the Expr is literal and get the literal value if it is.

**Traits:** Eq

**Trait Implementations:**

- **Normalizeable**
  - `fn can_normalize(self: &Self) -> bool`
- **BitXor**
  - `fn bitxor(self: Self, rhs: Self) -> Self`
- **Clone**
  - `fn clone(self: &Self) -> Expr`
- **From**
  - `fn from(value: WindowFunction) -> Self`
- **NormalizeEq**
  - `fn normalize_eq(self: &Self, other: &Self) -> bool`
- **PartialEq**
  - `fn eq(self: &Self, other: &Expr) -> bool`
- **From**
  - `fn from(value: ScalarAndMetadata) -> Self`
- **HashNode**
  - `fn hash_node<H>(self: &Self, state: & mut H)` - As it is pretty easy to forget changing this method when `Expr` changes the
- **Display**
  - `fn fmt(self: &Self, f: & mut Formatter) -> fmt::Result`
- **Shr**
  - `fn shr(self: Self, rhs: Self) -> <Self as >::Output`
- **Default**
  - `fn default() -> Self`
- **Add**
  - `fn add(self: Self, rhs: Self) -> Self`
- **BitOr**
  - `fn bitor(self: Self, rhs: Self) -> Self`
- **Sub**
  - `fn sub(self: Self, rhs: Self) -> Self`
- **ExprFunctionExt**
  - `fn order_by(self: Self, order_by: Vec<Sort>) -> ExprFuncBuilder`
  - `fn filter(self: Self, filter: Expr) -> ExprFuncBuilder`
  - `fn distinct(self: Self) -> ExprFuncBuilder`
  - `fn null_treatment<impl Into<Option<NullTreatment>>>(self: Self, null_treatment: impl Trait) -> ExprFuncBuilder`
  - `fn partition_by(self: Self, partition_by: Vec<Expr>) -> ExprFuncBuilder`
  - `fn window_frame(self: Self, window_frame: WindowFrame) -> ExprFuncBuilder`
- **Mul**
  - `fn mul(self: Self, rhs: Self) -> Self`
- **Div**
  - `fn div(self: Self, rhs: Self) -> Self`
- **Shl**
  - `fn shl(self: Self, rhs: Self) -> <Self as >::Output`
- **ExprSchemable**
  - `fn get_type(self: &Self, schema: &dyn ExprSchema) -> Result<DataType>` - Returns the [arrow::datatypes::DataType] of the expression
  - `fn nullable(self: &Self, input_schema: &dyn ExprSchema) -> Result<bool>` - Returns the nullability of the expression based on [ExprSchema].
  - `fn metadata(self: &Self, schema: &dyn ExprSchema) -> Result<FieldMetadata>`
  - `fn data_type_and_nullable(self: &Self, schema: &dyn ExprSchema) -> Result<(DataType, bool)>` - Returns the datatype and nullability of the expression based on [ExprSchema].
  - `fn to_field(self: &Self, schema: &dyn ExprSchema) -> Result<(Option<TableReference>, Arc<Field>)>` - Returns a [arrow::datatypes::Field] compatible with this expression.
  - `fn cast_to(self: Self, cast_to_type: &DataType, schema: &dyn ExprSchema) -> Result<Expr>` - Wraps this expression in a cast to a target [arrow::datatypes::DataType].
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Expr) -> $crate::option::Option<$crate::cmp::Ordering>`
- **From**
  - `fn from(value: (Option<&'a TableReference>, &'a FieldRef)) -> Self`
- **Rem**
  - `fn rem(self: Self, rhs: Self) -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **BitAnd**
  - `fn bitand(self: Self, rhs: Self) -> Self`
- **AsRef**
  - `fn as_ref(self: &Self) -> &Expr`
- **Not**
  - `fn not(self: Self) -> <Self as >::Output`
- **From**
  - `fn from(value: Column) -> Self`
- **TreeNode**
  - `fn apply_children<'n, F>(self: &'n Self, f: F) -> Result<TreeNodeRecursion>` - Applies a function `f` to each child expression of `self`.
  - `fn map_children<F>(self: Self, f: F) -> Result<Transformed<Self>>` - Maps each child of `self` using the provided closure `f`.
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **TreeNodeContainer**
  - `fn apply_elements<F>(self: &'a Self, f: F) -> Result<TreeNodeRecursion>`
  - `fn map_elements<F>(self: Self, f: F) -> Result<Transformed<Self>>`
- **Neg**
  - `fn neg(self: Self) -> <Self as >::Output`



## datafusion_expr::expr::ExprListDisplay

*Struct*

Formats a list of `&Expr` with a custom separator using SQL display format

**Generic Parameters:**
- 'a

**Methods:**

- `fn new(exprs: &'a [Expr], sep: &'a str) -> Self` - Create a new display struct with the given expressions and separator
- `fn comma_separated(exprs: &'a [Expr]) -> Self` - Create a new display struct with comma-space separator

**Trait Implementations:**

- **Display**
  - `fn fmt(self: &Self, f: & mut Formatter) -> fmt::Result`



## datafusion_expr::expr::GetFieldAccess

*Enum*

Access a sub field of a nested type, such as `Field` or `List`

**Variants:**
- `NamedStructField{ name: datafusion_common::ScalarValue }` - Named field, for example `struct["name"]`
- `ListIndex{ key: Box<Expr> }` - Single list index, for example: `list[i]`
- `ListRange{ start: Box<Expr>, stop: Box<Expr>, stride: Box<Expr> }` - List stride, for example `list[i:j:k]`

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> GetFieldAccess`
- **PartialEq**
  - `fn eq(self: &Self, other: &GetFieldAccess) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr::expr::GroupingSet

*Enum*

Grouping sets

See <https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-GROUPING-SETS>
for Postgres definition.
See <https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-groupby.html>
for Apache Spark definition.

**Variants:**
- `Rollup(Vec<Expr>)` - Rollup grouping sets
- `Cube(Vec<Expr>)` - Cube grouping sets
- `GroupingSets(Vec<Vec<Expr>>)` - User-defined grouping sets

**Methods:**

- `fn distinct_expr(self: &Self) -> Vec<&Expr>` - Return all distinct exprs in the grouping set. For `CUBE` and `ROLLUP` this

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> GroupingSet`
- **PartialEq**
  - `fn eq(self: &Self, other: &GroupingSet) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &GroupingSet) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_expr::expr::InList

*Struct*

InList expression

**Fields:**
- `expr: Box<Expr>` - The expression to compare
- `list: Vec<Expr>` - The list of values to compare against
- `negated: bool` - Whether the expression is negated

**Methods:**

- `fn new(expr: Box<Expr>, list: Vec<Expr>, negated: bool) -> Self` - Create a new InList expression

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> InList`
- **PartialEq**
  - `fn eq(self: &Self, other: &InList) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &InList) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_expr::expr::InSubquery

*Struct*

IN subquery

**Fields:**
- `expr: Box<Expr>` - The expression to compare
- `subquery: crate::logical_plan::Subquery` - Subquery that will produce a single column of data to compare against
- `negated: bool` - Whether the expression is negated

**Methods:**

- `fn new(expr: Box<Expr>, subquery: Subquery, negated: bool) -> Self` - Create a new InSubquery expression

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> InSubquery`
- **PartialEq**
  - `fn eq(self: &Self, other: &InSubquery) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &InSubquery) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr::expr::Like

*Struct*

LIKE expression

**Fields:**
- `negated: bool`
- `expr: Box<Expr>`
- `pattern: Box<Expr>`
- `escape_char: Option<char>`
- `case_insensitive: bool` - Whether to ignore case on comparing

**Methods:**

- `fn new(negated: bool, expr: Box<Expr>, pattern: Box<Expr>, escape_char: Option<char>, case_insensitive: bool) -> Self` - Create a new Like expression

**Traits:** Eq

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &Like) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Like) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Like`



## datafusion_expr::expr::NullTreatment

*Enum*

**Variants:**
- `IgnoreNulls`
- `RespectNulls`

**Traits:** Copy, Eq

**Trait Implementations:**

- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &NullTreatment) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Display**
  - `fn fmt(self: &Self, f: & mut Formatter) -> fmt::Result`
- **From**
  - `fn from(value: sqlparser::ast::NullTreatment) -> Self`
- **Clone**
  - `fn clone(self: &Self) -> NullTreatment`
- **Ord**
  - `fn cmp(self: &Self, other: &NullTreatment) -> $crate::cmp::Ordering`
- **PartialEq**
  - `fn eq(self: &Self, other: &NullTreatment) -> bool`



## datafusion_expr::expr::OUTER_REFERENCE_COLUMN_PREFIX

*Constant*: `&str`



## datafusion_expr::expr::Placeholder

*Struct*

Placeholder, representing bind parameter values such as `$1` or `$name`.

The type of these parameters is inferred using [`Expr::infer_placeholder_types`]
or can be specified directly using `PREPARE` statements.

**Fields:**
- `id: String` - The identifier of the parameter, including the leading `$` (e.g, `"$1"` or `"$foo"`)
- `field: Option<arrow::datatypes::FieldRef>` - The type the parameter will be filled in with

**Methods:**

- `fn new(id: String, data_type: Option<DataType>) -> Self` - Create a new Placeholder expression
- `fn new_with_field(id: String, field: Option<FieldRef>) -> Self` - Create a new Placeholder expression from a Field

**Traits:** Eq

**Trait Implementations:**

- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Placeholder`
- **PartialEq**
  - `fn eq(self: &Self, other: &Placeholder) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Placeholder) -> $crate::option::Option<$crate::cmp::Ordering>`



## datafusion_expr::expr::PlannedReplaceSelectItem

*Struct*

The planned expressions for `REPLACE`

**Fields:**
- `items: Vec<sqlparser::ast::ReplaceSelectElement>` - The original ast nodes
- `planned_expressions: Vec<Expr>` - The expression planned from the ast nodes. They will be used when expanding the wildcard.

**Methods:**

- `fn items(self: &Self) -> &[ReplaceSelectElement]`
- `fn expressions(self: &Self) -> &[Expr]`

**Traits:** Eq

**Trait Implementations:**

- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &PlannedReplaceSelectItem) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Default**
  - `fn default() -> PlannedReplaceSelectItem`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Display**
  - `fn fmt(self: &Self, f: & mut Formatter) -> fmt::Result`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> PlannedReplaceSelectItem`
- **PartialEq**
  - `fn eq(self: &Self, other: &PlannedReplaceSelectItem) -> bool`



## datafusion_expr::expr::ScalarFunction

*Struct*

Invoke a [`ScalarUDF`] with a set of arguments

[`ScalarUDF`]: crate::ScalarUDF

**Fields:**
- `func: std::sync::Arc<crate::ScalarUDF>` - The function
- `args: Vec<Expr>` - List of expressions to feed to the functions as arguments

**Methods:**

- `fn name(self: &Self) -> &str`
- `fn new_udf(udf: Arc<crate::ScalarUDF>, args: Vec<Expr>) -> Self` - Create a new `ScalarFunction` from a [`ScalarUDF`]

**Traits:** Eq

**Trait Implementations:**

- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> ScalarFunction`
- **PartialEq**
  - `fn eq(self: &Self, other: &ScalarFunction) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &ScalarFunction) -> $crate::option::Option<$crate::cmp::Ordering>`



## datafusion_expr::expr::SchemaFieldMetadata

*Type Alias*: `std::collections::HashMap<String, String>`

The metadata used in [`Field::metadata`].

This represents the metadata associated with an Arrow [`Field`]. The metadata consists of key-value pairs.

# Common Use Cases

Field metadata is commonly used to store:
- Default values for columns when data is missing
- Column descriptions or documentation
- Data lineage information
- Custom application-specific annotations
- Encoding hints or display formatting preferences

# Example: Storing Default Values

A practical example of using field metadata is storing default values for columns
that may be missing in the physical data but present in the logical schema.
See the [default_column_values.rs] example implementation.

[default_column_values.rs]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/custom_data_source/default_column_values.rs



## datafusion_expr::expr::Sort

*Struct*

SORT expression

**Fields:**
- `expr: Expr` - The expression to sort on
- `asc: bool` - The direction of the sort
- `nulls_first: bool` - Whether to put Nulls before all other data values

**Methods:**

- `fn new(expr: Expr, asc: bool, nulls_first: bool) -> Self` - Create a new Sort expression
- `fn reverse(self: &Self) -> Self` - Create a new Sort expression with the opposite sort direction
- `fn with_expr(self: &Self, expr: Expr) -> Self` - Replaces the Sort expressions with `expr`

**Traits:** Eq

**Trait Implementations:**

- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **TreeNodeContainer**
  - `fn apply_elements<F>(self: &'a Self, f: F) -> Result<TreeNodeRecursion>`
  - `fn map_elements<F>(self: Self, f: F) -> Result<Transformed<Self>>`
- **Clone**
  - `fn clone(self: &Self) -> Sort`
- **PartialEq**
  - `fn eq(self: &Self, other: &Sort) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Sort) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Display**
  - `fn fmt(self: &Self, f: & mut Formatter) -> fmt::Result`



## datafusion_expr::expr::TryCast

*Struct*

TryCast Expression

**Fields:**
- `expr: Box<Expr>` - The expression being cast
- `data_type: arrow::datatypes::DataType` - The `DataType` the expression will yield

**Methods:**

- `fn new(expr: Box<Expr>, data_type: DataType) -> Self` - Create a new TryCast expression

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> TryCast`
- **PartialEq**
  - `fn eq(self: &Self, other: &TryCast) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &TryCast) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_expr::expr::UNNEST_COLUMN_PREFIX

*Constant*: `&str`



## datafusion_expr::expr::Unnest

*Struct*

UNNEST expression.

**Fields:**
- `expr: Box<Expr>`

**Methods:**

- `fn new(expr: Expr) -> Self` - Create a new Unnest expression.
- `fn new_boxed(boxed: Box<Expr>) -> Self` - Create a new Unnest expression.

**Traits:** Eq

**Trait Implementations:**

- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Unnest`
- **PartialEq**
  - `fn eq(self: &Self, other: &Unnest) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Unnest) -> $crate::option::Option<$crate::cmp::Ordering>`



## datafusion_expr::expr::WildcardOptions

*Struct*

Additional options for wildcards, e.g. Snowflake `EXCLUDE`/`RENAME` and Bigquery `EXCEPT`.

**Fields:**
- `ilike: Option<sqlparser::ast::IlikeSelectItem>` - `[ILIKE...]`.
- `exclude: Option<sqlparser::ast::ExcludeSelectItem>` - `[EXCLUDE...]`.
- `except: Option<sqlparser::ast::ExceptSelectItem>` - `[EXCEPT...]`.
- `replace: Option<PlannedReplaceSelectItem>` - `[REPLACE]`
- `rename: Option<sqlparser::ast::RenameSelectItem>` - `[RENAME ...]`.

**Methods:**

- `fn with_replace(self: Self, replace: PlannedReplaceSelectItem) -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> WildcardOptions`
- **PartialEq**
  - `fn eq(self: &Self, other: &WildcardOptions) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &WildcardOptions) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Default**
  - `fn default() -> WildcardOptions`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Display**
  - `fn fmt(self: &Self, f: & mut Formatter) -> fmt::Result`



## datafusion_expr::expr::WindowFunction

*Struct*

Window function

Holds the actual function to call [`WindowFunction`] as well as its
arguments (`args`) and the contents of the `OVER` clause:

1. `PARTITION BY`
2. `ORDER BY`
3. Window frame (e.g. `ROWS 1 PRECEDING AND 1 FOLLOWING`)

See [`ExprFunctionExt`] for examples of how to create a `WindowFunction`.

[`ExprFunctionExt`]: crate::ExprFunctionExt

**Fields:**
- `fun: WindowFunctionDefinition` - Name of the function
- `params: WindowFunctionParams`

**Methods:**

- `fn new<impl Into<WindowFunctionDefinition>>(fun: impl Trait, args: Vec<Expr>) -> Self` - Create a new Window expression with the specified argument an
- `fn simplify(self: &Self) -> Option<WindowFunctionSimplification>` - Return the inner window simplification function, if any

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> WindowFunction`
- **PartialEq**
  - `fn eq(self: &Self, other: &WindowFunction) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &WindowFunction) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_expr::expr::WindowFunctionDefinition

*Enum*

A function used as a SQL window function

In SQL, you can use:
- Actual window functions ([`WindowUDF`])
- Normal aggregate functions ([`AggregateUDF`])

**Variants:**
- `AggregateUDF(std::sync::Arc<crate::AggregateUDF>)` - A user defined aggregate function
- `WindowUDF(std::sync::Arc<crate::WindowUDF>)` - A user defined aggregate function

**Methods:**

- `fn return_field(self: &Self, input_expr_fields: &[FieldRef], display_name: &str) -> Result<FieldRef>` - Returns the datatype of the window function
- `fn signature(self: &Self) -> Signature` - The signatures supported by the function `fun`.
- `fn name(self: &Self) -> &str` - Function's name for display
- `fn simplify(self: &Self) -> Option<WindowFunctionSimplification>` - Return the inner window simplification function, if any

**Traits:** Eq

**Trait Implementations:**

- **From**
  - `fn from(value: Arc<AggregateUDF>) -> Self`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **From**
  - `fn from(value: Arc<WindowUDF>) -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Display**
  - `fn fmt(self: &Self, f: & mut Formatter) -> fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> WindowFunctionDefinition`
- **PartialEq**
  - `fn eq(self: &Self, other: &WindowFunctionDefinition) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &WindowFunctionDefinition) -> $crate::option::Option<$crate::cmp::Ordering>`



## datafusion_expr::expr::WindowFunctionParams

*Struct*

**Fields:**
- `args: Vec<Expr>` - List of expressions to feed to the functions as arguments
- `partition_by: Vec<Expr>` - List of partition by expressions
- `order_by: Vec<Sort>` - List of order by expressions
- `window_frame: crate::WindowFrame` - Window frame
- `filter: Option<Box<Expr>>` - Optional filter expression (FILTER (WHERE ...))
- `null_treatment: Option<NullTreatment>` - Specifies how NULL value is treated: ignore or respect
- `distinct: bool` - Distinct flag

**Traits:** Eq

**Trait Implementations:**

- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> WindowFunctionParams`
- **PartialEq**
  - `fn eq(self: &Self, other: &WindowFunctionParams) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &WindowFunctionParams) -> $crate::option::Option<$crate::cmp::Ordering>`



## datafusion_expr::expr::intersect_metadata_for_union

*Function*

Intersects multiple metadata instances for UNION operations.

This function implements the intersection strategy used by UNION operations,
where only metadata keys that exist in ALL inputs with identical values
are preserved in the result.

# Union Metadata Behavior

Union operations require consistent metadata across all branches:
- Only metadata keys present in ALL union branches are kept
- For each kept key, the value must be identical across all branches
- If a key has different values across branches, it is excluded from the result
- If any input has no metadata, the result will be empty

# Arguments

* `metadatas` - An iterator of `SchemaFieldMetadata` instances to intersect

# Returns

A new `SchemaFieldMetadata` containing only the intersected metadata

```rust
fn intersect_metadata_for_union<'a, impl IntoIterator<Item = &'a SchemaFieldMetadata>>(metadatas: impl Trait) -> SchemaFieldMetadata
```



## datafusion_expr::expr::physical_name

*Function*

The name of the column (field) that this `Expr` will produce in the physical plan.
The difference from [Expr::schema_name] is that top-level columns are unqualified.

```rust
fn physical_name(expr: &Expr) -> datafusion_common::Result<String>
```



## datafusion_expr::expr::schema_name_from_exprs

*Function*

Get schema_name for Vector of expressions

```rust
fn schema_name_from_exprs(exprs: &[Expr]) -> datafusion_common::Result<String, fmt::Error>
```



## datafusion_expr::expr::schema_name_from_sorts

*Function*

```rust
fn schema_name_from_sorts(sorts: &[Sort]) -> datafusion_common::Result<String, fmt::Error>
```



