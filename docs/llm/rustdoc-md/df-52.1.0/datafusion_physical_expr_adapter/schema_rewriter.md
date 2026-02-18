**datafusion_physical_expr_adapter > schema_rewriter**

# Module: schema_rewriter

## Contents

**Structs**

- [`BatchAdapter`](#batchadapter) - Adapter for transforming record batches to match a target schema.
- [`BatchAdapterFactory`](#batchadapterfactory) - Factory for creating [`BatchAdapter`] instances to adapt record batches
- [`DefaultPhysicalExprAdapter`](#defaultphysicalexpradapter) - Default implementation of [`PhysicalExprAdapter`] for rewriting physical
- [`DefaultPhysicalExprAdapterFactory`](#defaultphysicalexpradapterfactory)

**Functions**

- [`replace_columns_with_literals`](#replace_columns_with_literals) - Replace column references in the given physical expression with literal values.

**Traits**

- [`PhysicalExprAdapter`](#physicalexpradapter) - Trait for adapting [`PhysicalExpr`] expressions to match a target schema.
- [`PhysicalExprAdapterFactory`](#physicalexpradapterfactory) - Creates instances of [`PhysicalExprAdapter`] for given logical and physical schemas.

---

## datafusion_physical_expr_adapter::schema_rewriter::BatchAdapter

*Struct*

Adapter for transforming record batches to match a target schema.

Create instances via [`BatchAdapterFactory`].

## Performance

The adapter pre-computes the projection expressions during creation,
so the [`adapt_batch`](BatchAdapter::adapt_batch) call is efficient and suitable
for use in hot paths like streaming file scans.

**Methods:**

- `fn adapt_batch(self: &Self, batch: &RecordBatch) -> Result<RecordBatch>` - Adapt the given record batch to match the target schema.

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_physical_expr_adapter::schema_rewriter::BatchAdapterFactory

*Struct*

Factory for creating [`BatchAdapter`] instances to adapt record batches
to a target schema.

This binds a target schema and allows creating adapters for different source schemas.
It handles:
- **Column reordering**: Columns are reordered to match the target schema
- **Type casting**: Automatic type conversion (e.g., Int32 to Int64)
- **Missing columns**: Nullable columns missing from source are filled with nulls
- **Struct field adaptation**: Nested struct fields are recursively adapted

## Examples

```rust
use arrow::array::{Int32Array, Int64Array, StringArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_physical_expr_adapter::BatchAdapterFactory;
use std::sync::Arc;

// Target schema has different column order and types
let target_schema = Arc::new(Schema::new(vec![
    Field::new("name", DataType::Utf8, true),
    Field::new("id", DataType::Int64, false),    // Int64 in target
    Field::new("score", DataType::Float64, true), // Missing from source
]));

// Source schema has different column order and Int32 for id
let source_schema = Arc::new(Schema::new(vec![
    Field::new("id", DataType::Int32, false),    // Int32 in source
    Field::new("name", DataType::Utf8, true),
    // Note: 'score' column is missing from source
]));

// Create factory with target schema
let factory = BatchAdapterFactory::new(Arc::clone(&target_schema));

// Create adapter for this specific source schema
let adapter = factory.make_adapter(Arc::clone(&source_schema)).unwrap();

// Create a source batch
let source_batch = RecordBatch::try_new(
    source_schema,
    vec![
        Arc::new(Int32Array::from(vec![1, 2, 3])),
        Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])),
    ],
).unwrap();

// Adapt the batch to match target schema
let adapted = adapter.adapt_batch(&source_batch).unwrap();

assert_eq!(adapted.num_columns(), 3);
assert_eq!(adapted.column(0).data_type(), &DataType::Utf8);   // name
assert_eq!(adapted.column(1).data_type(), &DataType::Int64);  // id (cast from Int32)
assert_eq!(adapted.column(2).data_type(), &DataType::Float64); // score (filled with nulls)
```

**Methods:**

- `fn new(target_schema: SchemaRef) -> Self` - Create a new [`BatchAdapterFactory`] with the given target schema.
- `fn with_adapter_factory(self: Self, factory: Arc<dyn PhysicalExprAdapterFactory>) -> Self` - Set a custom [`PhysicalExprAdapterFactory`] to use when adapting expressions.
- `fn make_adapter(self: &Self, source_schema: SchemaRef) -> Result<BatchAdapter>` - Create a new [`BatchAdapter`] for the given source schema.

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_physical_expr_adapter::schema_rewriter::DefaultPhysicalExprAdapter

*Struct*

Default implementation of [`PhysicalExprAdapter`] for rewriting physical
expressions to match different schemas.

## Overview

 [`DefaultPhysicalExprAdapter`] rewrites physical expressions to match
 different schemas, including:

- **Type casting**: When logical and physical schemas have different types, expressions are
  automatically wrapped with cast operations. For example, `lit(ScalarValue::Int32(123)) = int64_column`
  gets rewritten to `lit(ScalarValue::Int32(123)) = cast(int64_column, 'Int32')`.
  Note that this does not attempt to simplify such expressions - that is done by shared simplifiers.

- **Missing columns**: When a column exists in the logical schema but not in the physical schema,
  references to it are replaced with null literals.

- **Struct field access**: Expressions like `struct_column.field_that_is_missing_in_schema` are
  rewritten to `null` when the field doesn't exist in the physical schema.

- **Default column values**: Partition column references can be replaced with their literal values
  when scanning specific partitions. See [`replace_columns_with_literals`] for more details.

# Example

```rust
# use datafusion_physical_expr_adapter::{DefaultPhysicalExprAdapterFactory, PhysicalExprAdapterFactory};
# use arrow::datatypes::Schema;
# use std::sync::Arc;
#
# fn example(
#     predicate: std::sync::Arc<dyn datafusion_physical_expr_common::physical_expr::PhysicalExpr>,
#     physical_file_schema: &Schema,
#     logical_file_schema: &Schema,
# ) -> datafusion_common::Result<()> {
let factory = DefaultPhysicalExprAdapterFactory;
let adapter = factory.create(Arc::new(logical_file_schema.clone()), Arc::new(physical_file_schema.clone()));
let adapted_predicate = adapter.rewrite(predicate)?;
# Ok(())
# }
```

**Methods:**

- `fn new(logical_file_schema: SchemaRef, physical_file_schema: SchemaRef) -> Self` - Create a new instance of the default physical expression adapter.

**Trait Implementations:**

- **PhysicalExprAdapter**
  - `fn rewrite(self: &Self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>>`
- **Clone**
  - `fn clone(self: &Self) -> DefaultPhysicalExprAdapter`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_physical_expr_adapter::schema_rewriter::DefaultPhysicalExprAdapterFactory

*Struct*

**Unit Struct**

**Trait Implementations:**

- **PhysicalExprAdapterFactory**
  - `fn create(self: &Self, logical_file_schema: SchemaRef, physical_file_schema: SchemaRef) -> Arc<dyn PhysicalExprAdapter>`
- **Clone**
  - `fn clone(self: &Self) -> DefaultPhysicalExprAdapterFactory`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_physical_expr_adapter::schema_rewriter::PhysicalExprAdapter

*Trait*

Trait for adapting [`PhysicalExpr`] expressions to match a target schema.

This is used in file scans to rewrite expressions so that they can be
evaluated against the physical schema of the file being scanned. It allows
for handling differences between logical and physical schemas, such as type
mismatches or missing columns common in [Schema evolution] scenarios.

[Schema evolution]: https://www.dremio.com/wiki/schema-evolution/

## Default Implementations

The default implementation [`DefaultPhysicalExprAdapter`]  handles common
cases.

## Custom Implementations

You can create a custom implementation of this trait to handle specific rewriting logic.
For example, to fill in missing columns with default values instead of nulls:

```rust
use datafusion_physical_expr_adapter::{PhysicalExprAdapter, PhysicalExprAdapterFactory};
use arrow::datatypes::{Schema, Field, DataType, FieldRef, SchemaRef};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_common::{Result, ScalarValue, tree_node::{Transformed, TransformedResult, TreeNode}};
use datafusion_physical_expr::expressions::{self, Column};
use std::sync::Arc;

#[derive(Debug)]
pub struct CustomPhysicalExprAdapter {
    logical_file_schema: SchemaRef,
    physical_file_schema: SchemaRef,
}

impl PhysicalExprAdapter for CustomPhysicalExprAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        expr.transform(|expr| {
            if let Some(column) = expr.as_any().downcast_ref::<Column>() {
                // Check if the column exists in the physical schema
                if self.physical_file_schema.index_of(column.name()).is_err() {
                    // If the column is missing, fill it with a default value instead of null
                    // The default value could be stored in the table schema's column metadata for example.
                    let default_value = ScalarValue::Int32(Some(0));
                    return Ok(Transformed::yes(expressions::lit(default_value)));
                }
            }
            // If the column exists, return it as is
            Ok(Transformed::no(expr))
        }).data()
    }
}

#[derive(Debug)]
pub struct CustomPhysicalExprAdapterFactory;

impl PhysicalExprAdapterFactory for CustomPhysicalExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Arc<dyn PhysicalExprAdapter> {
        Arc::new(CustomPhysicalExprAdapter {
            logical_file_schema,
            physical_file_schema,
        })
    }
}
```

**Methods:**

- `rewrite`: Rewrite a physical expression to match the target schema.



## datafusion_physical_expr_adapter::schema_rewriter::PhysicalExprAdapterFactory

*Trait*

Creates instances of [`PhysicalExprAdapter`] for given logical and physical schemas.

See [`DefaultPhysicalExprAdapterFactory`] for the default implementation.

**Methods:**

- `create`: Create a new instance of the physical expression adapter.



## datafusion_physical_expr_adapter::schema_rewriter::replace_columns_with_literals

*Function*

Replace column references in the given physical expression with literal values.

Some use cases for this include:
- Partition column pruning: When scanning partitioned data, partition column references
  can be replaced with their literal values for the specific partition being scanned.
- Constant folding: In some cases, columns that can be proven to be constant
  from statistical analysis may be replaced with their literal values to optimize expression evaluation.
- Filling in non-null default values: in a custom [`PhysicalExprAdapter`] implementation,
  column references can be replaced with default literal values instead of nulls.

# Arguments
- `expr`: The physical expression in which to replace column references.
- `replacements`: A mapping from column names to their corresponding literal `ScalarValue`s.
  Accepts various HashMap types including `HashMap<&str, &ScalarValue>`,
  `HashMap<String, ScalarValue>`, `HashMap<String, &ScalarValue>`, etc.

# Returns
- `Result<Arc<dyn PhysicalExpr>>`: The rewritten physical expression with columns replaced by literals.

```rust
fn replace_columns_with_literals<K, V>(expr: std::sync::Arc<dyn PhysicalExpr>, replacements: &std::collections::HashMap<K, V>) -> datafusion_common::Result<std::sync::Arc<dyn PhysicalExpr>>
```



