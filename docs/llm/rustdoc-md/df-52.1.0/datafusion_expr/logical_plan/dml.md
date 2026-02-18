**datafusion_expr > logical_plan > dml**

# Module: logical_plan::dml

## Contents

**Structs**

- [`CopyTo`](#copyto) - Operator that copies the contents of a database to file(s)
- [`DmlStatement`](#dmlstatement) - Modifies the content of a database

**Enums**

- [`InsertOp`](#insertop)
- [`WriteOp`](#writeop) - The type of DML operation to perform.

---

## datafusion_expr::logical_plan::dml::CopyTo

*Struct*

Operator that copies the contents of a database to file(s)

**Fields:**
- `input: std::sync::Arc<crate::LogicalPlan>` - The relation that determines the tuples to write to the output file(s)
- `output_url: String` - The location to write the file(s)
- `partition_by: Vec<String>` - Determines which, if any, columns should be used for hive-style partitioned writes
- `file_type: std::sync::Arc<dyn FileType>` - File type trait
- `options: std::collections::HashMap<String, String>` - SQL Options that can affect the formats
- `output_schema: datafusion_common::DFSchemaRef` - The schema of the output (a single column "count")

**Methods:**

- `fn new(input: Arc<LogicalPlan>, output_url: String, partition_by: Vec<String>, file_type: Arc<dyn FileType>, options: HashMap<String, String>) -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> CopyTo`
- **Hash**
  - `fn hash<H>(self: &Self, state: & mut H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut Formatter) -> fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &Self) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Self) -> Option<Ordering>`



## datafusion_expr::logical_plan::dml::DmlStatement

*Struct*

Modifies the content of a database

This operator is used to perform DML operations such as INSERT, DELETE,
UPDATE, and CTAS (CREATE TABLE AS SELECT).

* `INSERT` - Appends new rows to the existing table. Calls
  [`TableProvider::insert_into`]

* `DELETE` - Removes rows from the table. Calls [`TableProvider::delete_from`]

* `UPDATE` - Modifies existing rows in the table. Calls [`TableProvider::update`]

* `CREATE TABLE AS SELECT` - Creates a new table and populates it with data
  from a query. This is similar to the `INSERT` operation, but it creates a new
  table instead of modifying an existing one.

Note that the structure is adapted from substrait WriteRel)

[`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html
[`TableProvider::insert_into`]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html#method.insert_into
[`TableProvider::delete_from`]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html#method.delete_from
[`TableProvider::update`]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html#method.update

**Fields:**
- `table_name: datafusion_common::TableReference` - The table name
- `target: std::sync::Arc<dyn TableSource>` - this is target table to insert into
- `op: WriteOp` - The type of operation to perform
- `input: std::sync::Arc<crate::LogicalPlan>` - The relation that determines the tuples to add/remove/modify the schema must match with table_schema
- `output_schema: datafusion_common::DFSchemaRef` - The schema of the output relation

**Methods:**

- `fn new(table_name: TableReference, target: Arc<dyn TableSource>, op: WriteOp, input: Arc<LogicalPlan>) -> Self` - Creates a new DML statement with the output schema set to a single `count` column.
- `fn name(self: &Self) -> &str` - Return a descriptive name of this [`DmlStatement`]

**Traits:** Eq

**Trait Implementations:**

- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Self) -> Option<Ordering>`
- **PartialEq**
  - `fn eq(self: &Self, other: &Self) -> bool`
- **Debug**
  - `fn fmt(self: &Self, f: & mut Formatter) -> fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> DmlStatement`
- **Hash**
  - `fn hash<H>(self: &Self, state: & mut H)`



## datafusion_expr::logical_plan::dml::InsertOp

*Enum*

**Variants:**
- `Append` - Appends new rows to the existing table without modifying any
- `Overwrite` - Overwrites all existing rows in the table with the new rows.
- `Replace` - If any existing rows collides with the inserted rows (typically based

**Methods:**

- `fn name(self: &Self) -> &str` - Return a descriptive name of this [`InsertOp`]

**Traits:** Eq, Copy

**Trait Implementations:**

- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Display**
  - `fn fmt(self: &Self, f: & mut Formatter) -> fmt::Result`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &InsertOp) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &InsertOp) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Clone**
  - `fn clone(self: &Self) -> InsertOp`



## datafusion_expr::logical_plan::dml::WriteOp

*Enum*

The type of DML operation to perform.

See [`DmlStatement`] for more details.

**Variants:**
- `Insert(InsertOp)` - `INSERT INTO` operation
- `Delete` - `DELETE` operation
- `Update` - `UPDATE` operation
- `Ctas` - `CREATE TABLE AS SELECT` operation

**Methods:**

- `fn name(self: &Self) -> &str` - Return a descriptive name of this [`WriteOp`]

**Traits:** Eq

**Trait Implementations:**

- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Display**
  - `fn fmt(self: &Self, f: & mut Formatter) -> fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> WriteOp`
- **PartialEq**
  - `fn eq(self: &Self, other: &WriteOp) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &WriteOp) -> $crate::option::Option<$crate::cmp::Ordering>`



