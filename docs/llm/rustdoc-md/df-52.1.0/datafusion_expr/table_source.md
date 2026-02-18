**datafusion_expr > table_source**

# Module: table_source

## Contents

**Enums**

- [`TableProviderFilterPushDown`](#tableproviderfilterpushdown) - Indicates how a filter expression is handled by
- [`TableType`](#tabletype) - Indicates the type of this table for metadata/catalog purposes.

**Traits**

- [`TableSource`](#tablesource) - Planning time information about a table.

---

## datafusion_expr::table_source::TableProviderFilterPushDown

*Enum*

Indicates how a filter expression is handled by
[`TableProvider::scan`].

Filter expressions are boolean expressions used to reduce the number of
rows that are read from a table. Only rows that evaluate to `true` ("pass
the filter") are returned. Rows that evaluate to `false` or `NULL` are
omitted.

[`TableProvider::scan`]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html#tymethod.scan

**Variants:**
- `Unsupported` - The filter cannot be used by the provider and will not be pushed down.
- `Inexact` - The filter can be used, but the provider might still return some tuples
- `Exact` - The provider **guarantees** that it will omit **only** tuples which

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &TableProviderFilterPushDown) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> TableProviderFilterPushDown`



## datafusion_expr::table_source::TableSource

*Trait*

Planning time information about a table.

This trait is used during logical query planning and optimizations, and
provides a subset of the [`TableProvider`] trait, such as schema information
and filter push-down capabilities. The [`TableProvider`] trait provides
additional information needed for physical query execution, such as the
ability to perform a scan or insert data.

# See Also:

[`DefaultTableSource`]  to go from [`TableProvider`], to `TableSource`

# Rationale

The reason for having two separate traits is to avoid having the logical
plan code be dependent on the DataFusion execution engine. Some projects use
DataFusion's logical plans and have their own execution engine.

[`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html
[`DefaultTableSource`]: https://docs.rs/datafusion/latest/datafusion/datasource/default_table_source/struct.DefaultTableSource.html

**Methods:**

- `as_any`
- `schema`: Get a reference to the schema for this table
- `constraints`: Get primary key indices, if any
- `table_type`: Get the type of this table for metadata/catalog purposes.
- `supports_filters_pushdown`: Tests whether the table provider can make use of any or all filter expressions
- `get_logical_plan`: Get the Logical plan of this table provider, if available.
- `get_column_default`: Get the default value for a column, if available.



## datafusion_expr::table_source::TableType

*Enum*

Indicates the type of this table for metadata/catalog purposes.

**Variants:**
- `Base` - An ordinary physical table.
- `View` - A non-materialized table that itself uses a query internally to provide data.
- `Temporary` - A transient table.

**Traits:** Copy, Eq

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &TableType) -> bool`
- **Display**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> TableType`



