**datafusion_common > table_reference**

# Module: table_reference

## Contents

**Structs**

- [`ResolvedTableReference`](#resolvedtablereference) - A fully resolved path to a table of the form "catalog.schema.table"

**Enums**

- [`TableReference`](#tablereference) - A multi part identifier (path) to a table that may require further

---

## datafusion_common::table_reference::ResolvedTableReference

*Struct*

A fully resolved path to a table of the form "catalog.schema.table"

**Fields:**
- `catalog: std::sync::Arc<str>` - The catalog (aka database) containing the table
- `schema: std::sync::Arc<str>` - The schema containing the table
- `table: std::sync::Arc<str>` - The table name

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Ord**
  - `fn cmp(self: &Self, other: &ResolvedTableReference) -> $crate::cmp::Ordering`
- **Clone**
  - `fn clone(self: &Self) -> ResolvedTableReference`
- **PartialEq**
  - `fn eq(self: &Self, other: &ResolvedTableReference) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Display**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &ResolvedTableReference) -> $crate::option::Option<$crate::cmp::Ordering>`



## datafusion_common::table_reference::TableReference

*Enum*

A multi part identifier (path) to a table that may require further
resolution (e.g. `foo.bar`).

[`TableReference`]s are cheap to `clone()` as they are implemented with
`Arc`.

See [`ResolvedTableReference`] for a fully resolved table reference.

# Creating [`TableReference`]

When converting strings to [`TableReference`]s, the string is parsed as
though it were a SQL identifier, normalizing (convert to lowercase) any
unquoted identifiers.  [`TableReference::bare`] creates references without
applying normalization semantics.

# Examples
```
# use datafusion_common::TableReference;
// Get a table reference to 'mytable'
let table_reference = TableReference::from("mytable");
assert_eq!(table_reference, TableReference::bare("mytable"));

// Get a table reference to 'mytable' (note the capitalization)
let table_reference = TableReference::from("MyTable");
assert_eq!(table_reference, TableReference::bare("mytable"));

// Get a table reference to 'MyTable' (note the capitalization) using double quotes
// (programmatically it is better to use `TableReference::bare` for this)
let table_reference = TableReference::from(r#""MyTable""#);
assert_eq!(table_reference, TableReference::bare("MyTable"));

// Get a table reference to 'myschema.mytable' (note the capitalization)
let table_reference = TableReference::from("MySchema.MyTable");
assert_eq!(
    table_reference,
    TableReference::partial("myschema", "mytable")
);
```

**Variants:**
- `Bare{ table: std::sync::Arc<str> }` - An unqualified table reference, e.g. "table"
- `Partial{ schema: std::sync::Arc<str>, table: std::sync::Arc<str> }` - A partially resolved table reference, e.g. "schema.table"
- `Full{ catalog: std::sync::Arc<str>, schema: std::sync::Arc<str>, table: std::sync::Arc<str> }` - A fully resolved table reference, e.g. "catalog.schema.table"

**Methods:**

- `fn none() -> Option<TableReference>` - Convenience method for creating a typed none `None`
- `fn bare<impl Into<Arc<str>>>(table: impl Trait) -> TableReference` - Convenience method for creating a [`TableReference::Bare`]
- `fn partial<impl Into<Arc<str>>, impl Into<Arc<str>>>(schema: impl Trait, table: impl Trait) -> TableReference` - Convenience method for creating a [`TableReference::Partial`].
- `fn full<impl Into<Arc<str>>, impl Into<Arc<str>>, impl Into<Arc<str>>>(catalog: impl Trait, schema: impl Trait, table: impl Trait) -> TableReference` - Convenience method for creating a [`TableReference::Full`]
- `fn table(self: &Self) -> &str` - Retrieve the table name, regardless of qualification.
- `fn schema(self: &Self) -> Option<&str>` - Retrieve the schema name if [`Self::Partial]` or [`Self::`Full`],
- `fn catalog(self: &Self) -> Option<&str>` - Retrieve the catalog name if  [`Self::Full`], `None` otherwise.
- `fn resolved_eq(self: &Self, other: &Self) -> bool` - Compare with another [`TableReference`] as if both are resolved.
- `fn resolve(self: Self, default_catalog: &str, default_schema: &str) -> ResolvedTableReference` - Given a default catalog and schema, ensure this table reference is fully
- `fn to_quoted_string(self: &Self) -> String` - Forms a string where the identifiers are quoted
- `fn parse_str(s: &str) -> Self` - Forms a [`TableReference`] by parsing `s` as a multipart SQL
- `fn parse_str_normalized(s: &str, ignore_case: bool) -> Self` - Forms a [`TableReference`] by parsing `s` as a multipart SQL
- `fn to_vec(self: &Self) -> Vec<String>` - Decompose a [`TableReference`] to separate parts. The result vector contains

**Traits:** Eq

**Trait Implementations:**

- **From**
  - `fn from(resolved: ResolvedTableReference) -> Self`
- **Ord**
  - `fn cmp(self: &Self, other: &TableReference) -> $crate::cmp::Ordering`
- **From**
  - `fn from(s: &'a String) -> Self`
- **Clone**
  - `fn clone(self: &Self) -> TableReference`
- **PartialEq**
  - `fn eq(self: &Self, other: &TableReference) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Display**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **From**
  - `fn from(s: String) -> Self`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &TableReference) -> $crate::option::Option<$crate::cmp::Ordering>`
- **From**
  - `fn from(s: &'a str) -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



