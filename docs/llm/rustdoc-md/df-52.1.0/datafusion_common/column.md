**datafusion_common > column**

# Module: column

## Contents

**Structs**

- [`Column`](#column) - A named reference to a qualified field in a schema.

---

## datafusion_common::column::Column

*Struct*

A named reference to a qualified field in a schema.

**Fields:**
- `relation: Option<crate::TableReference>` - relation/table reference.
- `name: String` - field/column name.
- `spans: crate::Spans` - Original source code location, if known

**Methods:**

- `fn new<impl Into<TableReference>, impl Into<String>>(relation: Option<impl Trait>, name: impl Trait) -> Self` - Create Column from optional qualifier and name. The optional qualifier, if present,
- `fn new_unqualified<impl Into<String>>(name: impl Trait) -> Self` - Convenience method for when there is no qualifier
- `fn from_name<impl Into<String>>(name: impl Trait) -> Self` - Create Column from unqualified name.
- `fn from_qualified_name<impl Into<String>>(flat_name: impl Trait) -> Self` - Deserialize a fully qualified name string into a column
- `fn from_qualified_name_ignore_case<impl Into<String>>(flat_name: impl Trait) -> Self` - Deserialize a fully qualified name string into a column preserving column text case
- `fn name(self: &Self) -> &str` - return the column's name.
- `fn flat_name(self: &Self) -> String` - Serialize column into a flat name string
- `fn quoted_flat_name(self: &Self) -> String` - Serialize column into a quoted flat name string
- `fn normalize_with_schemas_and_ambiguity_check(self: Self, schemas: &[&[&DFSchema]], using_columns: &[HashSet<Column>]) -> Result<Self>` - Qualify column if not done yet.
- `fn spans(self: &Self) -> &Spans` - Returns a reference to the set of locations in the SQL query where this
- `fn spans_mut(self: & mut Self) -> & mut Spans` - Returns a mutable reference to the set of locations in the SQL query
- `fn with_spans(self: Self, spans: Spans) -> Self` - Replaces the set of locations in the SQL query where this column
- `fn with_relation(self: &Self, relation: TableReference) -> Self` - Qualifies the column with the given table reference.

**Traits:** Eq

**Trait Implementations:**

- **From**
  - `fn from((relation, field): (Option<&TableReference>, &Field)) -> Self`
- **From**
  - `fn from((relation, field): (Option<&TableReference>, &FieldRef)) -> Self`
- **Clone**
  - `fn clone(self: &Self) -> Column`
- **Debug**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`
- **From**
  - `fn from(c: &str) -> Self`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Column) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Ord**
  - `fn cmp(self: &Self, other: &Column) -> $crate::cmp::Ordering`
- **FromStr**
  - `fn from_str(s: &str) -> Result<Self, <Self as >::Err>`
- **From**
  - `fn from(c: &String) -> Self`
- **Display**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &Column) -> bool`
- **From**
  - `fn from(c: String) -> Self`



