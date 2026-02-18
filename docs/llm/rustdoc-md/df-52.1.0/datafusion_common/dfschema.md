**datafusion_common > dfschema**

# Module: dfschema

## Contents

**Structs**

- [`DFSchema`](#dfschema) -  DFSchema wraps an Arrow schema and add a relation (table) name.

**Functions**

- [`qualified_name`](#qualified_name)

**Traits**

- [`ExprSchema`](#exprschema) - Provides schema information needed by certain methods of `Expr`
- [`SchemaExt`](#schemaext) - DataFusion-specific extensions to [`Schema`].
- [`ToDFSchema`](#todfschema) - Convenience trait to convert Schema like things to DFSchema and DFSchemaRef with fewer keystrokes

**Type Aliases**

- [`DFSchemaRef`](#dfschemaref) - A reference-counted reference to a [DFSchema].

---

## datafusion_common::dfschema::DFSchema

*Struct*

 DFSchema wraps an Arrow schema and add a relation (table) name.

 The schema may hold the fields across multiple tables. Some fields may be
 qualified and some unqualified. A qualified field is a field that has a
 relation name associated with it.

 Unqualified fields must be unique not only amongst themselves, but also must
 have a distinct name from any qualified field names. This allows finding a
 qualified field by name to be possible, so long as there aren't multiple
 qualified fields with the same name.
]
 # See Also
 * [DFSchemaRef], an alias to `Arc<DFSchema>`
 * [DataTypeExt], common methods for working with Arrow [DataType]s
 * [FieldExt], extension methods for working with Arrow [Field]s

 [DataTypeExt]: crate::datatype::DataTypeExt
 [FieldExt]: crate::datatype::FieldExt

 # Creating qualified schemas

 Use [DFSchema::try_from_qualified_schema] to create a qualified schema from
 an Arrow schema.

 ```rust
 use arrow::datatypes::{DataType, Field, Schema};
 use datafusion_common::{Column, DFSchema};

 let arrow_schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);

 let df_schema = DFSchema::try_from_qualified_schema("t1", &arrow_schema).unwrap();
 let column = Column::from_qualified_name("t1.c1");
 assert!(df_schema.has_column(&column));

 // Can also access qualified fields with unqualified name, if it's unambiguous
 let column = Column::from_qualified_name("c1");
 assert!(df_schema.has_column(&column));
 ```

 # Creating unqualified schemas

 Create an unqualified schema using TryFrom:

 ```rust
 use arrow::datatypes::{DataType, Field, Schema};
 use datafusion_common::{Column, DFSchema};

 let arrow_schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);

 let df_schema = DFSchema::try_from(arrow_schema).unwrap();
 let column = Column::new_unqualified("c1");
 assert!(df_schema.has_column(&column));
 ```

 # Converting back to Arrow schema

 Use the `Into` trait to convert `DFSchema` into an Arrow schema:

 ```rust
 use arrow::datatypes::{Field, Schema};
 use datafusion_common::DFSchema;
 use std::collections::HashMap;

 let df_schema = DFSchema::from_unqualified_fields(
     vec![Field::new("c1", arrow::datatypes::DataType::Int32, false)].into(),
     HashMap::new(),
 )
 .unwrap();
 let schema: &Schema = df_schema.as_arrow();
 assert_eq!(schema.fields().len(), 1);
 ```

**Methods:**

- `fn empty() -> Self` - Creates an empty `DFSchema`
- `fn as_arrow(self: &Self) -> &Schema` - Return a reference to the inner Arrow [`Schema`]
- `fn inner(self: &Self) -> &SchemaRef` - Return a reference to the inner Arrow [`SchemaRef`]
- `fn new_with_metadata(qualified_fields: Vec<(Option<TableReference>, Arc<Field>)>, metadata: HashMap<String, String>) -> Result<Self>` - Create a `DFSchema` from an Arrow schema where all the fields have a given qualifier
- `fn from_unqualified_fields(fields: Fields, metadata: HashMap<String, String>) -> Result<Self>` - Create a new `DFSchema` from a list of Arrow [Field]s
- `fn try_from_qualified_schema<impl Into<TableReference>>(qualifier: impl Trait, schema: &Schema) -> Result<Self>` - Create a `DFSchema` from an Arrow schema and a given qualifier
- `fn from_field_specific_qualified_schema(qualifiers: Vec<Option<TableReference>>, schema: &SchemaRef) -> Result<Self>` - Create a `DFSchema` from an Arrow schema where all the fields have a given qualifier
- `fn with_field_specific_qualified_schema(self: &Self, qualifiers: Vec<Option<TableReference>>) -> Result<Self>` - Return the same schema, where all fields have a given qualifier.
- `fn check_names(self: &Self) -> Result<()>` - Check if the schema have some fields with the same name
- `fn with_functional_dependencies(self: Self, functional_dependencies: FunctionalDependencies) -> Result<Self>` - Assigns functional dependencies.
- `fn join(self: &Self, schema: &DFSchema) -> Result<Self>` - Create a new schema that contains the fields from this schema followed by the fields
- `fn merge(self: & mut Self, other_schema: &DFSchema)` - Modify this schema by appending the fields from the supplied schema, ignoring any
- `fn fields(self: &Self) -> &Fields` - Get a list of fields for this schema
- `fn field(self: &Self, i: usize) -> &FieldRef` - Returns a reference to [`FieldRef`] for a column at specific index
- `fn qualified_field(self: &Self, i: usize) -> (Option<&TableReference>, &FieldRef)` - Returns the qualifier (if any) and [`FieldRef`] for a column at specific
- `fn index_of_column_by_name(self: &Self, qualifier: Option<&TableReference>, name: &str) -> Option<usize>`
- `fn maybe_index_of_column(self: &Self, col: &Column) -> Option<usize>` - Find the index of the column with the given qualifier and name,
- `fn index_of_column(self: &Self, col: &Column) -> Result<usize>` - Find the index of the column with the given qualifier and name,
- `fn is_column_from_schema(self: &Self, col: &Column) -> bool` - Check if the column is in the current schema
- `fn field_with_name(self: &Self, qualifier: Option<&TableReference>, name: &str) -> Result<&FieldRef>` - Find the [`FieldRef`] with the given name and optional qualifier
- `fn qualified_field_with_name(self: &Self, qualifier: Option<&TableReference>, name: &str) -> Result<(Option<&TableReference>, &FieldRef)>` - Find the qualified field with the given name
- `fn fields_with_qualified(self: &Self, qualifier: &TableReference) -> Vec<&FieldRef>` - Find all fields having the given qualifier
- `fn fields_indices_with_qualified(self: &Self, qualifier: &TableReference) -> Vec<usize>` - Find all fields indices having the given qualifier
- `fn fields_with_unqualified_name(self: &Self, name: &str) -> Vec<&FieldRef>` - Find all fields that match the given name
- `fn qualified_fields_with_unqualified_name(self: &Self, name: &str) -> Vec<(Option<&TableReference>, &FieldRef)>` - Find all fields that match the given name and return them with their qualifier
- `fn columns_with_unqualified_name(self: &Self, name: &str) -> Vec<Column>` - Find all fields that match the given name and convert to column
- `fn columns(self: &Self) -> Vec<Column>` - Return all `Column`s for the schema
- `fn qualified_field_with_unqualified_name(self: &Self, name: &str) -> Result<(Option<&TableReference>, &FieldRef)>` - Find the qualified field with the given unqualified name
- `fn field_with_unqualified_name(self: &Self, name: &str) -> Result<&FieldRef>` - Find the field with the given name
- `fn field_with_qualified_name(self: &Self, qualifier: &TableReference, name: &str) -> Result<&FieldRef>` - Find the field with the given qualified name
- `fn qualified_field_from_column(self: &Self, column: &Column) -> Result<(Option<&TableReference>, &FieldRef)>` - Find the field with the given qualified column
- `fn has_column_with_unqualified_name(self: &Self, name: &str) -> bool` - Find if the field exists with the given name
- `fn has_column_with_qualified_name(self: &Self, qualifier: &TableReference, name: &str) -> bool` - Find if the field exists with the given qualified name
- `fn has_column(self: &Self, column: &Column) -> bool` - Find if the field exists with the given qualified column
- `fn matches_arrow_schema(self: &Self, arrow_schema: &Schema) -> bool` - Check to see if unqualified field names matches field names in Arrow schema
- `fn check_arrow_schema_type_compatible(self: &Self, arrow_schema: &Schema) -> Result<()>` - Check to see if fields in 2 Arrow schemas are compatible
- `fn logically_equivalent_names_and_types(self: &Self, other: &Self) -> bool` - Returns true if the two schemas have the same qualified named
- `fn equivalent_names_and_types(self: &Self, other: &Self) -> bool`
- `fn has_equivalent_names_and_types(self: &Self, other: &Self) -> Result<()>` - Returns Ok if the two schemas have the same qualified named
- `fn datatype_is_logically_equal(dt1: &DataType, dt2: &DataType) -> bool` - Checks if two [`DataType`]s are logically equal. This is a notably weaker constraint
- `fn datatype_is_semantically_equal(dt1: &DataType, dt2: &DataType) -> bool` - Returns true of two [`DataType`]s are semantically equal (same
- `fn strip_qualifiers(self: Self) -> Self` - Strip all field qualifier in schema
- `fn replace_qualifier<impl Into<TableReference>>(self: Self, qualifier: impl Trait) -> Self` - Replace all field qualifier with new value in schema
- `fn field_names(self: &Self) -> Vec<String>` - Get list of fully-qualified field names in this schema
- `fn metadata(self: &Self) -> &HashMap<String, String>` - Get metadata of this schema
- `fn functional_dependencies(self: &Self) -> &FunctionalDependencies` - Get functional dependencies
- `fn iter(self: &Self) -> impl Trait` - Iterate over the qualifiers and fields in the DFSchema
- `fn tree_string(self: &Self) -> impl Trait` - Returns a tree-like string representation of the schema.

**Traits:** Eq

**Trait Implementations:**

- **TryFrom**
  - `fn try_from(schema: Schema) -> Result<Self, <Self as >::Error>`
- **TryFrom**
  - `fn try_from(schema: SchemaRef) -> Result<Self, <Self as >::Error>`
- **Clone**
  - `fn clone(self: &Self) -> DFSchema`
- **PartialEq**
  - `fn eq(self: &Self, other: &DFSchema) -> bool`
- **ExprSchema**
  - `fn field_from_column(self: &Self, col: &Column) -> Result<&FieldRef>`
- **AsRef**
  - `fn as_ref(self: &Self) -> &Schema`
- **AsRef**
  - `fn as_ref(self: &Self) -> &SchemaRef`
- **Hash**
  - `fn hash<H>(self: &Self, state: & mut H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Display**
  - `fn fmt(self: &Self, f: & mut Formatter) -> std::fmt::Result`



## datafusion_common::dfschema::DFSchemaRef

*Type Alias*: `std::sync::Arc<DFSchema>`

A reference-counted reference to a [DFSchema].



## datafusion_common::dfschema::ExprSchema

*Trait*

Provides schema information needed by certain methods of `Expr`
(defined in the datafusion-common crate).

Note that this trait is implemented for &[DFSchema] which is
widely used in the DataFusion codebase.

**Methods:**

- `nullable`: Is this column reference nullable?
- `data_type`: What is the datatype of this column?
- `metadata`: Returns the column's optional metadata.
- `data_type_and_nullable`: Return the column's datatype and nullability
- `field_from_column`



## datafusion_common::dfschema::SchemaExt

*Trait*

DataFusion-specific extensions to [`Schema`].

**Methods:**

- `equivalent_names_and_types`: This is a specialized version of Eq that ignores differences
- `logically_equivalent_names_and_types`: Returns nothing if the two schemas have the same qualified named



## datafusion_common::dfschema::ToDFSchema

*Trait*

Convenience trait to convert Schema like things to DFSchema and DFSchemaRef with fewer keystrokes

**Methods:**

- `to_dfschema`: Attempt to create a DSSchema
- `to_dfschema_ref`: Attempt to create a DSSchemaRef



## datafusion_common::dfschema::qualified_name

*Function*

```rust
fn qualified_name(qualifier: Option<&crate::TableReference>, name: &str) -> String
```



