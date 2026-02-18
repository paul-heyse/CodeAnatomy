**datafusion_common > error**

# Module: error

## Contents

**Structs**

- [`DataFusionErrorBuilder`](#datafusionerrorbuilder) - A builder for [`DataFusionError`]

**Enums**

- [`DataFusionError`](#datafusionerror) - DataFusion error
- [`SchemaError`](#schemaerror) - Schema-related errors

**Functions**

- [`add_possible_columns_to_diag`](#add_possible_columns_to_diag)
- [`field_not_found`](#field_not_found) - Create a "field not found" DataFusion::SchemaError
- [`unqualified_field_not_found`](#unqualified_field_not_found) - Convenience wrapper over [`field_not_found`] for when there is no qualifier

**Type Aliases**

- [`GenericError`](#genericerror) - Error type for generic operations that could result in DataFusionError::External
- [`Result`](#result) - Result type for operations that could result in an [DataFusionError]
- [`SharedResult`](#sharedresult) - Result type for operations that could result in an [DataFusionError] and needs to be shared (wrapped into `Arc`).

---

## datafusion_common::error::DataFusionError

*Enum*

DataFusion error

**Variants:**
- `ArrowError(Box<arrow::error::ArrowError>, Option<String>)` - Error returned by arrow.
- `ParquetError(Box<parquet::errors::ParquetError>)` - Error when reading / writing Parquet data.
- `ObjectStore(Box<object_store::Error>)` - Error when reading / writing to / from an object_store (e.g. S3 or LocalFile)
- `IoError(io::Error)` - Error when an I/O operation fails
- `SQL(Box<sqlparser::parser::ParserError>, Option<String>)` - Error when SQL is syntactically incorrect.
- `NotImplemented(String)` - Error when a feature is not yet implemented.
- `Internal(String)` - Error due to bugs in DataFusion
- `Plan(String)` - Error during planning of the query.
- `Configuration(String)` - Error for invalid or unsupported configuration options.
- `SchemaError(Box<SchemaError>, Box<Option<String>>)` - Error when there is a problem with the query related to schema.
- `Execution(String)` - Error during execution of the query.
- `ExecutionJoin(Box<tokio::task::JoinError>)` - [`JoinError`] during execution of the query.
- `ResourcesExhausted(String)` - Error when resources (such as memory of scratch disk space) are exhausted.
- `External(GenericError)` - Errors originating from outside DataFusion's core codebase.
- `Context(String, Box<DataFusionError>)` - Error with additional context
- `Substrait(String)` - Errors from either mapping LogicalPlans to/from Substrait plans
- `Diagnostic(Box<crate::Diagnostic>, Box<DataFusionError>)` - Error wrapped together with additional contextual information intended
- `Collection(Vec<DataFusionError>)` - A collection of one or more [`DataFusionError`]. Useful in cases where
- `Shared(std::sync::Arc<DataFusionError>)` - A [`DataFusionError`] which shares an underlying [`DataFusionError`].
- `Ffi(String)` - An error that originated during a foreign function interface call.

**Methods:**

- `fn find_root(self: &Self) -> &Self` - Get deepest underlying [`DataFusionError`]
- `fn context<impl Into<String>>(self: Self, description: impl Trait) -> Self` - wraps self in Self::Context with a description
- `fn strip_backtrace(self: &Self) -> String` - Strips backtrace out of the error message
- `fn get_back_trace() -> String` - To enable optional rust backtrace in DataFusion:
- `fn builder() -> DataFusionErrorBuilder` - Return a [`DataFusionErrorBuilder`] to build a [`DataFusionError`]
- `fn message(self: &Self) -> Cow<str>`
- `fn with_diagnostic(self: Self, diagnostic: Diagnostic) -> Self` - Wraps the error with contextual information intended for end users
- `fn with_diagnostic_fn<F>(self: Self, f: F) -> Self` - Wraps the error with contextual information intended for end users.
- `fn diagnostic(self: &Self) -> Option<&Diagnostic>` - Gets the [`Diagnostic`] associated with the error, if any. If there is
- `fn iter(self: &Self) -> impl Trait` - Return an iterator over this [`DataFusionError`] and any other

**Trait Implementations:**

- **From**
  - `fn from(e: object_store::Error) -> Self`
- **From**
  - `fn from(e: object_store::path::Error) -> Self`
- **From**
  - `fn from(e: io::Error) -> Self`
- **Display**
  - `fn fmt(self: &Self, f: & mut Formatter) -> std::fmt::Result`
- **From**
  - `fn from(e: &Arc<DataFusionError>) -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **From**
  - `fn from(e: ParserError) -> Self`
- **From**
  - `fn from(e: ArrowError) -> Self`
- **Error**
  - `fn source(self: &Self) -> Option<&dyn Error>`
- **From**
  - `fn from(e: ParquetError) -> Self`
- **From**
  - `fn from(_e: std::fmt::Error) -> Self`
- **From**
  - `fn from(err: GenericError) -> Self`



## datafusion_common::error::DataFusionErrorBuilder

*Struct*

A builder for [`DataFusionError`]

This builder can be used to collect multiple errors and return them as a
[`DataFusionError::Collection`].

# Example: no errors
```
# use datafusion_common::DataFusionError;
let mut builder = DataFusionError::builder();
// ok_or returns the value if no errors have been added
assert_eq!(builder.error_or(42).unwrap(), 42);
```

# Example: with errors
```
# use datafusion_common::{assert_contains, DataFusionError};
let mut builder = DataFusionError::builder();
builder.add_error(DataFusionError::Internal("foo".to_owned()));
// ok_or returns the value if no errors have been added
assert_contains!(
    builder.error_or(42).unwrap_err().to_string(),
    "Internal error: foo"
);
```

**Tuple Struct**: `()`

**Methods:**

- `fn new() -> Self` - Create a new [`DataFusionErrorBuilder`]
- `fn add_error(self: & mut Self, error: DataFusionError)` - Add an error to the in progress list
- `fn with_error(self: Self, error: DataFusionError) -> Self` - Add an error to the in progress list, returning the builder
- `fn error_or<T>(self: Self, ok: T) -> Result<T, DataFusionError>` - Returns `Ok(ok)` if no errors were added to the builder,

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Default**
  - `fn default() -> DataFusionErrorBuilder`



## datafusion_common::error::GenericError

*Type Alias*: `Box<dyn Error>`

Error type for generic operations that could result in DataFusionError::External



## datafusion_common::error::Result

*Type Alias*: `result::Result<T, E>`

Result type for operations that could result in an [DataFusionError]



## datafusion_common::error::SchemaError

*Enum*

Schema-related errors

**Variants:**
- `AmbiguousReference{ field: Box<crate::Column> }` - Schema contains a (possibly) qualified and unqualified field with same unqualified name
- `DuplicateQualifiedField{ qualifier: Box<crate::TableReference>, name: String }` - Schema contains duplicate qualified field name
- `DuplicateUnqualifiedField{ name: String }` - Schema contains duplicate unqualified field name
- `FieldNotFound{ field: Box<crate::Column>, valid_fields: Vec<crate::Column> }` - No field with this name

**Traits:** Error

**Trait Implementations:**

- **Display**
  - `fn fmt(self: &Self, f: & mut Formatter) -> std::fmt::Result`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_common::error::SharedResult

*Type Alias*: `result::Result<T, std::sync::Arc<DataFusionError>>`

Result type for operations that could result in an [DataFusionError] and needs to be shared (wrapped into `Arc`).



## datafusion_common::error::add_possible_columns_to_diag

*Function*

```rust
fn add_possible_columns_to_diag(diagnostic: & mut crate::Diagnostic, field: &crate::Column, valid_fields: &[crate::Column])
```



## datafusion_common::error::field_not_found

*Function*

Create a "field not found" DataFusion::SchemaError

```rust
fn field_not_found<R>(qualifier: Option<R>, name: &str, schema: &crate::DFSchema) -> DataFusionError
```



## datafusion_common::error::unqualified_field_not_found

*Function*

Convenience wrapper over [`field_not_found`] for when there is no qualifier

```rust
fn unqualified_field_not_found(name: &str, schema: &crate::DFSchema) -> DataFusionError
```



