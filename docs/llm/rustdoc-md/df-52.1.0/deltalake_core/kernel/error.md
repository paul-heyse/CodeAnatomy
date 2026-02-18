**deltalake_core > kernel > error**

# Module: kernel::error

## Contents

**Enums**

- [`Error`](#error)

**Type Aliases**

- [`DeltaResult`](#deltaresult) - A specialized [`Result`] type for Delta Lake operations.

---

## deltalake_core::kernel::error::DeltaResult

*Type Alias*: `std::result::Result<T, E>`

A specialized [`Result`] type for Delta Lake operations.



## deltalake_core::kernel::error::Error

*Enum*

**Variants:**
- `Arrow(arrow_schema::ArrowError)`
- `Generic(String)`
- `GenericError{ source: Box<dyn std::error::Error> }`
- `Parquet(parquet::errors::ParquetError)`
- `ObjectStore(object_store::Error)`
- `FileNotFound(String)`
- `MissingColumn(String)`
- `UnexpectedColumnType(String)`
- `MissingData(String)`
- `MissingVersion`
- `DeletionVector(String)`
- `Schema(String)`
- `InvalidUrl(url::ParseError)`
- `MalformedJson(serde_json::Error)`
- `MissingMetadata`
- `InvalidInvariantJson{ json_err: serde_json::error::Error, line: String }` - Error returned when the log contains invalid stats JSON.
- `InvalidGenerationExpressionJson{ json_err: serde_json::error::Error, line: String }` - Error returned when the log contains invalid stats JSON.
- `MetadataError(String)`
- `Parse(String, super::DataType)`

**Trait Implementations:**

- **From**
  - `fn from(source: arrow_schema::ArrowError) -> Self`
- **From**
  - `fn from(source: url::ParseError) -> Self`
- **Error**
  - `fn source(self: &Self) -> ::core::option::Option<&dyn ::thiserror::__private18::Error>`
- **From**
  - `fn from(source: parquet::errors::ParquetError) -> Self`
- **From**
  - `fn from(source: serde_json::Error) -> Self`
- **Display**
  - `fn fmt(self: &Self, __formatter: & mut ::core::fmt::Formatter) -> ::core::fmt::Result`
- **From**
  - `fn from(source: object_store::Error) -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



