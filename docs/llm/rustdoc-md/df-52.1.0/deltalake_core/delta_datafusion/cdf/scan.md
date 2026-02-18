**deltalake_core > delta_datafusion > cdf > scan**

# Module: delta_datafusion::cdf::scan

## Contents

**Structs**

- [`DeltaCdfTableProvider`](#deltacdftableprovider)

---

## deltalake_core::delta_datafusion::cdf::scan::DeltaCdfTableProvider

*Struct*

**Methods:**

- `fn try_new(cdf_builder: CdfLoadBuilder) -> DeltaResult<Self>` - Build a DeltaCDFTableProvider

**Trait Implementations:**

- **TableProvider**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn schema(self: &Self) -> SchemaRef`
  - `fn table_type(self: &Self) -> TableType`
  - `fn scan(self: &'life0 Self, session: &'life1 dyn Session, projection: Option<&'life2 Vec<usize>>, filters: &'life3 [Expr], limit: Option<usize>) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn supports_filters_pushdown(self: &Self, filter: &[&Expr]) -> DataFusionResult<Vec<TableProviderFilterPushDown>>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



