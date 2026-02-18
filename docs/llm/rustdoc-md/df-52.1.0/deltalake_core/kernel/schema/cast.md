**deltalake_core > kernel > schema > cast**

# Module: kernel::schema::cast

## Contents

**Functions**

- [`cast_record_batch`](#cast_record_batch) - Cast recordbatch to a new target_schema, by casting each column array

---

## deltalake_core::kernel::schema::cast::cast_record_batch

*Function*

Cast recordbatch to a new target_schema, by casting each column array

```rust
fn cast_record_batch(batch: &arrow_array::RecordBatch, target_schema: arrow_schema::SchemaRef, safe: bool, add_missing: bool) -> crate::DeltaResult<arrow_array::RecordBatch>
```



