**deltalake_core > delta_datafusion > engine**

# Module: delta_datafusion::engine

## Contents

**Structs**

- [`DataFusionEngine`](#datafusionengine) - A Datafusion based Kernel Engine

---

## deltalake_core::delta_datafusion::engine::DataFusionEngine

*Struct*

A Datafusion based Kernel Engine

**Methods:**

- `fn new_from_session(session: &dyn Session) -> Arc<Self>`
- `fn new_from_context(ctx: Arc<TaskContext>) -> Arc<Self>`
- `fn new(ctx: Arc<TaskContext>, handle: Handle) -> Self`

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> DataFusionEngine`
- **Engine**
  - `fn evaluation_handler(self: &Self) -> Arc<dyn EvaluationHandler>`
  - `fn storage_handler(self: &Self) -> Arc<dyn StorageHandler>`
  - `fn json_handler(self: &Self) -> Arc<dyn JsonHandler>`
  - `fn parquet_handler(self: &Self) -> Arc<dyn ParquetHandler>`



