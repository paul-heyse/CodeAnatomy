**deltalake_core > delta_datafusion > cdf**

# Module: delta_datafusion::cdf

## Contents

**Modules**

- [`scan`](#scan)

**Traits**

- [`FileAction`](#fileaction) - This trait defines a generic set of operations used by CDF Reader

**Constants**

- [`CHANGE_TYPE_COL`](#change_type_col) - Change type column name
- [`COMMIT_TIMESTAMP_COL`](#commit_timestamp_col) - Commit Timestamp column name
- [`COMMIT_VERSION_COL`](#commit_version_col) - Commit version column name

---

## deltalake_core::delta_datafusion::cdf::CHANGE_TYPE_COL

*Constant*: `&str`

Change type column name



## deltalake_core::delta_datafusion::cdf::COMMIT_TIMESTAMP_COL

*Constant*: `&str`

Commit Timestamp column name



## deltalake_core::delta_datafusion::cdf::COMMIT_VERSION_COL

*Constant*: `&str`

Commit version column name



## deltalake_core::delta_datafusion::cdf::FileAction

*Trait*

This trait defines a generic set of operations used by CDF Reader

**Methods:**

- `partition_values`: Adds partition values
- `path`: Physical Path to the data
- `size`: Byte size of the physical file



## Module: scan



