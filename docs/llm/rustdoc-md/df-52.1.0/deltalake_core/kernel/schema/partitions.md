**deltalake_core > kernel > schema > partitions**

# Module: kernel::schema::partitions

## Contents

**Structs**

- [`DeltaTablePartition`](#deltatablepartition) - A Struct DeltaTablePartition used to represent a partition of a DeltaTable.
- [`PartitionFilter`](#partitionfilter) - A Struct used for filtering a DeltaTable partition by key and value.

**Enums**

- [`PartitionValue`](#partitionvalue) - A Enum used for selecting the partition value operation when filtering a DeltaTable partition.

**Constants**

- [`NULL_PARTITION_VALUE_DATA_PATH`](#null_partition_value_data_path) - A special value used in Hive to represent the null partition in partitioned tables

---

## deltalake_core::kernel::schema::partitions::DeltaTablePartition

*Struct*

A Struct DeltaTablePartition used to represent a partition of a DeltaTable.

**Fields:**
- `key: String` - The key of the DeltaTable partition.
- `value: delta_kernel::expressions::Scalar` - The value of the DeltaTable partition.

**Methods:**

- `fn from_partition_value(partition_value: (&str, &Scalar)) -> Self` - Create a DeltaTable partition from a Tuple of (key, value).

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> DeltaTablePartition`
- **TryFrom**
  - `fn try_from(partition: &str) -> Result<Self, DeltaTableError>` - Try to create a DeltaTable partition from a HivePartition string.
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &DeltaTablePartition) -> bool`



## deltalake_core::kernel::schema::partitions::NULL_PARTITION_VALUE_DATA_PATH

*Constant*: `&str`

A special value used in Hive to represent the null partition in partitioned tables



## deltalake_core::kernel::schema::partitions::PartitionFilter

*Struct*

A Struct used for filtering a DeltaTable partition by key and value.

**Fields:**
- `key: String` - The key of the PartitionFilter
- `value: PartitionValue` - The value of the PartitionFilter

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &PartitionFilter) -> bool`
- **Serialize**
  - `fn serialize<S>(self: &Self, serializer: S) -> Result<<S as >::Ok, <S as >::Error>`
- **TryFrom**
  - `fn try_from(filter: (&str, &str, &str)) -> Result<Self, DeltaTableError>` - Try to create a PartitionFilter from a Tuple of (key, operation, value).
- **TryFrom**
  - `fn try_from(filter: (&str, &str, &[&str])) -> Result<Self, DeltaTableError>` - Try to create a PartitionFilter from a Tuple of (key, operation, list(value)).
- **Clone**
  - `fn clone(self: &Self) -> PartitionFilter`



## deltalake_core::kernel::schema::partitions::PartitionValue

*Enum*

A Enum used for selecting the partition value operation when filtering a DeltaTable partition.

**Variants:**
- `Equal(String)` - The partition value with the equal operator
- `NotEqual(String)` - The partition value with the not equal operator
- `GreaterThan(String)` - The partition value with the greater than operator
- `GreaterThanOrEqual(String)` - The partition value with the greater than or equal operator
- `LessThan(String)` - The partition value with the less than operator
- `LessThanOrEqual(String)` - The partition value with the less than or equal operator
- `In(Vec<String>)` - The partition values with the in operator
- `NotIn(Vec<String>)` - The partition values with the not in operator

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> PartitionValue`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &PartitionValue) -> bool`



