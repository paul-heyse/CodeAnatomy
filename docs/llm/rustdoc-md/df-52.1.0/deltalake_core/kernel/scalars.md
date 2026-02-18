**deltalake_core > kernel > scalars**

# Module: kernel::scalars

## Contents

**Traits**

- [`ScalarExt`](#scalarext) - Auxiliary methods for dealing with kernel scalars

---

## deltalake_core::kernel::scalars::ScalarExt

*Trait*

Auxiliary methods for dealing with kernel scalars

**Methods:**

- `serialize`: Serialize to string
- `serialize_encoded`: Serialize to string for use in hive partition file names
- `from_array`: Create a [`Scalar`] from an arrow array row
- `to_json`: Serialize as serde_json::Value



