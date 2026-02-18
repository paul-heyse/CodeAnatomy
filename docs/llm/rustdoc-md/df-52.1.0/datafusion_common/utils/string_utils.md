**datafusion_common > utils > string_utils**

# Module: utils::string_utils

## Contents

**Functions**

- [`string_array_to_vec`](#string_array_to_vec) - Convenient function to convert an Arrow string array to a vector of strings

---

## datafusion_common::utils::string_utils::string_array_to_vec

*Function*

Convenient function to convert an Arrow string array to a vector of strings

```rust
fn string_array_to_vec(array: &dyn Array) -> Vec<Option<&str>>
```



