**datafusion_physical_expr_common**

# Module: datafusion_physical_expr_common

## Contents

**Modules**

- [`binary_map`](#binary_map) - [`ArrowBytesMap`] and [`ArrowBytesSet`] for storing maps/sets of values from
- [`binary_view_map`](#binary_view_map) - [`ArrowBytesViewMap`] and [`ArrowBytesViewSet`] for storing maps/sets of values from
- [`datum`](#datum)
- [`metrics`](#metrics) - Metrics for recording information about execution
- [`physical_expr`](#physical_expr)
- [`sort_expr`](#sort_expr) - Sort expressions
- [`tree_node`](#tree_node) - This module provides common traits for visiting or rewriting tree nodes easily.
- [`utils`](#utils)

---

## Module: binary_map

[`ArrowBytesMap`] and [`ArrowBytesSet`] for storing maps/sets of values from
StringArray / LargeStringArray / BinaryArray / LargeBinaryArray.



## Module: binary_view_map

[`ArrowBytesViewMap`] and [`ArrowBytesViewSet`] for storing maps/sets of values from
`StringViewArray`/`BinaryViewArray`.
Much of the code is from `binary_map.rs`, but with simpler implementation because we directly use the
[`GenericByteViewBuilder`].



## Module: datum



## Module: metrics

Metrics for recording information about execution



## Module: physical_expr



## Module: sort_expr

Sort expressions



## Module: tree_node

This module provides common traits for visiting or rewriting tree nodes easily.



## Module: utils



