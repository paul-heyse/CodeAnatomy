**datafusion_expr_common > dyn_eq**

# Module: dyn_eq

## Contents

**Traits**

- [`DynEq`](#dyneq) - A dyn-compatible version of [`Eq`] trait.
- [`DynHash`](#dynhash) - A dyn-compatible version of [`Hash`] trait.

---

## datafusion_expr_common::dyn_eq::DynEq

*Trait*

A dyn-compatible version of [`Eq`] trait.
The implementation constraints for this trait are the same as for [`Eq`]:
the implementation must be reflexive, symmetric, and transitive.
Additionally, if two values can be compared with [`DynEq`] and [`PartialEq`] then
they must be [`DynEq`]-equal if and only if they are [`PartialEq`]-equal.
It is therefore strongly discouraged to implement this trait for types
that implement `PartialEq<Other>` or `Eq<Other>` for any type `Other` other than `Self`.

Note: This trait should not be implemented directly. Implement `Eq` and `Any` and use
the blanket implementation.

**Methods:**

- `dyn_eq`



## datafusion_expr_common::dyn_eq::DynHash

*Trait*

A dyn-compatible version of [`Hash`] trait.
If two values are equal according to [`DynEq`], they must produce the same hash value.

Note: This trait should not be implemented directly. Implement `Hash` and `Any` and use
the blanket implementation.

**Methods:**

- `dyn_hash`



