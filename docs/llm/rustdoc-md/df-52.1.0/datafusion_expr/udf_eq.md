**datafusion_expr > udf_eq**

# Module: udf_eq

## Contents

**Structs**

- [`UdfEq`](#udfeq) - A wrapper around a pointer to UDF that implements `Eq` and `Hash` delegating to

---

## datafusion_expr::udf_eq::UdfEq

*Struct*

A wrapper around a pointer to UDF that implements `Eq` and `Hash` delegating to
corresponding methods on the UDF trait.

If you want to just compare pointers for equality, use [`super::ptr_eq::PtrEq`].

**Generic Parameters:**
- Ptr

**Tuple Struct**: `()`

**Traits:** Eq

**Trait Implementations:**

- **From**
  - `fn from(ptr: Ptr) -> Self`
- **Clone**
  - `fn clone(self: &Self) -> UdfEq<Ptr>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **Deref**
  - `fn deref(self: &Self) -> &<Self as >::Target`
- **PartialEq**
  - `fn eq(self: &Self, other: &Self) -> bool`
- **Hash**
  - `fn hash<H>(self: &Self, state: & mut H)`



