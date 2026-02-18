**datafusion_common > null_equality**

# Module: null_equality

## Contents

**Enums**

- [`NullEquality`](#nullequality) - Represents the behavior for null values when evaluating equality. Currently, its primary use

---

## datafusion_common::null_equality::NullEquality

*Enum*

Represents the behavior for null values when evaluating equality. Currently, its primary use
case is to define the behavior of joins for null values.

# Examples

The following table shows the expected equality behavior for `NullEquality`.

| A    | B    | NullEqualsNothing | NullEqualsNull |
|------|------|-------------------|----------------|
| NULL | NULL | false             | true           |
| NULL | 'b'  | false             | false          |
| 'a'  | NULL | false             | false          |
| 'a'  | 'b'  | false             | false          |

# Order

The order on this type represents the "restrictiveness" of the behavior. The more restrictive
a behavior is, the fewer elements are considered to be equal to null.
[NullEquality::NullEqualsNothing] represents the most restrictive behavior.

This mirrors the old order with `null_equals_null` booleans, as `false` indicated that
`null != null`.

**Variants:**
- `NullEqualsNothing` - Null is *not* equal to anything (`null != null`)
- `NullEqualsNull` - Null is equal to null (`null == null`)

**Traits:** Eq, Copy

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &NullEquality) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &NullEquality) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Clone**
  - `fn clone(self: &Self) -> NullEquality`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



