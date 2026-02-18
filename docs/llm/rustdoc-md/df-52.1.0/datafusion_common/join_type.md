**datafusion_common > join_type**

# Module: join_type

## Contents

**Enums**

- [`JoinConstraint`](#joinconstraint) - Join constraint
- [`JoinSide`](#joinside) - Join side.
- [`JoinType`](#jointype) - Join type

---

## datafusion_common::join_type::JoinConstraint

*Enum*

Join constraint

**Variants:**
- `On` - Join ON
- `Using` - Join USING

**Traits:** Eq, Copy

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &JoinConstraint) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &JoinConstraint) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Clone**
  - `fn clone(self: &Self) -> JoinConstraint`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_common::join_type::JoinSide

*Enum*

Join side.
Stores the referred table side during calculations

**Variants:**
- `Left` - Left side of the join
- `Right` - Right side of the join
- `None` - Neither side of the join, used for Mark joins where the mark column does not belong to

**Methods:**

- `fn negate(self: &Self) -> Self` - Inverse the join side

**Traits:** Copy

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &JoinSide) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> JoinSide`
- **Display**
  - `fn fmt(self: &Self, f: & mut Formatter) -> fmt::Result`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_common::join_type::JoinType

*Enum*

Join type

**Variants:**
- `Inner` - Inner Join - Returns only rows where there is a matching value in both tables based on the join condition.
- `Left` - Left Join - Returns all rows from the left table and matching rows from the right table.
- `Right` - Right Join - Returns all rows from the right table and matching rows from the left table.
- `Full` - Full Join (also called Full Outer Join) - Returns all rows from both tables, matching rows where possible.
- `LeftSemi` - Left Semi Join - Returns rows from the left table that have matching rows in the right table.
- `RightSemi` - Right Semi Join - Returns rows from the right table that have matching rows in the left table.
- `LeftAnti` - Left Anti Join - Returns rows from the left table that do not have a matching row in the right table.
- `RightAnti` - Right Anti Join - Returns rows from the right table that do not have a matching row in the left table.
- `LeftMark` - Left Mark join
- `RightMark` - Right Mark Join

**Methods:**

- `fn is_outer(self: Self) -> bool`
- `fn swap(self: &Self) -> JoinType` - Returns the `JoinType` if the (2) inputs were swapped
- `fn supports_swap(self: &Self) -> bool` - Does the join type support swapping inputs?

**Traits:** Eq, Copy

**Trait Implementations:**

- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Display**
  - `fn fmt(self: &Self, f: & mut Formatter) -> fmt::Result`
- **FromStr**
  - `fn from_str(s: &str) -> Result<Self>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &JoinType) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &JoinType) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Clone**
  - `fn clone(self: &Self) -> JoinType`



