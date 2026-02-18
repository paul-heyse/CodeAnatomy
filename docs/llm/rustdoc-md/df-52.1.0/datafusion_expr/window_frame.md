**datafusion_expr > window_frame**

# Module: window_frame

## Contents

**Structs**

- [`WindowFrame`](#windowframe) - The frame specification determines which output rows are read by an aggregate

**Enums**

- [`WindowFrameBound`](#windowframebound) - There are five ways to describe starting and ending frame boundaries:
- [`WindowFrameUnits`](#windowframeunits) - There are three frame types: ROWS, GROUPS, and RANGE. The frame type determines how the

---

## datafusion_expr::window_frame::WindowFrame

*Struct*

The frame specification determines which output rows are read by an aggregate
window function. The ending frame boundary can be omitted if the `BETWEEN`
and `AND` keywords that surround the starting frame boundary are also omitted,
in which case the ending frame boundary defaults to `CURRENT ROW`.

**Fields:**
- `units: WindowFrameUnits` - Frame type - either `ROWS`, `RANGE` or `GROUPS`
- `start_bound: WindowFrameBound` - Starting frame boundary
- `end_bound: WindowFrameBound` - Ending frame boundary

**Methods:**

- `fn new(order_by: Option<bool>) -> Self` - Creates a new, default window frame (with the meaning of default
- `fn reverse(self: &Self) -> Self` - Get reversed window frame. For example
- `fn is_causal(self: &Self) -> bool` - Get whether window frame is causal
- `fn new_bounds(units: WindowFrameUnits, start_bound: WindowFrameBound, end_bound: WindowFrameBound) -> Self` - Initializes window frame from units (type), start bound and end bound.
- `fn regularize_order_bys(self: &Self, order_by: & mut Vec<Sort>) -> Result<()>` - Regularizes the ORDER BY clause of the window frame.
- `fn can_accept_multi_orderby(self: &Self) -> bool` - Returns whether the window frame can accept multiple ORDER BY expressions.
- `fn is_ever_expanding(self: &Self) -> bool` - Is the window frame ever-expanding (it always grows in the superset sense).

**Traits:** Eq

**Trait Implementations:**

- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &WindowFrame) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut Formatter) -> fmt::Result`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Display**
  - `fn fmt(self: &Self, f: & mut Formatter) -> fmt::Result`
- **TryFrom**
  - `fn try_from(value: ast::WindowFrame) -> Result<Self>`
- **Clone**
  - `fn clone(self: &Self) -> WindowFrame`
- **PartialEq**
  - `fn eq(self: &Self, other: &WindowFrame) -> bool`



## datafusion_expr::window_frame::WindowFrameBound

*Enum*

There are five ways to describe starting and ending frame boundaries:

1. UNBOUNDED PRECEDING
2. `<expr>` PRECEDING
3. CURRENT ROW
4. `<expr>` FOLLOWING
5. UNBOUNDED FOLLOWING

**Variants:**
- `Preceding(datafusion_common::ScalarValue)` - 1. UNBOUNDED PRECEDING
- `CurrentRow` - 3. The current row.
- `Following(datafusion_common::ScalarValue)` - 4. This is the same as "`<expr>` PRECEDING" except that the boundary is `<expr>` units after the

**Methods:**

- `fn is_unbounded(self: &Self) -> bool`

**Traits:** Eq

**Trait Implementations:**

- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &WindowFrameBound) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Display**
  - `fn fmt(self: &Self, f: & mut Formatter) -> fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> WindowFrameBound`
- **PartialEq**
  - `fn eq(self: &Self, other: &WindowFrameBound) -> bool`



## datafusion_expr::window_frame::WindowFrameUnits

*Enum*

There are three frame types: ROWS, GROUPS, and RANGE. The frame type determines how the
starting and ending boundaries of the frame are measured.

**Variants:**
- `Rows` - The ROWS frame type means that the starting and ending boundaries for the frame are
- `Range` - The RANGE frame type requires that the ORDER BY clause of the window have exactly one
- `Groups` - The GROUPS frame type means that the starting and ending boundaries are determine

**Traits:** Eq, Copy

**Trait Implementations:**

- **Display**
  - `fn fmt(self: &Self, f: & mut Formatter) -> fmt::Result`
- **From**
  - `fn from(value: ast::WindowFrameUnits) -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &WindowFrameUnits) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &WindowFrameUnits) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Clone**
  - `fn clone(self: &Self) -> WindowFrameUnits`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



