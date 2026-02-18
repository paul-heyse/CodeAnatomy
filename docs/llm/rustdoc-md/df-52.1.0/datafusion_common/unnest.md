**datafusion_common > unnest**

# Module: unnest

## Contents

**Structs**

- [`RecursionUnnestOption`](#recursionunnestoption) - Instruction on how to unnest a column (mostly with a list type)
- [`UnnestOptions`](#unnestoptions) - Options for unnesting a column that contains a list type,

---

## datafusion_common::unnest::RecursionUnnestOption

*Struct*

Instruction on how to unnest a column (mostly with a list type)
such as how to name the output, and how many level it should be unnested

**Fields:**
- `input_column: crate::Column`
- `output_column: crate::Column`
- `depth: usize`

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> RecursionUnnestOption`
- **PartialEq**
  - `fn eq(self: &Self, other: &RecursionUnnestOption) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &RecursionUnnestOption) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_common::unnest::UnnestOptions

*Struct*

Options for unnesting a column that contains a list type,
replicating values in the other, non nested rows.

Conceptually this operation is like joining each row with all the
values in the list column.

If `preserve_nulls` is false, nulls and empty lists
from the input column are not carried through to the output. This
is the default behavior for other systems such as ClickHouse and
DuckDB

If `preserve_nulls` is true (the default), nulls from the input
column are carried through to the output.

# Examples

## `Unnest(c1)`, preserve_nulls: false
```text
     ┌─────────┐ ┌─────┐                ┌─────────┐ ┌─────┐
     │ {1, 2}  │ │  A  │   Unnest       │    1    │ │  A  │
     ├─────────┤ ├─────┤                ├─────────┤ ├─────┤
     │  null   │ │  B  │                │    2    │ │  A  │
     ├─────────┤ ├─────┤ ────────────▶  ├─────────┤ ├─────┤
     │   {}    │ │  D  │                │    3    │ │  E  │
     ├─────────┤ ├─────┤                └─────────┘ └─────┘
     │   {3}   │ │  E  │                    c1        c2
     └─────────┘ └─────┘
       c1         c2
```

## `Unnest(c1)`, preserve_nulls: true
```text
     ┌─────────┐ ┌─────┐                ┌─────────┐ ┌─────┐
     │ {1, 2}  │ │  A  │   Unnest       │    1    │ │  A  │
     ├─────────┤ ├─────┤                ├─────────┤ ├─────┤
     │  null   │ │  B  │                │    2    │ │  A  │
     ├─────────┤ ├─────┤ ────────────▶  ├─────────┤ ├─────┤
     │   {}    │ │  D  │                │  null   │ │  B  │
     ├─────────┤ ├─────┤                ├─────────┤ ├─────┤
     │   {3}   │ │  E  │                │    3    │ │  E  │
     └─────────┘ └─────┘                └─────────┘ └─────┘
       c1         c2                        c1        c2
```

`recursions` instruct how a column should be unnested (e.g unnesting a column multiple
time, with depth = 1 and depth = 2). Any unnested column not being mentioned inside this
options is inferred to be unnested with depth = 1

**Fields:**
- `preserve_nulls: bool` - Should nulls in the input be preserved? Defaults to true
- `recursions: Vec<RecursionUnnestOption>` - If specific columns need to be unnested multiple times (e.g at different depth),

**Methods:**

- `fn new() -> Self` - Create a new [`UnnestOptions`] with default values
- `fn with_preserve_nulls(self: Self, preserve_nulls: bool) -> Self` - Set the behavior with nulls in the input as described on
- `fn with_recursions(self: Self, recursion: RecursionUnnestOption) -> Self` - Set the recursions for the unnest operation

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> UnnestOptions`
- **PartialEq**
  - `fn eq(self: &Self, other: &UnnestOptions) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &UnnestOptions) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Default**
  - `fn default() -> Self`



