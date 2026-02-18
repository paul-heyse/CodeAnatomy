**datafusion_common > spans**

# Module: spans

## Contents

**Structs**

- [`Location`](#location) - Represents a location, determined by a line and a column number, in the
- [`Span`](#span) - Represents an interval of characters in the original SQL query.
- [`Spans`](#spans) - A collection of [`Span`], meant to be used as a field of entities whose

---

## datafusion_common::spans::Location

*Struct*

Represents a location, determined by a line and a column number, in the
original SQL query.

**Fields:**
- `line: u64` - Line number, starting from 1.
- `column: u64` - Line column, starting from 1.

**Traits:** Eq, Copy

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`
- **From**
  - `fn from(value: sqlparser::tokenizer::Location) -> Self`
- **Clone**
  - `fn clone(self: &Self) -> Location`
- **Ord**
  - `fn cmp(self: &Self, other: &Location) -> $crate::cmp::Ordering`
- **PartialEq**
  - `fn eq(self: &Self, other: &Location) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Location) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_common::spans::Span

*Struct*

Represents an interval of characters in the original SQL query.

**Fields:**
- `start: Location`
- `end: Location`

**Methods:**

- `fn new(start: Location, end: Location) -> Self` - Creates a new [`Span`] from a start and an end [`Location`].
- `fn try_from_sqlparser_span(span: sqlparser::tokenizer::Span) -> Option<Span>` - Convert a [`Span`](sqlparser::tokenizer::Span) from the parser, into a
- `fn union(self: &Self, other: &Span) -> Span` - Returns the smallest Span that contains both `self` and `other`
- `fn union_opt(self: &Self, other: &Option<Span>) -> Span` - Same as [Span::union] for `Option<Span>`.
- `fn union_iter<I>(iter: I) -> Option<Span>` - Return the [Span::union] of all spans in the iterator.

**Traits:** Copy, Eq

**Trait Implementations:**

- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Ord**
  - `fn cmp(self: &Self, other: &Span) -> $crate::cmp::Ordering`
- **Debug**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Span`
- **PartialEq**
  - `fn eq(self: &Self, other: &Span) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Span) -> $crate::option::Option<$crate::cmp::Ordering>`



## datafusion_common::spans::Spans

*Struct*

A collection of [`Span`], meant to be used as a field of entities whose
location in the original SQL query is desired to be tracked. Sometimes an
entity can have multiple spans. e.g. if you want to track the position of
the column a that comes from SELECT 1 AS a UNION ALL SELECT 2 AS a you'll
need two spans.

**Tuple Struct**: `(Vec<Span>)`

**Methods:**

- `fn new() -> Self` - Creates a new empty [`Spans`] with no [`Span`].
- `fn first(self: &Self) -> Option<Span>` - Returns the first [`Span`], if any. This is useful when you know that
- `fn get_spans(self: &Self) -> &[Span]` - Returns a slice of the [`Span`]s.
- `fn add_span(self: & mut Self, span: Span)` - Adds a [`Span`] to the collection.
- `fn iter(self: &Self) -> impl Trait` - Iterates over the [`Span`]s.

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> Spans`
- **Ord**
  - `fn cmp(self: &Self, _other: &Self) -> Ordering`
- **Default**
  - `fn default() -> Self`
- **Hash**
  - `fn hash<H>(self: &Self, _state: & mut H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, _other: &Self) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Self) -> Option<Ordering>`



