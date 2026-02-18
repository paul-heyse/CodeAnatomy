**datafusion_common > diagnostic**

# Module: diagnostic

## Contents

**Structs**

- [`Diagnostic`](#diagnostic) - Additional contextual information intended for end users, to help them
- [`DiagnosticHelp`](#diagnostichelp) - A "help" enriches a [`Diagnostic`] with extra information, possibly
- [`DiagnosticNote`](#diagnosticnote) - A note enriches a [`Diagnostic`] with extra information, possibly referring

**Enums**

- [`DiagnosticKind`](#diagnostickind) - A [`Diagnostic`] can either be a hard error that prevents the query from

---

## datafusion_common::diagnostic::Diagnostic

*Struct*

Additional contextual information intended for end users, to help them
understand what went wrong by providing human-readable messages, and
locations in the source query that relate to the error in some way.

You can think of a single [`Diagnostic`] as a single "block" of output from
rustc. i.e. either an error or a warning, optionally with some notes and
help messages.

Example:

```rust
# use datafusion_common::{Location, Span, Diagnostic};
let span = Some(Span {
    start: Location { line: 2, column: 1 },
    end: Location {
        line: 4,
        column: 15,
    },
});
let diagnostic = Diagnostic::new_error("Something went wrong", span)
    .with_help("Have you tried turning it on and off again?", None);
```

**Fields:**
- `kind: DiagnosticKind`
- `message: String`
- `span: Option<crate::Span>`
- `notes: Vec<DiagnosticNote>`
- `helps: Vec<DiagnosticHelp>`

**Methods:**

- `fn new_error<impl Into<String>>(message: impl Trait, span: Option<Span>) -> Self` - Creates a new [`Diagnostic`] for a fatal error that prevents the SQL
- `fn new_warning<impl Into<String>>(message: impl Trait, span: Option<Span>) -> Self` - Creates a new [`Diagnostic`] for a NON-fatal warning, such as a
- `fn add_note<impl Into<String>>(self: & mut Self, message: impl Trait, span: Option<Span>)` - Adds a "note" to the [`Diagnostic`], which can have zero or many. A "note"
- `fn add_help<impl Into<String>>(self: & mut Self, message: impl Trait, span: Option<Span>)` - Adds a "help" to the [`Diagnostic`], which can have zero or many. A
- `fn with_note<impl Into<String>>(self: Self, message: impl Trait, span: Option<Span>) -> Self` - Like [`Diagnostic::add_note`], but returns `self` to allow chaining.
- `fn with_help<impl Into<String>>(self: Self, message: impl Trait, span: Option<Span>) -> Self` - Like [`Diagnostic::add_help`], but returns `self` to allow chaining.

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Diagnostic`



## datafusion_common::diagnostic::DiagnosticHelp

*Struct*

A "help" enriches a [`Diagnostic`] with extra information, possibly
referring to different locations in the original SQL query, that helps the
user understand how they might fix the error or warning.

Example:
SELECT id, name FROM users GROUP BY id
Help: Add 'name' here                 ^^^^

**Fields:**
- `message: String`
- `span: Option<crate::Span>`

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> DiagnosticHelp`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_common::diagnostic::DiagnosticKind

*Enum*

A [`Diagnostic`] can either be a hard error that prevents the query from
being planned and executed, or a warning that indicates potential issues,
performance problems, or causes for unexpected results, but is non-fatal.
This enum expresses these two possibilities.

**Variants:**
- `Error`
- `Warning`

**Traits:** Eq, Copy

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &DiagnosticKind) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> DiagnosticKind`



## datafusion_common::diagnostic::DiagnosticNote

*Struct*

A note enriches a [`Diagnostic`] with extra information, possibly referring
to different locations in the original SQL query, that helps contextualize
the error and helps the end user understand why it occurred.

Example:
SELECT id, name FROM users GROUP BY id
Note:      ^^^^ 'name' is not in the GROUP BY clause

**Fields:**
- `message: String`
- `span: Option<crate::Span>`

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> DiagnosticNote`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



