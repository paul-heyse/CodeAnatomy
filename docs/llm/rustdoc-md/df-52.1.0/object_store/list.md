**object_store > list**

# Module: list

## Contents

**Structs**

- [`PaginatedListOptions`](#paginatedlistoptions) - Options for a paginated list request
- [`PaginatedListResult`](#paginatedlistresult) - A [`ListResult`] with optional pagination token

**Traits**

- [`PaginatedListStore`](#paginatedliststore) - A low-level interface for interacting with paginated listing APIs

---

## object_store::list::PaginatedListOptions

*Struct*

Options for a paginated list request

**Fields:**
- `offset: Option<String>` - Path to start listing from
- `delimiter: Option<std::borrow::Cow<'static, str>>` - A delimiter use to group keys with a common prefix
- `max_keys: Option<usize>` - The maximum number of paths to return
- `page_token: Option<String>` - A page token from a previous request
- `extensions: http::Extensions` - Implementation-specific extensions. Intended for use by implementations

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> PaginatedListOptions`
- **Default**
  - `fn default() -> PaginatedListOptions`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## object_store::list::PaginatedListResult

*Struct*

A [`ListResult`] with optional pagination token

**Fields:**
- `result: crate::ListResult` - The list result
- `page_token: Option<String>` - If result set truncated, the pagination token to fetch next results

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## object_store::list::PaginatedListStore

*Trait*

A low-level interface for interacting with paginated listing APIs

Most use-cases should prefer [`ObjectStore::list`] as this is supported by more
backends, including [`LocalFileSystem`], however, [`PaginatedListStore`] can be
used where stateless pagination or non-path segment based listing is required

[`ObjectStore::list`]: crate::ObjectStore::list
[`LocalFileSystem`]: crate::local::LocalFileSystem

**Methods:**

- `list_paginated`: Perform a paginated list request



