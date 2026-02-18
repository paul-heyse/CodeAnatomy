**datafusion_datasource > url**

# Module: url

## Contents

**Structs**

- [`ListingTableUrl`](#listingtableurl) - A parsed URL identifying files for a listing table, see [`ListingTableUrl::parse`]

---

## datafusion_datasource::url::ListingTableUrl

*Struct*

A parsed URL identifying files for a listing table, see [`ListingTableUrl::parse`]
for more information on the supported expressions

**Methods:**

- `fn parse<impl AsRef<str>>(s: impl Trait) -> Result<Self>` - Parse a provided string as a `ListingTableUrl`
- `fn try_new(url: Url, glob: Option<Pattern>) -> Result<Self>` - Creates a new [`ListingTableUrl`] from a url and optional glob expression
- `fn scheme(self: &Self) -> &str` - Returns the URL scheme
- `fn prefix(self: &Self) -> &Path` - Return the URL path not excluding any glob expression
- `fn contains(self: &Self, path: &Path, ignore_subdirectory: bool) -> bool` - Returns `true` if `path` matches this [`ListingTableUrl`]
- `fn is_collection(self: &Self) -> bool` - Returns `true` if `path` refers to a collection of objects
- `fn file_extension(self: &Self) -> Option<&str>` - Returns the file extension of the last path segment if it exists
- `fn strip_prefix<'a, 'b>(self: &'a Self, path: &'b Path) -> Option<impl Trait>` - Strips the prefix of this [`ListingTableUrl`] from the provided path, returning
- `fn list_prefixed_files<'a>(self: &'a Self, ctx: &'a dyn Session, store: &'a dyn ObjectStore, prefix: Option<Path>, file_extension: &'a str) -> Result<BoxStream<'a, Result<ObjectMeta>>>` - List all files identified by this [`ListingTableUrl`] for the provided `file_extension`,
- `fn list_all_files<'a>(self: &'a Self, ctx: &'a dyn Session, store: &'a dyn ObjectStore, file_extension: &'a str) -> Result<BoxStream<'a, Result<ObjectMeta>>>` - List all files identified by this [`ListingTableUrl`] for the provided `file_extension`
- `fn as_str(self: &Self) -> &str` - Returns this [`ListingTableUrl`] as a string
- `fn object_store(self: &Self) -> ObjectStoreUrl` - Return the [`ObjectStoreUrl`] for this [`ListingTableUrl`]
- `fn is_folder(self: &Self) -> bool` - Returns true if the [`ListingTableUrl`] points to the folder
- `fn get_url(self: &Self) -> &Url` - Return the `url` for [`ListingTableUrl`]
- `fn get_glob(self: &Self) -> &Option<Pattern>` - Return the `glob` for [`ListingTableUrl`]
- `fn with_glob(self: Self, glob: &str) -> Result<Self>` - Returns a copy of current [`ListingTableUrl`] with a specified `glob`
- `fn with_table_ref(self: Self, table_ref: TableReference) -> Self`
- `fn get_table_ref(self: &Self) -> &Option<TableReference>`

**Traits:** Eq

**Trait Implementations:**

- **Display**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **AsRef**
  - `fn as_ref(self: &Self) -> &str`
- **PartialEq**
  - `fn eq(self: &Self, other: &ListingTableUrl) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> ListingTableUrl`
- **AsRef**
  - `fn as_ref(self: &Self) -> &Url`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



