**datafusion_doc**

# Module: datafusion_doc

## Contents

**Structs**

- [`DocSection`](#docsection)
- [`Documentation`](#documentation) - Documentation for use by `ScalarUDFImpl`, `AggregateUDFImpl` and `WindowUDFImpl` functions.
- [`DocumentationBuilder`](#documentationbuilder) - A builder for [`Documentation`]'s.

---

## datafusion_doc::DocSection

*Struct*

**Fields:**
- `include: bool` - True to include this doc section in the public
- `label: &'static str` - A display label for the doc section. For example: "Math Expressions"
- `description: Option<&'static str>` - An optional description for the doc section

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> DocSection`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Default**
  - `fn default() -> Self` - Returns a "default" Doc section.
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **PartialEq**
  - `fn eq(self: &Self, other: &DocSection) -> bool`



## datafusion_doc::Documentation

*Struct*

Documentation for use by `ScalarUDFImpl`, `AggregateUDFImpl` and `WindowUDFImpl` functions.

See the [`DocumentationBuilder`] to create a new [`Documentation`] struct.

The DataFusion [SQL function documentation] is automatically  generated from these structs.
The name of the udf will be pulled from the `ScalarUDFImpl::name`,
`AggregateUDFImpl::name` or `WindowUDFImpl::name`
function as appropriate.

All strings in the documentation are required to be
in [markdown format](https://www.markdownguide.org/basic-syntax/).

Currently, documentation only supports a single language
thus all text should be in English.

[SQL function documentation]: https://datafusion.apache.org/user-guide/sql/index.html

**Fields:**
- `doc_section: DocSection` - The section in the documentation where the UDF will be documented
- `description: String` - The description for the UDF
- `syntax_example: String` - A brief example of the syntax. For example "ascii(str)"
- `sql_example: Option<String>` - A sql example for the UDF, usually in the form of a sql prompt
- `arguments: Option<Vec<(String, String)>>` - Arguments for the UDF which will be displayed in array order.
- `alternative_syntax: Option<Vec<String>>` - A list of alternative syntax examples for a function
- `related_udfs: Option<Vec<String>>` - Related functions if any. Values should match the related

**Methods:**

- `fn builder<impl Into<String>, impl Into<String>>(doc_section: DocSection, description: impl Trait, syntax_example: impl Trait) -> DocumentationBuilder` - Returns a new [`DocumentationBuilder`] with no options set.
- `fn to_doc_attribute(self: &Self) -> String` - Output the `Documentation` struct in form of custom Rust documentation attributes

**Traits:** Eq

**Trait Implementations:**

- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &Documentation) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> Documentation`



## datafusion_doc::DocumentationBuilder

*Struct*

A builder for [`Documentation`]'s.

Example:

```rust

# fn main() {
    use datafusion_doc::{DocSection, Documentation};
    let doc_section = DocSection {
        include: true,
        label: "Display Label",
        description: None,
    };

    let documentation = Documentation::builder(doc_section, "Add one to an int32".to_owned(), "add_one(2)".to_owned())
          .with_argument("arg_1", "The int32 number to add one to")
          .build();
# }

**Fields:**
- `doc_section: DocSection`
- `description: String`
- `syntax_example: String`
- `sql_example: Option<String>`
- `arguments: Option<Vec<(String, String)>>`
- `alternative_syntax: Option<Vec<String>>`
- `related_udfs: Option<Vec<String>>`

**Methods:**

- `fn new_with_details<impl Into<String>, impl Into<String>>(doc_section: DocSection, description: impl Trait, syntax_example: impl Trait) -> Self` - Creates a new [`DocumentationBuilder`] with all required fields
- `fn with_doc_section(self: Self, doc_section: DocSection) -> Self`
- `fn with_description<impl Into<String>>(self: Self, description: impl Trait) -> Self`
- `fn with_syntax_example<impl Into<String>>(self: Self, syntax_example: impl Trait) -> Self`
- `fn with_sql_example<impl Into<String>>(self: Self, sql_example: impl Trait) -> Self`
- `fn with_argument<impl Into<String>, impl Into<String>>(self: Self, arg_name: impl Trait, arg_description: impl Trait) -> Self` - Adds documentation for a specific argument to the documentation.
- `fn with_standard_argument<impl Into<String>>(self: Self, arg_name: impl Trait, expression_type: Option<&str>) -> Self` - Add a standard "expression" argument to the documentation
- `fn with_alternative_syntax<impl Into<String>>(self: Self, syntax_name: impl Trait) -> Self`
- `fn with_related_udf<impl Into<String>>(self: Self, related_udf: impl Trait) -> Self`
- `fn build(self: Self) -> Documentation` - Build the documentation from provided components



