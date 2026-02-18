**datafusion_sql > unparser > extension_unparser**

# Module: unparser::extension_unparser

## Contents

**Enums**

- [`UnparseToStatementResult`](#unparsetostatementresult) - The result of unparsing a custom logical node to a statement.
- [`UnparseWithinStatementResult`](#unparsewithinstatementresult) - The result of unparsing a custom logical node within a statement.

**Traits**

- [`UserDefinedLogicalNodeUnparser`](#userdefinedlogicalnodeunparser) - This trait allows users to define custom unparser logic for their custom logical nodes.

---

## datafusion_sql::unparser::extension_unparser::UnparseToStatementResult

*Enum*

The result of unparsing a custom logical node to a statement.

**Variants:**
- `Modified(sqlparser::ast::Statement)` - If the custom logical node was successfully unparsed to a statement.
- `Unmodified` - If the custom logical node wasn't unparsed.



## datafusion_sql::unparser::extension_unparser::UnparseWithinStatementResult

*Enum*

The result of unparsing a custom logical node within a statement.

**Variants:**
- `Modified` - If the custom logical node was successfully unparsed within a statement.
- `Unmodified` - If the custom logical node wasn't unparsed.



## datafusion_sql::unparser::extension_unparser::UserDefinedLogicalNodeUnparser

*Trait*

This trait allows users to define custom unparser logic for their custom logical nodes.

**Methods:**

- `unparse`: Unparse the custom logical node to SQL within a statement.
- `unparse_to_statement`: Unparse the custom logical node to a statement.



