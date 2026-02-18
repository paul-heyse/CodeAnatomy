**datafusion_sql > resolve**

# Module: resolve

## Contents

**Functions**

- [`resolve_table_references`](#resolve_table_references) - Collects all tables and views referenced in the SQL statement. CTEs are collected separately.

---

## datafusion_sql::resolve::resolve_table_references

*Function*

Collects all tables and views referenced in the SQL statement. CTEs are collected separately.
This can be used to determine which tables need to be in the catalog for a query to be planned.

# Returns

A `(table_refs, ctes)` tuple, the first element contains table and view references and the second
element contains any CTE aliases that were defined and possibly referenced.

## Example

```
# use datafusion_sql::parser::DFParser;
# use datafusion_sql::resolve::resolve_table_references;
let query = "SELECT a FROM foo where x IN (SELECT y FROM bar)";
let statement = DFParser::parse_sql(query).unwrap().pop_back().unwrap();
let (table_refs, ctes) = resolve_table_references(&statement, true).unwrap();
assert_eq!(table_refs.len(), 2);
assert_eq!(table_refs[0].to_string(), "bar");
assert_eq!(table_refs[1].to_string(), "foo");
assert_eq!(ctes.len(), 0);
```

## Example with CTEs  
  
```  
# use datafusion_sql::parser::DFParser;
# use datafusion_sql::resolve::resolve_table_references;
let query = "with my_cte as (values (1), (2)) SELECT * from my_cte;";
let statement = DFParser::parse_sql(query).unwrap().pop_back().unwrap();
let (table_refs, ctes) = resolve_table_references(&statement, true).unwrap();
assert_eq!(table_refs.len(), 0);
assert_eq!(ctes.len(), 1);
assert_eq!(ctes[0].to_string(), "my_cte");
```

```rust
fn resolve_table_references(statement: &crate::parser::Statement, enable_ident_normalization: bool) -> datafusion_common::Result<(Vec<crate::TableReference>, Vec<crate::TableReference>)>
```



