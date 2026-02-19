use datafusion_expr::sqlparser::ast::{SetExpr, Statement, TableFactor};
use datafusion_expr::sqlparser::dialect::GenericDialect;
use datafusion_expr::sqlparser::parser::Parser;

use datafusion_ext::relation_planner::supports_codeanatomy_relation;

fn parse_relation(sql: &str) -> TableFactor {
    let dialect = GenericDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql).expect("sql parses");
    let statement = statements.remove(0);
    let Statement::Query(query) = statement else {
        panic!("expected query statement");
    };
    let SetExpr::Select(select) = *query.body else {
        panic!("expected select query");
    };
    select
        .from
        .first()
        .expect("from clause exists")
        .relation
        .clone()
}

#[test]
fn supports_codeanatomy_relation_for_prefixed_tables() {
    let relation = parse_relation("SELECT * FROM codeanatomy.events");
    assert!(supports_codeanatomy_relation(&relation));
}

#[test]
fn does_not_support_non_codeanatomy_relations() {
    let relation = parse_relation("SELECT * FROM plain_table");
    assert!(!supports_codeanatomy_relation(&relation));
}
