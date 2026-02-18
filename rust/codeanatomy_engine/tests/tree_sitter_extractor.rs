use codeanatomy_engine::python::tree_sitter_extractor::{
    extract_tree_sitter_row, TreeSitterExtractOptions, TreeSitterExtractRequest,
};
use serde_json::Value;

fn array_len(value: &Value, key: &str) -> usize {
    value
        .get(key)
        .and_then(Value::as_array)
        .map_or(0, Vec::len)
}

#[test]
fn tree_sitter_extractor_emits_full_nested_payload() {
    let source = r#"
import os
from pathlib import Path as P

"""module doc"""

class C:
    """class doc"""
    def run(self, x):
        return foo(x)
"#;
    let request = TreeSitterExtractRequest {
        repo: Some("repo-a".to_string()),
        file_id: Some("file-1".to_string()),
        file_sha256: Some("sha".to_string()),
        options: TreeSitterExtractOptions::default(),
    };

    let row = extract_tree_sitter_row(source, "a.py", &request).expect("extract row");
    assert_eq!(row.get("repo").and_then(Value::as_str), Some("repo-a"));
    assert_eq!(row.get("path").and_then(Value::as_str), Some("a.py"));
    assert_eq!(row.get("file_id").and_then(Value::as_str), Some("file-1"));
    assert!(array_len(&row, "nodes") > 0);
    assert!(array_len(&row, "edges") > 0);
    assert!(array_len(&row, "defs") > 0);
    assert!(array_len(&row, "calls") > 0);
    assert!(array_len(&row, "imports") > 0);
    assert!(array_len(&row, "docstrings") > 0);
    assert!(array_len(&row, "captures") > 0);
    assert!(row.get("stats").is_some());
}

#[test]
fn tree_sitter_extractor_honors_include_toggles() {
    let source = "def f():\n    return 1\n";
    let options = TreeSitterExtractOptions {
        include_nodes: false,
        include_errors: false,
        include_missing: false,
        include_edges: false,
        include_captures: false,
        include_defs: true,
        include_calls: false,
        include_imports: false,
        include_docstrings: false,
        include_stats: false,
        ..TreeSitterExtractOptions::default()
    };
    let request = TreeSitterExtractRequest {
        repo: Some("repo-b".to_string()),
        file_id: Some("file-2".to_string()),
        file_sha256: None,
        options,
    };

    let row = extract_tree_sitter_row(source, "b.py", &request).expect("extract row");
    assert_eq!(array_len(&row, "nodes"), 0);
    assert_eq!(array_len(&row, "edges"), 0);
    assert_eq!(array_len(&row, "errors"), 0);
    assert_eq!(array_len(&row, "missing"), 0);
    assert_eq!(array_len(&row, "captures"), 0);
    assert_eq!(array_len(&row, "calls"), 0);
    assert_eq!(array_len(&row, "imports"), 0);
    assert_eq!(array_len(&row, "docstrings"), 0);
    assert!(array_len(&row, "defs") > 0);
    assert!(row.get("stats").is_some_and(Value::is_null));
}
