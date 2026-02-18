//! Tree-sitter extraction bridge for Python source files.

use std::collections::BTreeMap;
use std::time::Instant;

use datafusion_common::DataFusionError;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use tree_sitter::{Node, Parser, TreeCursor};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct TreeSitterExtractOptions {
    pub include_nodes: bool,
    pub include_errors: bool,
    pub include_missing: bool,
    pub include_edges: bool,
    pub include_captures: bool,
    pub include_defs: bool,
    pub include_calls: bool,
    pub include_imports: bool,
    pub include_docstrings: bool,
    pub include_stats: bool,
    pub max_text_bytes: usize,
    pub max_docstring_bytes: usize,
}

impl Default for TreeSitterExtractOptions {
    fn default() -> Self {
        Self {
            include_nodes: true,
            include_errors: true,
            include_missing: true,
            include_edges: true,
            include_captures: true,
            include_defs: true,
            include_calls: true,
            include_imports: true,
            include_docstrings: true,
            include_stats: true,
            max_text_bytes: 256,
            max_docstring_bytes: 2048,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct TreeSitterExtractRequest {
    pub repo: Option<String>,
    pub file_id: Option<String>,
    pub file_sha256: Option<String>,
    pub options: TreeSitterExtractOptions,
}

#[derive(Debug, Clone, Serialize)]
struct NodeFlags {
    is_named: bool,
    has_error: bool,
    is_error: bool,
    is_missing: bool,
    is_extra: bool,
    has_changes: bool,
}

#[derive(Debug, Clone, Serialize)]
struct TreeSitterNodeRow {
    node_id: String,
    node_uid: i64,
    parent_id: Option<String>,
    kind: String,
    kind_id: i32,
    grammar_id: i32,
    grammar_name: String,
    span: JsonValue,
    flags: NodeFlags,
    attrs: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize)]
struct TreeSitterEdgeRow {
    parent_id: String,
    child_id: String,
    field_name: Option<String>,
    child_index: Option<i32>,
    attrs: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize)]
struct TreeSitterErrorRow {
    error_id: String,
    node_id: String,
    span: JsonValue,
    attrs: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize)]
struct TreeSitterMissingRow {
    missing_id: String,
    node_id: String,
    span: JsonValue,
    attrs: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize)]
struct TreeSitterCaptureRow {
    capture_id: String,
    query_name: String,
    capture_name: String,
    pattern_index: i32,
    node_id: String,
    node_kind: String,
    span: JsonValue,
    attrs: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize)]
struct TreeSitterDefRow {
    node_id: String,
    parent_id: Option<String>,
    kind: String,
    name: Option<String>,
    span: JsonValue,
    attrs: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize)]
struct TreeSitterCallRow {
    node_id: String,
    parent_id: Option<String>,
    callee_kind: String,
    callee_text: Option<String>,
    callee_node_id: String,
    span: JsonValue,
    attrs: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize)]
struct TreeSitterImportRow {
    node_id: String,
    parent_id: Option<String>,
    kind: String,
    module: Option<String>,
    name: Option<String>,
    asname: Option<String>,
    alias_index: i32,
    level: Option<i32>,
    span: JsonValue,
    attrs: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize)]
struct TreeSitterDocstringRow {
    owner_node_id: String,
    owner_kind: String,
    owner_name: Option<String>,
    doc_node_id: String,
    docstring: Option<String>,
    source: Option<String>,
    span: JsonValue,
    attrs: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize)]
struct TreeSitterStatsRow {
    node_count: i32,
    named_count: i32,
    error_count: i32,
    missing_count: i32,
    parse_ms: i64,
    parse_timed_out: bool,
    incremental_used: bool,
    query_match_count: i32,
    query_capture_count: i32,
    match_limit_exceeded: bool,
}

fn node_id(file_path: &str, start: i64, end: i64, kind: &str) -> String {
    format!("{file_path}:{kind}:{start}:{end}")
}

fn span_value(start: i64, end: i64) -> JsonValue {
    json!({
        "start": JsonValue::Null,
        "end": JsonValue::Null,
        "end_exclusive": true,
        "col_unit": "byte",
        "byte_span": {
            "byte_start": start,
            "byte_len": std::cmp::max(0, end - start),
        },
    })
}

fn text_from_range(source: &str, start: usize, end: usize, max_bytes: usize) -> (Option<String>, bool) {
    if start >= end || start >= source.len() {
        return (None, false);
    }
    let clamped_end = std::cmp::min(end, source.len());
    let value = source.get(start..clamped_end).map(|s| s.to_string());
    let Some(mut text) = value else {
        return (None, false);
    };
    if max_bytes == 0 {
        return (None, !text.is_empty());
    }
    let bytes = text.as_bytes();
    if bytes.len() <= max_bytes {
        return (Some(text), false);
    }
    let truncated = String::from_utf8_lossy(&bytes[..max_bytes]).to_string();
    text = truncated;
    (Some(text), true)
}

fn text_for_node(node: Node<'_>, source: &str, max_bytes: usize, allow_non_leaf: bool) -> (Option<String>, bool) {
    if !allow_non_leaf && node.child_count() > 0 {
        return (None, false);
    }
    text_from_range(source, node.start_byte(), node.end_byte(), max_bytes)
}

fn split_import_alias(value: Option<String>) -> (Option<String>, Option<String>) {
    let Some(text) = value else {
        return (None, None);
    };
    let trimmed = text.trim().to_string();
    if trimmed.is_empty() {
        return (None, None);
    }
    if let Some((name, alias)) = trimmed.split_once(" as ") {
        let normalized_name = name.trim().to_string();
        let normalized_alias = alias.trim().to_string();
        return (
            if normalized_name.is_empty() {
                None
            } else {
                Some(normalized_name)
            },
            if normalized_alias.is_empty() {
                None
            } else {
                Some(normalized_alias)
            },
        );
    }
    (Some(trimmed), None)
}

fn decode_docstring_literal(value: &str) -> Option<String> {
    let trimmed = value.trim();
    let triple_double = "\"\"\"";
    let triple_single = "'''";
    if trimmed.starts_with(triple_double)
        && trimmed.ends_with(triple_double)
        && trimmed.len() >= triple_double.len() * 2
    {
        let inner = &trimmed[triple_double.len()..trimmed.len() - triple_double.len()];
        return Some(inner.to_string());
    }
    if trimmed.starts_with(triple_single)
        && trimmed.ends_with(triple_single)
        && trimmed.len() >= triple_single.len() * 2
    {
        let inner = &trimmed[triple_single.len()..trimmed.len() - triple_single.len()];
        return Some(inner.to_string());
    }
    if (trimmed.starts_with('\"') && trimmed.ends_with('\"'))
        || (trimmed.starts_with('\'') && trimmed.ends_with('\''))
    {
        if trimmed.len() >= 2 {
            return Some(trimmed[1..trimmed.len() - 1].to_string());
        }
    }
    None
}

fn first_docstring_node(owner: Node<'_>) -> Option<Node<'_>> {
    let owner_kind = owner.kind();
    let first_stmt = if owner_kind == "module" {
        let mut cursor = owner.walk();
        let mut found: Option<Node<'_>> = None;
        if cursor.goto_first_child() {
            loop {
                let candidate = cursor.node();
                if candidate.is_named() {
                    found = Some(candidate);
                    break;
                }
                if !cursor.goto_next_sibling() {
                    break;
                }
            }
        }
        found
    } else {
        let body = owner.child_by_field_name("body")?;
        let mut cursor = body.walk();
        let mut found: Option<Node<'_>> = None;
        if cursor.goto_first_child() {
            loop {
                let candidate = cursor.node();
                if candidate.is_named() {
                    found = Some(candidate);
                    break;
                }
                if !cursor.goto_next_sibling() {
                    break;
                }
            }
        }
        found
    }?;
    if first_stmt.kind() != "expression_statement" {
        return None;
    }
    let mut cursor = first_stmt.walk();
    if !cursor.goto_first_child() {
        return None;
    }
    loop {
        let child = cursor.node();
        if child.is_named() && child.kind() == "string" {
            return Some(child);
        }
        if !cursor.goto_next_sibling() {
            break;
        }
    }
    None
}

fn child_entries(parent: Node<'_>) -> Vec<(Node<'_>, Option<String>, i32)> {
    let mut entries: Vec<(Node<'_>, Option<String>, i32)> = Vec::new();
    let mut cursor: TreeCursor<'_> = parent.walk();
    if !cursor.goto_first_child() {
        return entries;
    }
    let mut index: i32 = 0;
    loop {
        entries.push((cursor.node(), cursor.field_name().map(str::to_string), index));
        index += 1;
        if !cursor.goto_next_sibling() {
            break;
        }
    }
    entries
}

/// Extract Python tree-sitter rows as a mapping payload compatible with
/// `tree_sitter_files_v1` nested schema fields.
pub fn extract_tree_sitter_row(
    source: &str,
    file_path: &str,
    request: &TreeSitterExtractRequest,
) -> Result<JsonValue, DataFusionError> {
    let language: tree_sitter::Language = tree_sitter_python::LANGUAGE.into();
    let mut parser = Parser::new();
    parser
        .set_language(&language)
        .map_err(|err| DataFusionError::Execution(format!("tree-sitter language init failed: {err}")))?;
    let started = Instant::now();
    let tree = parser
        .parse(source, None)
        .ok_or_else(|| DataFusionError::Execution("tree-sitter parse failed".to_string()))?;
    let parse_ms = started.elapsed().as_millis() as i64;

    let options = &request.options;
    let file_id = request.file_id.clone().unwrap_or_else(|| file_path.to_string());
    let mut nodes: Vec<TreeSitterNodeRow> = Vec::new();
    let mut edges: Vec<TreeSitterEdgeRow> = Vec::new();
    let mut errors: Vec<TreeSitterErrorRow> = Vec::new();
    let mut missing: Vec<TreeSitterMissingRow> = Vec::new();
    let mut captures: Vec<TreeSitterCaptureRow> = Vec::new();
    let mut defs: Vec<TreeSitterDefRow> = Vec::new();
    let mut calls: Vec<TreeSitterCallRow> = Vec::new();
    let mut imports: Vec<TreeSitterImportRow> = Vec::new();
    let mut docstrings: Vec<TreeSitterDocstringRow> = Vec::new();
    let mut import_alias_index: BTreeMap<String, i32> = BTreeMap::new();

    let mut node_ids: Vec<String> = Vec::new();
    let mut named_count: i32 = 0;
    let mut error_count: i32 = 0;
    let mut missing_count: i32 = 0;
    let mut query_match_count: i32 = 0;

    let mut stack: Vec<(Node<'_>, Option<usize>, Option<String>, Option<i32>)> =
        vec![(tree.root_node(), None, None, None)];

    while let Some((node, parent_index, field_name, child_index)) = stack.pop() {
        let start = node.start_byte() as i64;
        let end = node.end_byte() as i64;
        let kind = node.kind().to_string();
        let current_index = node_ids.len();
        let current_node_id = node_id(file_path, start, end, kind.as_str());
        let parent_id = parent_index.and_then(|idx| node_ids.get(idx).cloned());

        if node.is_named() {
            named_count += 1;
        }
        if node.is_error() {
            error_count += 1;
        }
        if node.is_missing() {
            missing_count += 1;
        }

        if options.include_nodes {
            let (text, truncated) = text_for_node(node, source, options.max_text_bytes, false);
            let mut attrs: Vec<(String, String)> = Vec::new();
            if let Some(text) = text {
                attrs.push(("text".to_string(), text));
            }
            if truncated {
                attrs.push(("text_truncated".to_string(), "true".to_string()));
            }
            nodes.push(TreeSitterNodeRow {
                node_id: current_node_id.clone(),
                node_uid: current_index as i64,
                parent_id: parent_id.clone(),
                kind: kind.clone(),
                kind_id: 0,
                grammar_id: 0,
                grammar_name: "python".to_string(),
                span: span_value(start, end),
                flags: NodeFlags {
                    is_named: node.is_named(),
                    has_error: node.has_error(),
                    is_error: node.is_error(),
                    is_missing: node.is_missing(),
                    is_extra: false,
                    has_changes: false,
                },
                attrs,
            });
        }

        if options.include_edges {
            if let Some(parent_id) = parent_id.clone() {
                edges.push(TreeSitterEdgeRow {
                    parent_id,
                    child_id: current_node_id.clone(),
                    field_name,
                    child_index,
                    attrs: Vec::new(),
                });
            }
        }

        if options.include_errors && node.is_error() {
            errors.push(TreeSitterErrorRow {
                error_id: node_id(file_path, start, end, "ts_error"),
                node_id: current_node_id.clone(),
                span: span_value(start, end),
                attrs: Vec::new(),
            });
        }

        if options.include_missing && node.is_missing() {
            missing.push(TreeSitterMissingRow {
                missing_id: node_id(file_path, start, end, "ts_missing"),
                node_id: current_node_id.clone(),
                span: span_value(start, end),
                attrs: Vec::new(),
            });
        }

        if options.include_defs && (node.kind() == "function_definition" || node.kind() == "class_definition")
        {
            let name = node
                .child_by_field_name("name")
                .and_then(|name_node| text_for_node(name_node, source, options.max_text_bytes, true).0);
            defs.push(TreeSitterDefRow {
                node_id: current_node_id.clone(),
                parent_id: parent_id.clone(),
                kind: kind.clone(),
                name: name.clone(),
                span: span_value(start, end),
                attrs: Vec::new(),
            });
            query_match_count += 1;
            if options.include_captures {
                captures.push(TreeSitterCaptureRow {
                    capture_id: node_id(file_path, start, end, "ts_capture:defs:def.node"),
                    query_name: "defs".to_string(),
                    capture_name: "def.node".to_string(),
                    pattern_index: 0,
                    node_id: current_node_id.clone(),
                    node_kind: kind.clone(),
                    span: span_value(start, end),
                    attrs: Vec::new(),
                });
            }
        }

        if options.include_calls && node.kind() == "call" {
            if let Some(callee_node) = node.child_by_field_name("function") {
                let (callee_text, _) =
                    text_for_node(callee_node, source, options.max_text_bytes, true);
                let callee_start = callee_node.start_byte() as i64;
                let callee_end = callee_node.end_byte() as i64;
                calls.push(TreeSitterCallRow {
                    node_id: current_node_id.clone(),
                    parent_id: parent_id.clone(),
                    callee_kind: callee_node.kind().to_string(),
                    callee_text,
                    callee_node_id: node_id(file_path, callee_start, callee_end, callee_node.kind()),
                    span: span_value(start, end),
                    attrs: Vec::new(),
                });
                query_match_count += 1;
                if options.include_captures {
                    captures.push(TreeSitterCaptureRow {
                        capture_id: node_id(file_path, start, end, "ts_capture:calls:call.node"),
                        query_name: "calls".to_string(),
                        capture_name: "call.node".to_string(),
                        pattern_index: 0,
                        node_id: current_node_id.clone(),
                        node_kind: kind.clone(),
                        span: span_value(start, end),
                        attrs: Vec::new(),
                    });
                }
            }
        }

        if options.include_imports
            && (node.kind() == "import_statement" || node.kind() == "import_from_statement")
        {
            let import_kind = if node.kind() == "import_from_statement" {
                "ImportFrom".to_string()
            } else {
                "Import".to_string()
            };
            let module_text = node
                .child_by_field_name("module_name")
                .and_then(|module_node| text_for_node(module_node, source, options.max_text_bytes, true).0);
            let (name, asname) = split_import_alias(
                node.child_by_field_name("name")
                    .and_then(|name_node| text_for_node(name_node, source, options.max_text_bytes, true).0),
            );
            let level = module_text.as_ref().map(|module| {
                let dot_count = module.chars().take_while(|ch| *ch == '.').count();
                dot_count as i32
            });
            let normalized_module = module_text.as_ref().and_then(|module| {
                let trimmed = module.trim_start_matches('.').trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_string())
                }
            });
            let alias_index = import_alias_index
                .entry(current_node_id.clone())
                .and_modify(|value| *value += 1)
                .or_insert(0);
            let current_alias_index = *alias_index;
            imports.push(TreeSitterImportRow {
                node_id: current_node_id.clone(),
                parent_id: parent_id.clone(),
                kind: import_kind,
                module: normalized_module,
                name,
                asname,
                alias_index: current_alias_index,
                level,
                span: span_value(start, end),
                attrs: Vec::new(),
            });
            query_match_count += 1;
            if options.include_captures {
                captures.push(TreeSitterCaptureRow {
                    capture_id: node_id(file_path, start, end, "ts_capture:imports:import.node"),
                    query_name: "imports".to_string(),
                    capture_name: "import.node".to_string(),
                    pattern_index: 0,
                    node_id: current_node_id.clone(),
                    node_kind: kind.clone(),
                    span: span_value(start, end),
                    attrs: Vec::new(),
                });
            }
        }

        if options.include_docstrings
            && (node.kind() == "module"
                || node.kind() == "class_definition"
                || node.kind() == "function_definition")
        {
            if let Some(doc_node) = first_docstring_node(node) {
                let (source_value, truncated) =
                    text_for_node(doc_node, source, options.max_docstring_bytes, true);
                let decoded = source_value
                    .as_ref()
                    .and_then(|value| decode_docstring_literal(value.as_str()));
                let owner_name = node
                    .child_by_field_name("name")
                    .and_then(|name_node| text_for_node(name_node, source, options.max_text_bytes, true).0);
                let mut attrs: Vec<(String, String)> = Vec::new();
                if truncated {
                    attrs.push(("text_truncated".to_string(), "true".to_string()));
                }
                let doc_start = doc_node.start_byte() as i64;
                let doc_end = doc_node.end_byte() as i64;
                docstrings.push(TreeSitterDocstringRow {
                    owner_node_id: current_node_id.clone(),
                    owner_kind: kind.clone(),
                    owner_name,
                    doc_node_id: node_id(file_path, doc_start, doc_end, doc_node.kind()),
                    docstring: decoded,
                    source: source_value,
                    span: span_value(doc_start, doc_end),
                    attrs,
                });
                query_match_count += 1;
                if options.include_captures {
                    captures.push(TreeSitterCaptureRow {
                        capture_id: node_id(file_path, doc_start, doc_end, "ts_capture:docstrings:doc.string"),
                        query_name: "docstrings".to_string(),
                        capture_name: "doc.string".to_string(),
                        pattern_index: 0,
                        node_id: node_id(file_path, doc_start, doc_end, doc_node.kind()),
                        node_kind: doc_node.kind().to_string(),
                        span: span_value(doc_start, doc_end),
                        attrs: Vec::new(),
                    });
                }
            }
        }

        node_ids.push(current_node_id);
        let mut children = child_entries(node);
        children.reverse();
        for (child, child_field, child_offset) in children {
            stack.push((child, Some(current_index), child_field, Some(child_offset)));
        }
    }

    let stats = if options.include_stats {
        Some(TreeSitterStatsRow {
            node_count: node_ids.len() as i32,
            named_count,
            error_count,
            missing_count,
            parse_ms,
            parse_timed_out: false,
            incremental_used: false,
            query_match_count,
            query_capture_count: captures.len() as i32,
            match_limit_exceeded: false,
        })
    } else {
        None
    };

    let attrs = vec![
        ("language_name".to_string(), "python".to_string()),
        ("language_abi_version".to_string(), "unknown".to_string()),
        ("parse_ms".to_string(), parse_ms.to_string()),
        ("node_count".to_string(), (node_ids.len() as i32).to_string()),
        ("named_count".to_string(), named_count.to_string()),
        ("error_count".to_string(), error_count.to_string()),
        ("missing_count".to_string(), missing_count.to_string()),
    ];

    Ok(json!({
        "repo": request.repo.clone().unwrap_or_default(),
        "path": file_path,
        "file_id": file_id,
        "file_sha256": request.file_sha256,
        "nodes": if options.include_nodes { serde_json::to_value(nodes).unwrap_or_else(|_| json!([])) } else { json!([]) },
        "edges": if options.include_edges { serde_json::to_value(edges).unwrap_or_else(|_| json!([])) } else { json!([]) },
        "errors": if options.include_errors { serde_json::to_value(errors).unwrap_or_else(|_| json!([])) } else { json!([]) },
        "missing": if options.include_missing { serde_json::to_value(missing).unwrap_or_else(|_| json!([])) } else { json!([]) },
        "captures": if options.include_captures { serde_json::to_value(captures).unwrap_or_else(|_| json!([])) } else { json!([]) },
        "defs": if options.include_defs { serde_json::to_value(defs).unwrap_or_else(|_| json!([])) } else { json!([]) },
        "calls": if options.include_calls { serde_json::to_value(calls).unwrap_or_else(|_| json!([])) } else { json!([]) },
        "imports": if options.include_imports { serde_json::to_value(imports).unwrap_or_else(|_| json!([])) } else { json!([]) },
        "docstrings": if options.include_docstrings { serde_json::to_value(docstrings).unwrap_or_else(|_| json!([])) } else { json!([]) },
        "stats": stats,
        "attrs": attrs,
    }))
}
