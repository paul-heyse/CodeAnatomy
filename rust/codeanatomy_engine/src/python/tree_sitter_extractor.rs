//! Tree-sitter extraction bridge for Python source files.

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::DataFusionError;
use tree_sitter::{Node, Parser};

fn walk_nodes<'a>(root: Node<'a>) -> Vec<(String, i64, i64, Option<i64>)> {
    let mut rows: Vec<(String, i64, i64, Option<i64>)> = Vec::new();
    let mut stack: Vec<(Node<'a>, Option<i64>)> = vec![(root, None)];
    while let Some((node, parent_id)) = stack.pop() {
        let current_id = rows.len() as i64;
        rows.push((
            node.kind().to_string(),
            node.start_byte() as i64,
            node.end_byte() as i64,
            parent_id,
        ));
        let mut cursor = node.walk();
        let mut children: Vec<Node<'a>> = node.children(&mut cursor).collect();
        children.reverse();
        for child in children {
            stack.push((child, Some(current_id)));
        }
    }
    rows
}

/// Extract Python tree-sitter node rows as an Arrow RecordBatch.
pub fn extract_tree_sitter_batch(
    source: &str,
    file_path: &str,
) -> Result<RecordBatch, DataFusionError> {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_python::LANGUAGE.into())
        .map_err(|err| DataFusionError::Execution(format!("tree-sitter language init failed: {err}")))?;
    let tree = parser
        .parse(source, None)
        .ok_or_else(|| DataFusionError::Execution("tree-sitter parse failed".to_string()))?;

    let rows = walk_nodes(tree.root_node());
    let node_type = StringArray::from_iter_values(rows.iter().map(|row| row.0.as_str()));
    let bstart = Int64Array::from_iter_values(rows.iter().map(|row| row.1));
    let bend = Int64Array::from_iter_values(rows.iter().map(|row| row.2));
    let parent_id = Int64Array::from(rows.iter().map(|row| row.3).collect::<Vec<Option<i64>>>());
    let file = StringArray::from_iter_values((0..rows.len()).map(|_| file_path));

    let schema = Arc::new(Schema::new(vec![
        Field::new("node_type", DataType::Utf8, false),
        Field::new("bstart", DataType::Int64, false),
        Field::new("bend", DataType::Int64, false),
        Field::new("parent_id", DataType::Int64, true),
        Field::new("file", DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(node_type),
            Arc::new(bstart),
            Arc::new(bend),
            Arc::new(parent_id),
            Arc::new(file),
        ],
    )
    .map_err(|err| DataFusionError::Execution(format!("tree-sitter batch build failed: {err}")))
}
