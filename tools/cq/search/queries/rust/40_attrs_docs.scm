((attribute_item) @attr.item
 (#set! cq.emit "attributes")
 (#set! cq.kind "attribute_item")
 (#set! cq.anchor "attr.item"))

((line_comment) @doc.line
 (#set! cq.emit "docs")
 (#set! cq.kind "line_comment")
 (#set! cq.anchor "doc.line"))

((block_comment) @doc.block
 (#set! cq.emit "docs")
 (#set! cq.kind "block_comment")
 (#set! cq.anchor "doc.block"))
