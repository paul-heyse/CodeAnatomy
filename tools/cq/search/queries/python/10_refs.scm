((identifier) @ref.identifier
 (#set! cq.emit "references")
 (#set! cq.kind "identifier")
 (#set! cq.anchor "ref.identifier"))

((attribute attribute: (identifier) @ref.attribute)
 (#set! cq.emit "references")
 (#set! cq.kind "attribute")
 (#set! cq.anchor "ref.attribute"))
