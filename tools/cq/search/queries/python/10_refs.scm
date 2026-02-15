((identifier) @ref.identifier
 (#match? @ref.identifier "^[A-Za-z_][A-Za-z0-9_]*$")
 (#set! cq.emit "references")
 (#set! cq.kind "identifier")
 (#set! cq.anchor "ref.identifier"))

((attribute attribute: (identifier) @ref.attribute)
 (#match? @ref.attribute "^[A-Za-z_][A-Za-z0-9_]*$")
 (#set! cq.emit "references")
 (#set! cq.kind "attribute")
 (#set! cq.anchor "ref.attribute"))
