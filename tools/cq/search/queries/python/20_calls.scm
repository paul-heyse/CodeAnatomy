((call function: (identifier) @call.target.identifier) @call
 (#match? @call.target.identifier "^[A-Za-z_][A-Za-z0-9_]*$")
 (#set! cq.emit "calls")
 (#set! cq.kind "call")
 (#set! cq.anchor "call.target.identifier")
 (#set! cq.slot.callee "call.target.identifier"))

((call function: (attribute) @call.target.attribute) @call
 (#match? @call.target.attribute "^[A-Za-z_][A-Za-z0-9_\.:]*$")
 (#set! cq.emit "calls")
 (#set! cq.kind "call")
 (#set! cq.anchor "call.target.attribute")
 (#set! cq.slot.callee "call.target.attribute"))
