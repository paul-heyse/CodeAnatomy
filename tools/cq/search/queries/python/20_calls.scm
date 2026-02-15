((call function: (identifier) @call.target.identifier) @call
 (#set! cq.emit "calls")
 (#set! cq.kind "call")
 (#set! cq.anchor "call.target.identifier")
 (#set! cq.slot.callee "call.target.identifier"))

((call function: (attribute) @call.target.attribute) @call
 (#set! cq.emit "calls")
 (#set! cq.kind "call")
 (#set! cq.anchor "call.target.attribute")
 (#set! cq.slot.callee "call.target.attribute"))
