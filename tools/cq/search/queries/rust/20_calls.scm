((call_expression function: (identifier) @call.target.identifier) @call
 (#match? @call.target.identifier "^[A-Za-z_][A-Za-z0-9_]*$")
 (#set! cq.emit "calls")
 (#set! cq.kind "call")
 (#set! cq.anchor "call.target")
 (#set! cq.slot.callee "call.target"))

((call_expression function: (scoped_identifier) @call.target.scoped)
 (#match? @call.target.scoped "^[A-Za-z_][A-Za-z0-9_:]*$")
 (#set! cq.emit "calls")
 (#set! cq.kind "call")
 (#set! cq.anchor "call.target")
 (#set! cq.slot.callee "call.target"))

((call_expression function: (_) @call.target.field_expression) @call
 (#match? @call.target.field_expression "^[A-Za-z_][A-Za-z0-9_\.]*$")
 (#set! cq.emit "calls")
 (#set! cq.kind "call")
 (#set! cq.anchor "call.target")
 (#set! cq.slot.callee "call.target"))

((macro_invocation macro: (identifier) @call.macro.path) @call.macro
 (#any-of? @call.macro.path "sql" "query" "regex")
 (#set! cq.emit "calls")
 (#set! cq.kind "macro_call")
 (#set! cq.anchor "call.macro.path")
 (#set! cq.slot.callee "call.macro.path"))
