((call_expression function: (_) @call.callee) @call.site
 (#match? @call.callee "^[A-Za-z_][A-Za-z0-9_:\\.]*$")
 (#set! cq.emit "calls")
 (#set! cq.kind "call")
 (#set! cq.anchor "call.callee"))

((macro_invocation macro: (_) @call.callee) @call.site
 (#match? @call.callee "^[A-Za-z_][A-Za-z0-9_:\\.]*$")
 (#set! cq.emit "calls")
 (#set! cq.kind "macro_call")
 (#set! cq.anchor "call.callee"))
