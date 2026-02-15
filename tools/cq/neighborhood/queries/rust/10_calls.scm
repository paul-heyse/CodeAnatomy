((call_expression function: (_) @call.callee) @call.site
 (#set! cq.emit "calls")
 (#set! cq.kind "call")
 (#set! cq.anchor "call.callee"))

((macro_invocation macro: (_) @call.callee) @call.site
 (#set! cq.emit "calls")
 (#set! cq.kind "macro_call")
 (#set! cq.anchor "call.callee"))

