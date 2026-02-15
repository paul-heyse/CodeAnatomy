((call_expression function: (_) @call.target) @call
 (#set! cq.emit "calls")
 (#set! cq.kind "call")
 (#set! cq.anchor "call.target")
 (#set! cq.slot.callee "call.target"))

((macro_invocation macro: (_) @call.macro.path) @call.macro
 (#set! cq.emit "calls")
 (#set! cq.kind "macro_call")
 (#set! cq.anchor "call.macro.path")
 (#set! cq.slot.callee "call.macro.path"))
