; Additional high-selectivity predicate filters.

((call_expression function: (identifier) @call.target.identifier) @call
 (#match? @call.target.identifier "^[A-Za-z_][A-Za-z0-9_]*$")
 (#set! cq.emit "calls")
 (#set! cq.kind "call_filtered")
 (#set! cq.anchor "call.target.identifier"))

((macro_invocation macro: (identifier) @macro.name) @macro.call
 (#any-of? @macro.name "sql" "query" "regex")
 (#set! cq.emit "injections")
 (#set! cq.kind "macro_injection_candidate")
 (#set! cq.anchor "macro.name"))
