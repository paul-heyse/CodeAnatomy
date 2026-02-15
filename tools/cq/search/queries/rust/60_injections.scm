((macro_invocation
  macro: (identifier) @injection.macro.name
  (token_tree) @injection.content @injection.combined)
 (#any-of? @injection.macro.name "sql" "query" "regex")
 (#set! cq.emit "injections")
 (#set! cq.kind "macro_invocation")
 (#set! cq.anchor "injection.content")
 (#set! injection.language "sql")
 (#set! injection.include-children))

((macro_rule
  (token_tree) @injection.content)
 (#set! cq.emit "injections")
 (#set! cq.kind "macro_rule")
 (#set! cq.anchor "injection.content")
 (#set! injection.language "rust")
 (#set! injection.include-children))
