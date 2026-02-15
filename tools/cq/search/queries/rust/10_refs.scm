((identifier) @ref.identifier
 (#set! cq.emit "references")
 (#set! cq.kind "identifier")
 (#set! cq.anchor "ref.identifier"))

((scoped_identifier name: (identifier) @ref.scoped.name)
 (#set! cq.emit "references")
 (#set! cq.kind "scoped_identifier")
 (#set! cq.anchor "ref.scoped.name"))

((use_declaration argument: (_) @ref.use.path) @ref.use
 (#set! cq.emit "references")
 (#set! cq.kind "use_path")
 (#set! cq.anchor "ref.use.path"))

((macro_invocation macro: (_) @ref.macro.path) @ref.macro
 (#set! cq.emit "references")
 (#set! cq.kind "macro_path")
 (#set! cq.anchor "ref.macro.path"))
