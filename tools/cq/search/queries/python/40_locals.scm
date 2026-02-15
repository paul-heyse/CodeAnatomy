((function_definition
   name: (identifier) @local.definition
   body: (block) @local.scope)
 (#set! cq.emit "locals")
 (#set! cq.kind "local_definition")
 (#set! cq.anchor "local.definition"))

((class_definition
   name: (identifier) @local.definition
   body: (block) @local.scope)
 (#set! cq.emit "locals")
 (#set! cq.kind "local_definition")
 (#set! cq.anchor "local.definition"))

((lambda
   body: (_) @local.scope)
 (#set! cq.emit "locals")
 (#set! cq.kind "local_scope")
 (#set! cq.anchor "local.scope"))

((assignment left: (identifier) @local.definition)
 (#set! cq.emit "locals")
 (#set! cq.kind "local_definition")
 (#set! cq.anchor "local.definition"))

((for_statement left: (identifier) @local.definition)
 (#set! cq.emit "locals")
 (#set! cq.kind "local_definition")
 (#set! cq.anchor "local.definition"))

((assignment right: (identifier) @local.reference)
 (#set! cq.emit "locals")
 (#set! cq.kind "local_reference")
 (#set! cq.anchor "local.reference"))

((call function: (identifier) @local.reference)
 (#set! cq.emit "locals")
 (#set! cq.kind "local_reference")
 (#set! cq.anchor "local.reference"))

((attribute object: (identifier) @local.reference)
 (#set! cq.emit "locals")
 (#set! cq.kind "local_reference")
 (#set! cq.anchor "local.reference"))
