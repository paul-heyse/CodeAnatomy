((assignment left: (identifier) @local.definition)
 (#set! cq.emit "locals")
 (#set! cq.kind "local_definition")
 (#set! cq.anchor "local.definition"))

((for_statement left: (identifier) @local.definition)
 (#set! cq.emit "locals")
 (#set! cq.kind "local_definition")
 (#set! cq.anchor "local.definition"))

((identifier) @local.reference
 (#set! cq.emit "locals")
 (#set! cq.kind "local_reference")
 (#set! cq.anchor "local.reference"))
