((let_declaration
   pattern: (identifier) @local.definition) @local.binding
 (#set! cq.emit "locals")
 (#set! cq.kind "local_definition")
 (#set! cq.anchor "local.definition"))

((for_expression
   pattern: (identifier) @local.definition) @local.binding
 (#set! cq.emit "locals")
 (#set! cq.kind "local_definition")
 (#set! cq.anchor "local.definition"))

((closure_expression
   parameters: (closure_parameters
                 (closure_parameter
                   pattern: (identifier) @local.definition))) @local.binding
 (#set! cq.emit "locals")
 (#set! cq.kind "local_definition")
 (#set! cq.anchor "local.definition"))

((identifier) @local.reference
 (#match? @local.reference "^[a-z_][A-Za-z0-9_]*$")
 (#set! cq.emit "locals")
 (#set! cq.kind "local_reference")
 (#set! cq.anchor "local.reference"))
