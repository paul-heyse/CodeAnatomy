((function_definition body: (block) @structure.body) @structure.function
 (#set! cq.emit "structure")
 (#set! cq.kind "function_structure")
 (#set! cq.anchor "structure.function"))

((class_definition body: (block) @structure.body) @structure.class
 (#set! cq.emit "structure")
 (#set! cq.kind "class_structure")
 (#set! cq.anchor "structure.class"))

((return_statement) @structure.return
 (#set! cq.emit "structure")
 (#set! cq.kind "return")
 (#set! cq.anchor "structure.return"))

((if_statement) @structure.if
 (#set! cq.emit "structure")
 (#set! cq.kind "if")
 (#set! cq.anchor "structure.if"))

((try_statement) @structure.try
 (#set! cq.emit "structure")
 (#set! cq.kind "try")
 (#set! cq.anchor "structure.try"))

((with_statement) @structure.with
 (#set! cq.emit "structure")
 (#set! cq.kind "with")
 (#set! cq.anchor "structure.with"))
