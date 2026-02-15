((type_identifier) @type.identifier
 (#set! cq.emit "types")
 (#set! cq.kind "type_identifier")
 (#set! cq.anchor "type.identifier"))

((primitive_type) @type.primitive
 (#set! cq.emit "types")
 (#set! cq.kind "primitive_type")
 (#set! cq.anchor "type.primitive"))

((generic_type type: (_) @type.generic.base) @type.generic
 (#set! cq.emit "types")
 (#set! cq.kind "generic_type")
 (#set! cq.anchor "type.generic.base"))
