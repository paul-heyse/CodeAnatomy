use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion_python::common::data_type::DataTypeMap;

#[test]
fn scalar_utf8_maps_to_utf8_arrow_type() {
    let mapped = DataTypeMap::map_from_scalar_value(&ScalarValue::Utf8(Some("x".to_string())))
        .expect("utf8 scalar should map");
    assert_eq!(mapped.arrow_type.data_type, DataType::Utf8);
}

#[test]
fn union_variant_error_label_mentions_union() {
    let source = include_str!("../src/common/data_type.rs");
    let compact: String = source.chars().filter(|ch| !ch.is_whitespace()).collect();

    assert!(
        compact.contains("ScalarValue::Union(_,_,_)=>Err(PyNotImplementedError::new_err(\"ScalarValue::Union\".to_string(),))"),
        "Union branch should emit ScalarValue::Union label"
    );
    assert!(
        !compact.contains("ScalarValue::Union(_,_,_)=>Err(PyNotImplementedError::new_err(\"ScalarValue::LargeList\".to_string(),))"),
        "Union branch must not be mislabeled as ScalarValue::LargeList"
    );
}
