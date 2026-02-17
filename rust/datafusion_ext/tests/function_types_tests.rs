use datafusion_ext::function_types::FunctionKind;

#[test]
fn function_kind_round_trips_with_serde() {
    let value = FunctionKind::Aggregate;
    let encoded = serde_json::to_string(&value).expect("serialize");
    let decoded: FunctionKind = serde_json::from_str(&encoded).expect("deserialize");
    assert_eq!(decoded, value);
}

