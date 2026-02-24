pub const FEATURE_MSG: &str = "Async UDFs require the async-udf feature";

pub fn string_only_hits() {
    let _copy = "Async UDFs require the async-udf feature";
    let _raw = r#"Async UDFs require the async-udf feature"#;
    println!("Async UDFs require the async-udf feature");
}
