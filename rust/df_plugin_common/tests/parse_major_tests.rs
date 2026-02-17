use df_plugin_common::parse_major;

#[test]
fn parse_major_accepts_semver() {
    assert_eq!(parse_major("51.0.0").expect("parse"), 51);
}

#[test]
fn parse_major_rejects_invalid() {
    assert!(parse_major("v51").is_err());
}
