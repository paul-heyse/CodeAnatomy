use std::collections::HashSet;

use datafusion_ext::udaf_builtin::builtin_udafs;

#[test]
fn arg_best_family_is_registered() {
    let names: HashSet<String> = builtin_udafs()
        .into_iter()
        .map(|udaf| udaf.name().to_string())
        .collect();

    for name in ["any_value_det", "arg_max", "arg_min", "asof_select"] {
        assert!(names.contains(name), "missing UDAF: {name}");
    }
}
