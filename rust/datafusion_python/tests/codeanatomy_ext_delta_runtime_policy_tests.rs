use pyo3::prelude::*;
use pyo3::types::{PyDict, PyModule};

fn build_module(py: Python<'_>) -> Bound<'_, PyModule> {
    let module = PyModule::new(py, "delta_runtime_policy_test").expect("create module");
    datafusion_python::codeanatomy_ext::init_internal_module(py, &module)
        .expect("init internal module");
    module
}

#[test]
fn delta_runtime_policy_bridge_applies_memory_and_cache_limits() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = build_module(py);
        let policy_cls = module
            .getattr("DeltaSessionRuntimePolicyOptions")
            .expect("runtime policy class exported");
        let policy = policy_cls.call0().expect("instantiate runtime policy options");
        let memory_limit_bytes = 16_u64 * 1024_u64 * 1024_u64;
        let metadata_limit_bytes = 8_u64 * 1024_u64 * 1024_u64;
        policy
            .setattr("memory_limit", memory_limit_bytes)
            .expect("set memory_limit");
        policy
            .setattr("metadata_cache_limit", metadata_limit_bytes)
            .expect("set metadata_cache_limit");
        policy
            .setattr("list_files_cache_limit", 2_u64 * 1024_u64 * 1024_u64)
            .expect("set list_files_cache_limit");
        policy
            .setattr("list_files_cache_ttl_seconds", 30_u64)
            .expect("set list_files_cache_ttl_seconds");

        let kwargs = PyDict::new(py);
        kwargs
            .set_item("runtime_policy", &policy)
            .expect("set runtime_policy kwarg");
        let ctx = module
            .call_method("delta_session_context", (), Some(&kwargs))
            .expect("build delta session context");

        let probe = module
            .call_method1("session_context_contract_probe", (ctx.clone(),))
            .expect("session context contract probe");
        let payload = probe.downcast::<PyDict>().expect("probe returns dict");
        let metadata_limit = payload
            .get_item("metadata_cache_limit_bytes")
            .expect("metadata_cache_limit_bytes lookup")
            .expect("metadata_cache_limit_bytes present")
            .extract::<i64>()
            .expect("metadata_cache_limit_bytes as int");
        assert_eq!(metadata_limit, metadata_limit_bytes as i64);
        let list_files_limit = payload
            .get_item("list_files_cache_limit_bytes")
            .expect("list_files_cache_limit_bytes lookup")
            .expect("list_files_cache_limit_bytes present")
            .extract::<i64>()
            .expect("list_files_cache_limit_bytes as int");
        assert_eq!(list_files_limit, 2_i64 * 1024_i64 * 1024_i64);
        let list_files_ttl = payload
            .get_item("list_files_cache_ttl_seconds")
            .expect("list_files_cache_ttl_seconds lookup")
            .expect("list_files_cache_ttl_seconds present")
            .extract::<i64>()
            .expect("list_files_cache_ttl_seconds as int");
        assert_eq!(list_files_ttl, 30);

        let memory_limit_kind = payload
            .get_item("memory_limit_kind")
            .expect("memory_limit_kind lookup")
            .expect("memory_limit_kind present")
            .extract::<String>()
            .expect("memory_limit_kind as string");
        assert_eq!(memory_limit_kind, "finite");
        let memory_limit = payload
            .get_item("memory_limit_bytes")
            .expect("memory_limit_bytes lookup")
            .expect("memory_limit_bytes present")
            .extract::<i64>()
            .expect("memory_limit_bytes as int");
        assert_eq!(memory_limit, memory_limit_bytes as i64);
    });
}
