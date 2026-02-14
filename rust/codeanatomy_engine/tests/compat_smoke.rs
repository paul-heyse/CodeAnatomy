//! WS0: API Compatibility Smoke Tests
//!
//! This module contains compile-checked API conformance tests that validate
//! we're using correct DataFusion 51.0.0 / DeltaLake 0.30.1 API surfaces.
//!
//! Each test verifies a specific API that the implementation plan depends on.
//! These tests must compile and pass to ensure version compatibility.
//!
//! Tests correspond to P0 corrections identified in the implementation plan:
//! - RuntimeEnv builder API (FairSpillPool, DiskManagerBuilder)
//! - SessionConfig typed mutation API
//! - Version capture API (crate_version, DATAFUSION_VERSION)
//! - DeltaTable provider type signatures
//! - View registration API (into_view)
//! - Physical plan API (create_physical_plan)
//! - Write table API (write_table returns Vec<RecordBatch>)
//! - Rule registration via SessionStateBuilder

use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::*;
use deltalake::delta_datafusion::{DeltaScanConfig, DeltaTableProvider};
use deltalake::DeltaTable;

/// P0 Correction #1: RuntimeEnv builder API
///
/// Verifies:
/// - `RuntimeEnvBuilder::default()` exists
/// - `with_memory_pool(Arc::new(FairSpillPool::new(...)))` works
/// - `DiskManagerBuilder::new().with_mode(DiskManagerMode::OsTmpDirectory)` works
/// - `with_disk_manager_builder()` exists
#[test]
fn test_runtime_env_builder_api() {
    // Verify FairSpillPool construction
    let pool = Arc::new(FairSpillPool::new(1024 * 1024 * 100)); // 100MB

    // Verify DiskManagerBuilder API
    let disk_manager_builder =
        DiskManagerBuilder::default().with_mode(DiskManagerMode::OsTmpDirectory);

    // Verify RuntimeEnvBuilder chaining
    let runtime_env_result = RuntimeEnvBuilder::default()
        .with_memory_pool(pool)
        .with_disk_manager_builder(disk_manager_builder)
        .build();

    assert!(
        runtime_env_result.is_ok(),
        "RuntimeEnv build should succeed"
    );
}

/// P0 Correction #2: SessionConfig typed mutation API
///
/// Verifies:
/// - `SessionConfig::new()` with method chaining works
/// - Typed config mutation via `options_mut().execution.parquet.*` works
/// - Common config methods exist (with_default_catalog_and_schema, with_information_schema, etc.)
#[test]
fn test_session_config_api() {
    // Verify SessionConfig builder pattern
    let mut config = SessionConfig::new()
        .with_default_catalog_and_schema("datafusion", "public")
        .with_information_schema(true)
        .with_target_partitions(4)
        .with_batch_size(8192)
        .with_repartition_joins(true)
        .with_repartition_aggregations(true)
        .with_repartition_windows(true)
        .with_parquet_pruning(true);

    // Verify typed config mutation (NOT string-based set)
    config.options_mut().execution.parquet.pushdown_filters = true;
    config.options_mut().execution.parquet.reorder_filters = true;
    config.options_mut().execution.parquet.enable_page_index = true;

    // Verify we can read back the values
    assert_eq!(config.options().execution.target_partitions, 4);
    assert_eq!(config.options().execution.batch_size, 8192);
    assert!(config.options().execution.parquet.pushdown_filters);
}

/// P0 Correction #3: Version capture API
///
/// Verifies:
/// - `datafusion::DATAFUSION_VERSION` constant exists
/// - `deltalake::crate_version()` function exists
/// - `env!("CARGO_PKG_VERSION")` works for engine version
#[test]
fn test_version_capture_api() {
    // Verify DataFusion version constant
    let datafusion_version = datafusion::DATAFUSION_VERSION;
    assert!(
        !datafusion_version.is_empty(),
        "DataFusion version should not be empty"
    );
    assert!(
        datafusion_version.starts_with("51."),
        "Expected DataFusion 51.x"
    );

    // Verify DeltaLake crate_version function
    let deltalake_version = deltalake::crate_version();
    assert!(
        !deltalake_version.is_empty(),
        "DeltaLake version should not be empty"
    );
    assert!(
        deltalake_version.starts_with("0.30."),
        "Expected DeltaLake 0.30.x"
    );

    // Verify engine version from Cargo.toml
    let engine_version = env!("CARGO_PKG_VERSION");
    assert!(
        !engine_version.is_empty(),
        "Engine version should not be empty"
    );
}

/// P0 Correction #5: View registration API
///
/// Verifies:
/// - `df.into_view()` exists and returns a view
/// - `ctx.register_table("name", view)` works for registration
/// - NOT using non-existent `register_view()` method
#[tokio::test]
async fn test_view_registration_api() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Create sample data
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )?;

    let df = ctx.read_batch(batch)?;

    // Verify into_view() exists
    let view = df.into_view();

    // Verify register_table works with view (NOT register_view)
    ctx.register_table("test_view", view)?;

    // Verify we can query the registered view
    let result = ctx.sql("SELECT * FROM test_view").await?;
    let batches = result.collect().await?;
    assert_eq!(batches.len(), 1);

    Ok(())
}

/// P0 Correction #6: Physical plan API
///
/// Verifies:
/// - `df.create_physical_plan().await` exists
/// - NOT using non-existent `execution_plan()` method
#[tokio::test]
async fn test_physical_plan_api() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Create sample data
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )?;

    let df = ctx.read_batch(batch)?;

    // Verify create_physical_plan exists and returns ExecutionPlan
    let physical_plan = df.create_physical_plan().await?;

    // Verify we got a valid physical plan
    assert_eq!(physical_plan.schema().fields().len(), 1);
    assert_eq!(physical_plan.schema().field(0).name(), "id");

    Ok(())
}

/// P0 Correction #7: Write table API
///
/// Verifies:
/// - `df.write_table("name", DataFrameWriteOptions::new()).await` exists
/// - Returns `Vec<RecordBatch>` directly (no .collect() needed)
#[tokio::test]
async fn test_write_table_api() -> datafusion::error::Result<()> {
    use datafusion::datasource::MemTable;

    let ctx = SessionContext::new();

    // Create sample data
    let schema = Arc::new(Schema::new(vec![Field::new(
        "data",
        DataType::Int32,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![10, 20, 30]))],
    )?;

    // Register destination table first (write_table requires existing table)
    // MemTable needs at least one partition (even if empty)
    let dest_table = MemTable::try_new(schema.clone(), vec![vec![]])?;
    ctx.register_table("test_output", Arc::new(dest_table))?;

    let df = ctx.read_batch(batch)?;

    // Verify write_table exists and returns Vec<RecordBatch> directly
    let result_batches: Vec<RecordBatch> = df
        .write_table("test_output", DataFrameWriteOptions::new())
        .await?;

    // Verify the result type (compile-time check that Vec<RecordBatch> is returned)
    let _: &[RecordBatch] = &result_batches;

    Ok(())
}

/// P0 Correction #8: Rule registration via SessionStateBuilder
///
/// Verifies:
/// - `SessionStateBuilder::from(state)` exists
/// - `.with_optimizer_rules()` exists
/// - `.with_physical_optimizer_rules()` exists
/// - `.with_analyzer_rules()` exists
/// - NOT using post-build mutation
#[tokio::test]
async fn test_rule_registration_via_builder() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    let state = ctx.state();

    // Verify SessionStateBuilder::from exists and accepts SessionState
    let builder = SessionStateBuilder::from(state);

    // Verify rule registration methods exist (empty vecs for smoke test)
    let new_state = builder
        .with_optimizer_rules(vec![])
        .with_physical_optimizer_rules(vec![])
        .with_analyzer_rules(vec![])
        .build();

    // Verify we can create a new context from the built state
    let new_ctx = SessionContext::new_with_state(new_state);

    // Verify basic functionality works
    let df = new_ctx.sql("SELECT 1 as value").await?;
    let batches = df.collect().await?;
    assert_eq!(batches.len(), 1);

    Ok(())
}

/// P0 Correction #4: Delta provider type signatures
///
/// Verifies:
/// - `DeltaTableProvider` type exists
/// - `DeltaScanConfig` type exists
/// - `DeltaTable` type exists
/// - Types are usable in function signatures
///
/// Note: This is a type-signature check only. Actual table loading
/// delegates to existing `datafusion_ext::delta_control_plane`.
#[test]
fn test_delta_provider_api() {
    // Type signature verification function
    fn _verify_delta_types(
        _provider: DeltaTableProvider,
        _config: DeltaScanConfig,
        _table: DeltaTable,
    ) {
        // This function only needs to compile, proving the types exist
        // and are compatible with our usage patterns
    }

    // If this test compiles, the API surface is correct.
}

/// Feature gate: delta-planner
///
/// Verifies that the `delta-planner` feature gate compiles correctly
/// and that the DeltaPlanner type is accessible from the deltalake crate.
#[cfg(feature = "delta-planner")]
#[test]
fn test_delta_planner_feature_compiles() {
    use deltalake::delta_datafusion::planner::DeltaPlanner;

    // Type-level assertion: DeltaPlanner can be constructed.
    let _planner = DeltaPlanner::new();
}

/// Feature gate: delta-codec
///
/// Verifies that the `delta-codec` feature gate compiles correctly
/// and that the plan_codec module's encode function is accessible.
#[cfg(feature = "delta-codec")]
#[test]
fn test_delta_codec_feature_compiles() {
    use codeanatomy_engine::compiler::plan_codec::encode_with_delta_codecs;

    // Type-level assertion: the function exists and is importable.
    // Actual invocation requires a valid plan; compile check is sufficient.
    type EncodeWithDeltaCodecsFn = fn(
        &datafusion::logical_expr::LogicalPlan,
        Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    ) -> datafusion_common::Result<(Vec<u8>, Vec<u8>)>;
    let _fn_ref: EncodeWithDeltaCodecsFn = encode_with_delta_codecs;
}

/// Feature gate: delta-codec install
///
/// Verifies that `install_delta_codecs` works with a real SessionContext
/// when the `delta-codec` feature is enabled.
#[cfg(feature = "delta-codec")]
#[test]
fn test_delta_codec_install_compiles() {
    use codeanatomy_engine::compiler::plan_codec::install_delta_codecs;

    let ctx = SessionContext::new();
    // Should not panic — installs Delta codecs as config extensions.
    install_delta_codecs(&ctx);
}

// ---------------------------------------------------------------------------
// Scope 2: FormatPolicySpec, build_table_options, default_file_formats compile/link
// ---------------------------------------------------------------------------

/// Scope 2: Verify that FormatPolicySpec, build_table_options, and
/// default_file_formats compile and link correctly as public API.
#[test]
fn test_format_policy_types_compile_and_link() {
    use codeanatomy_engine::session::format_policy::{
        build_table_options, default_file_formats, FormatPolicySpec,
    };

    // Type-level: FormatPolicySpec is constructible.
    let policy = FormatPolicySpec {
        parquet_pushdown_filters: true,
        parquet_enable_page_index: false,
        csv_delimiter: Some("|".into()),
    };

    // Function-level: build_table_options is callable.
    let config = SessionConfig::new();
    let result = build_table_options(&config, &policy);
    assert!(result.is_ok(), "build_table_options must succeed");

    // Function-level: default_file_formats is callable.
    let formats = default_file_formats();
    // Currently returns empty vec; compile/link is the assertion.
    let _ = formats;
}

// ---------------------------------------------------------------------------
// Scope 7: Feature-gated types compile under delta-planner / delta-codec
// ---------------------------------------------------------------------------
// (Already tested above by test_delta_planner_feature_compiles and
// test_delta_codec_feature_compiles / test_delta_codec_install_compiles.)

// ---------------------------------------------------------------------------
// Scope 12: RuntimeProfileCoverage, evaluate_profile_coverage compile/link
// ---------------------------------------------------------------------------

/// Scope 12: RuntimeProfileCoverage and evaluate_profile_coverage compile and
/// link as public API.
#[test]
fn test_profile_coverage_types_compile_and_link() {
    use codeanatomy_engine::session::profile_coverage::{
        applied_count, evaluate_profile_coverage, reserved_count, CoverageState,
        RuntimeProfileCoverage,
    };

    // Type-level: RuntimeProfileCoverage is constructible.
    let entry = RuntimeProfileCoverage {
        field: "test_field".to_string(),
        state: CoverageState::Applied,
        note: "Compile-time check".to_string(),
    };
    assert_eq!(entry.state, CoverageState::Applied);

    // Function-level: evaluate_profile_coverage returns non-empty results.
    let coverage = evaluate_profile_coverage();
    assert!(!coverage.is_empty());

    // Function-level: applied_count and reserved_count are callable.
    let applied = applied_count();
    let reserved = reserved_count();
    assert_eq!(applied + reserved, coverage.len());
}
