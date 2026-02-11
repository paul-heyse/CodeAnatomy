use pyo3::prelude::*;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use tokio::runtime::Runtime;

use codeanatomy_engine::compiler::compile_contract::{
    compile_request, compile_response_to_json, CompileRequest,
};
use codeanatomy_engine::rules::rulepack::RulepackFactory;
use codeanatomy_engine::spec::execution_spec::SemanticExecutionSpec;
use codeanatomy_engine::spec::execution_spec::SPEC_SCHEMA_VERSION;

use super::errors::engine_execution_error;
use super::session::SessionFactory as PySessionFactory;

/// Semantic plan compiler that validates and prepares execution specs.
///
/// This is a thin Python wrapper that validates specs and prepares them
/// for execution. The actual DataFusion compilation happens in the
/// CpgMaterializer during execution.
#[pyclass]
pub struct SemanticPlanCompiler {
    #[allow(dead_code)]
    runtime: Arc<Runtime>,
}

#[pymethods]
impl SemanticPlanCompiler {
    #[new]
    fn new() -> PyResult<Self> {
        let runtime = Runtime::new().map_err(|e| {
            engine_execution_error(
                "runtime",
                "TOKIO_RUNTIME_INIT_FAILED",
                format!("Failed to create Tokio runtime: {e}"),
                None,
            )
        })?;
        Ok(Self {
            runtime: Arc::new(runtime),
        })
    }

    /// Compile a spec JSON into a validated, executable plan.
    ///
    /// This performs structural validation and hash computation but does not
    /// create a SessionContext (that happens during execution).
    ///
    /// Args:
    ///     spec_json: JSON-serialized SemanticExecutionSpec
    ///
    /// Returns:
    ///     CompiledPlan with validated spec and computed hash
    ///
    /// Raises:
    ///     ValueError: If JSON is malformed or spec is invalid
    fn compile(&self, spec_json: &str) -> PyResult<CompiledPlan> {
        let spec = parse_and_validate_spec(spec_json)?;
        Ok(CompiledPlan {
            spec_json: spec_json.to_string(),
            spec_hash: spec.spec_hash,
        })
    }

    /// Compile a spec to deterministic compile metadata JSON without materialization.
    ///
    /// This runs session preparation + logical compilation + artifact capture
    /// and returns a JSON-serialized compile contract payload.
    fn compile_metadata_json(
        &self,
        session_factory: &PySessionFactory,
        spec_json: &str,
    ) -> PyResult<String> {
        let spec = parse_and_validate_spec(spec_json)?;
        let env_profile = session_factory.get_profile();
        let ruleset = RulepackFactory::build_ruleset(
            &spec.rulepack_profile,
            &spec.rule_intents,
            &env_profile,
        );
        let tracing = spec.runtime.effective_tracing();
        self.runtime
            .block_on(async {
                let response = compile_request(CompileRequest {
                    session_factory: session_factory.inner(),
                    spec: &spec,
                    ruleset: &ruleset,
                    tracing_config: Some(&tracing),
                })
                .await
                .map_err(|err| {
                    engine_execution_error(
                        "compilation",
                        "COMPILE_METADATA_CAPTURE_FAILED",
                        format!("Compile metadata capture failed: {err}"),
                        None,
                    )
                })?;
                compile_response_to_json(&response).map_err(|err| {
                    engine_execution_error(
                        "compilation",
                        "COMPILE_METADATA_SERIALIZE_FAILED",
                        format!("Failed to serialize compile metadata: {err}"),
                        None,
                    )
                })
            })
    }

    /// Build a canonical SemanticExecutionSpec JSON payload from semantic IR + request.
    ///
    /// This is the Rust-authoritative replacement for Python-side spec derivation.
    fn build_spec_json(&self, semantic_ir_json: &str, request_json: &str) -> PyResult<String> {
        let semantic_ir: SemanticIrPayload = serde_json::from_str(semantic_ir_json).map_err(|err| {
            engine_execution_error(
                "validation",
                "INVALID_SEMANTIC_IR_JSON",
                format!("Invalid semantic IR JSON: {err}"),
                Some(json!({"error": err.to_string()})),
            )
        })?;
        let request: BuildSpecRequest = serde_json::from_str(request_json).map_err(|err| {
            engine_execution_error(
                "validation",
                "INVALID_BUILD_SPEC_REQUEST",
                format!("Invalid spec build request JSON: {err}"),
                Some(json!({"error": err.to_string()})),
            )
        })?;
        if semantic_ir.views.is_empty() {
            return Err(engine_execution_error(
                "validation",
                "SEMANTIC_IR_MISSING_VIEWS",
                "Cannot build execution spec without at least one semantic IR view",
                None,
            ));
        }
        if request.output_targets.is_empty() {
            return Err(engine_execution_error(
                "validation",
                "SPEC_MISSING_OUTPUT_TARGETS",
                "Execution spec must include at least one output target",
                None,
            ));
        }

        let profile = canonical_rulepack_profile(request.rulepack_profile.as_deref());
        let view_names: HashSet<&str> =
            semantic_ir.views.iter().map(|view| view.name.as_str()).collect();
        let group_by_relationship = join_group_index(&semantic_ir.join_groups);
        let join_edges = build_join_edges(&semantic_ir.join_groups)?;

        let mut sorted_inputs: Vec<(&String, &String)> = request.input_locations.iter().collect();
        sorted_inputs.sort_by(|a, b| a.0.cmp(b.0));
        let input_relations = sorted_inputs
            .into_iter()
            .map(|(name, location)| {
                json!({
                    "logical_name": name,
                    "delta_location": location,
                    "requires_lineage": false,
                    "version_pin": null
                })
            })
            .collect::<Vec<_>>();

        let view_definitions = semantic_ir
            .views
            .iter()
            .map(|view| {
                let deps = view
                    .inputs
                    .iter()
                    .filter(|name| view_names.contains(name.as_str()))
                    .cloned()
                    .collect::<Vec<_>>();
                Ok(json!({
                    "name": view.name,
                    "view_kind": view.kind,
                    "view_dependencies": deps,
                    "transform": build_transform(view, &group_by_relationship)?,
                    "output_schema": {"columns": {}}
                }))
            })
            .collect::<PyResult<Vec<_>>>()?;

        let default_output_source = semantic_ir
            .views
            .last()
            .map(|view| view.name.clone())
            .unwrap_or_default();
        let output_targets = request
            .output_targets
            .iter()
            .map(|target| {
                let source_view = if view_names.contains(target.as_str()) {
                    target.clone()
                } else {
                    default_output_source.clone()
                };
                let delta_location = request
                    .output_locations
                    .as_ref()
                    .and_then(|locations| locations.get(target))
                    .cloned();
                json!({
                    "table_name": target,
                    "source_view": source_view,
                    "columns": [],
                    "delta_location": delta_location,
                    "materialization_mode": "Overwrite",
                    "partition_by": [],
                    "write_metadata": {},
                    "max_commit_retries": null
                })
            })
            .collect::<Vec<_>>();

        let runtime = request.runtime.unwrap_or_else(|| json!({}));
        let spec_value = json!({
            "version": SPEC_SCHEMA_VERSION,
            "input_relations": input_relations,
            "view_definitions": view_definitions,
            "join_graph": {
                "edges": join_edges,
                "constraints": [],
            },
            "output_targets": output_targets,
            "rule_intents": default_rule_intents(profile),
            "rulepack_profile": profile,
            "typed_parameters": [],
            "runtime": runtime,
        });
        let spec_json = serde_json::to_string(&spec_value).map_err(|err| {
            engine_execution_error(
                "validation",
                "SPEC_BUILD_SERIALIZE_FAILED",
                format!("Failed to serialize spec payload: {err}"),
                None,
            )
        })?;
        let spec = parse_and_validate_spec(&spec_json)?;
        serde_json::to_string(&spec).map_err(|err| {
            engine_execution_error(
                "validation",
                "SPEC_BUILD_SERIALIZE_FAILED",
                format!("Failed to serialize canonical spec payload: {err}"),
                None,
            )
        })
    }

    fn __repr__(&self) -> String {
        "SemanticPlanCompiler()".to_string()
    }
}

#[derive(Debug, Deserialize)]
struct BuildSpecRequest {
    input_locations: BTreeMap<String, String>,
    output_targets: Vec<String>,
    #[serde(default)]
    rulepack_profile: Option<String>,
    #[serde(default)]
    output_locations: Option<BTreeMap<String, String>>,
    #[serde(default)]
    runtime: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct SemanticIrPayload {
    views: Vec<SemanticIrViewPayload>,
    #[serde(default)]
    join_groups: Vec<SemanticIrJoinGroupPayload>,
}

#[derive(Debug, Deserialize)]
struct SemanticIrViewPayload {
    name: String,
    kind: String,
    #[serde(default)]
    inputs: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
struct SemanticIrJoinGroupPayload {
    #[serde(default)]
    left_view: String,
    #[serde(default)]
    right_view: String,
    #[serde(default)]
    left_on: Vec<String>,
    #[serde(default)]
    right_on: Vec<String>,
    #[serde(default)]
    how: String,
    #[serde(default)]
    relationship_names: Vec<String>,
}

fn canonical_rulepack_profile(raw: Option<&str>) -> &'static str {
    let normalized = raw.unwrap_or("default").trim().to_ascii_lowercase();
    match normalized.as_str() {
        "default" => "Default",
        "low_latency" | "lowlatency" => "LowLatency",
        "replay" => "Replay",
        "strict" => "Strict",
        _ => "Default",
    }
}

fn map_join_type(raw: &str) -> &'static str {
    match raw.trim().to_ascii_lowercase().as_str() {
        "inner" => "Inner",
        "left" => "Left",
        "right" => "Right",
        "full" => "Full",
        "semi" => "Semi",
        "anti" => "Anti",
        _ => "Inner",
    }
}

fn cpg_output_kind_for_view(name: &str) -> Option<&'static str> {
    match name {
        "cpg_nodes" => Some("Nodes"),
        "cpg_edges" => Some("Edges"),
        "cpg_props" => Some("Props"),
        "cpg_props_map" => Some("PropsMap"),
        "cpg_edges_by_src" => Some("EdgesBySrc"),
        "cpg_edges_by_dst" => Some("EdgesByDst"),
        _ => None,
    }
}

fn default_rule_intents(profile: &str) -> Vec<Value> {
    let mut baseline = vec![
        json!({"name": "semantic_integrity", "class": "SemanticIntegrity", "params": {}}),
        json!({"name": "span_containment_rewrite", "class": "SemanticIntegrity", "params": {}}),
        json!({"name": "delta_scan_aware", "class": "DeltaScanAware", "params": {}}),
        json!({"name": "cpg_physical", "class": "SemanticIntegrity", "params": {}}),
        json!({"name": "cost_shape", "class": "CostShape", "params": {}}),
    ];
    if matches!(profile, "Strict" | "Replay") {
        baseline.push(json!({"name": "strict_safety", "class": "Safety", "params": {}}));
    }
    baseline
}

fn join_group_index(
    groups: &[SemanticIrJoinGroupPayload],
) -> BTreeMap<String, SemanticIrJoinGroupPayload> {
    let mut by_relationship = BTreeMap::new();
    for group in groups {
        for relationship in &group.relationship_names {
            by_relationship.insert(relationship.clone(), group.clone());
        }
    }
    by_relationship
}

fn build_join_edges(groups: &[SemanticIrJoinGroupPayload]) -> PyResult<Vec<Value>> {
    groups
        .iter()
        .map(|group| {
            Ok(json!({
                "left_relation": group.left_view,
                "right_relation": group.right_view,
                "join_type": map_join_type(&group.how),
                "left_keys": group.left_on,
                "right_keys": group.right_on,
            }))
        })
        .collect::<PyResult<Vec<_>>>()
}

fn build_relate_transform(
    view: &SemanticIrViewPayload,
    by_relationship: &BTreeMap<String, SemanticIrJoinGroupPayload>,
) -> PyResult<Value> {
    if let Some(group) = by_relationship.get(&view.name) {
        let join_keys = group
            .left_on
            .iter()
            .zip(group.right_on.iter())
            .map(|(left, right)| json!({"left_key": left, "right_key": right}))
            .collect::<Vec<_>>();
        return Ok(json!({
            "kind": "Relate",
            "left": group.left_view,
            "right": group.right_view,
            "join_type": map_join_type(&group.how),
            "join_keys": join_keys,
        }));
    }
    if view.inputs.len() < 2 {
        return Err(engine_execution_error(
            "validation",
            "RELATE_VIEW_INPUTS_INVALID",
            format!("relate view '{}' must have two inputs", view.name),
            None,
        ));
    }
    Ok(json!({
        "kind": "Relate",
        "left": view.inputs[0],
        "right": view.inputs[1],
        "join_type": "Inner",
        "join_keys": [],
    }))
}

fn build_transform(
    view: &SemanticIrViewPayload,
    by_relationship: &BTreeMap<String, SemanticIrJoinGroupPayload>,
) -> PyResult<Value> {
    if let Some(output_kind) = cpg_output_kind_for_view(&view.name) {
        return Ok(json!({
            "kind": "CpgEmit",
            "output_kind": output_kind,
            "sources": view.inputs,
        }));
    }
    if view.kind == "relate" {
        return build_relate_transform(view, by_relationship);
    }
    if view.kind.starts_with("union") || view.inputs.len() > 1 {
        return Ok(json!({
            "kind": "Union",
            "sources": view.inputs,
            "discriminator_column": null,
            "distinct": false
        }));
    }
    if view.inputs.len() == 1 && matches!(view.kind.as_str(), "projection" | "project") {
        return Ok(json!({
            "kind": "Project",
            "source": view.inputs[0],
            "columns": []
        }));
    }
    if view.inputs.len() == 1 && view.kind == "aggregate" {
        return Ok(json!({
            "kind": "Aggregate",
            "source": view.inputs[0],
            "group_by": [],
            "aggregations": []
        }));
    }
    if view.inputs.len() == 1 && view.kind.ends_with("normalize") {
        return Ok(json!({
            "kind": "Normalize",
            "source": view.inputs[0],
            "id_columns": [],
            "span_columns": null,
            "text_columns": []
        }));
    }
    if view.inputs.len() == 1 {
        return Ok(json!({
            "kind": "Filter",
            "source": view.inputs[0],
            "predicate": "TRUE"
        }));
    }
    Err(engine_execution_error(
        "validation",
        "VIEW_TRANSFORM_DERIVATION_FAILED",
        format!(
            "Unable to derive transform for view '{}' with kind '{}'",
            view.name, view.kind
        ),
        None,
    ))
}

fn parse_and_validate_spec(spec_json: &str) -> PyResult<SemanticExecutionSpec> {
    // Hard-cutover contract: reject legacy template mode payloads explicitly.
    let payload: Value = serde_json::from_str(spec_json).map_err(|err| {
        engine_execution_error(
            "validation",
            "INVALID_SPEC_JSON",
            format!("Invalid spec JSON: {err}"),
            Some(json!({"error": err.to_string()})),
        )
    })?;
    if payload
        .as_object()
        .and_then(|obj| obj.get("parameter_templates"))
        .is_some()
    {
        return Err(engine_execution_error(
            "validation",
            "LEGACY_PARAMETER_TEMPLATES_UNSUPPORTED",
            "Legacy field 'parameter_templates' is no longer supported. Use typed_parameters.",
            None,
        ));
    }

    // Parse and validate spec structure with field-path diagnostics.
    let mut deserializer = serde_json::Deserializer::from_str(spec_json);
    let mut spec: SemanticExecutionSpec = serde_path_to_error::deserialize(&mut deserializer)
        .map_err(|err| {
            let path = err.path().to_string();
            let inner = err.into_inner();
            if path.is_empty() {
                engine_execution_error(
                    "validation",
                    "INVALID_SPEC_JSON",
                    format!("Invalid spec JSON: {inner}"),
                    Some(json!({"error": inner.to_string()})),
                )
            } else {
                engine_execution_error(
                    "validation",
                    "INVALID_SPEC_JSON",
                    format!("Invalid spec JSON at '{path}': {inner}"),
                    Some(json!({"path": path, "error": inner.to_string()})),
                )
            }
        })?;

    if spec.version < SPEC_SCHEMA_VERSION {
        return Err(engine_execution_error(
            "validation",
            "SPEC_VERSION_UNSUPPORTED",
            format!(
                "Spec version {} is below minimum required version {}",
                spec.version, SPEC_SCHEMA_VERSION
            ),
            Some(json!({"version": spec.version, "minimum": SPEC_SCHEMA_VERSION})),
        ));
    }

    // Compute canonical hash
    spec.spec_hash = codeanatomy_engine::spec::hashing::hash_spec(&spec);

    // Validate basic structure
    if spec.view_definitions.is_empty() {
        return Err(engine_execution_error(
            "validation",
            "SPEC_MISSING_VIEW_DEFINITIONS",
            "Spec must have at least one view definition",
            None,
        ));
    }
    if spec.output_targets.is_empty() {
        return Err(engine_execution_error(
            "validation",
            "SPEC_MISSING_OUTPUT_TARGETS",
            "Spec must have at least one output target",
            None,
        ));
    }
    Ok(spec)
}

/// Compiled execution plan ready for materialization.
///
/// Contains validated spec JSON and computed hash. The actual DataFusion
/// compilation happens during execution in CpgMaterializer.
#[pyclass]
pub struct CompiledPlan {
    spec_json: String,
    spec_hash: [u8; 32],
}

#[pymethods]
impl CompiledPlan {
    /// Get the spec JSON.
    fn spec_json(&self) -> &str {
        &self.spec_json
    }

    /// Get the spec hash as hex string.
    fn spec_hash_hex(&self) -> String {
        hex::encode(self.spec_hash)
    }

    /// Get the spec hash as bytes.
    fn spec_hash_bytes(&self) -> Vec<u8> {
        self.spec_hash.to_vec()
    }

    fn __repr__(&self) -> String {
        format!("CompiledPlan(spec_hash={})", hex::encode(self.spec_hash))
    }
}

impl CompiledPlan {
    /// Internal: parse the spec from stored JSON.
    pub(crate) fn parse_spec(&self) -> Result<SemanticExecutionSpec, serde_json::Error> {
        let mut spec: SemanticExecutionSpec = serde_json::from_str(&self.spec_json)?;
        spec.spec_hash = self.spec_hash;
        Ok(spec)
    }

    /// Internal: get the spec hash.
    #[allow(dead_code)]
    pub(crate) fn spec_hash(&self) -> [u8; 32] {
        self.spec_hash
    }
}

impl SemanticPlanCompiler {
    pub(crate) fn new_internal() -> PyResult<Self> {
        Self::new()
    }

    pub(crate) fn compile_internal(&self, spec_json: &str) -> PyResult<CompiledPlan> {
        self.compile(spec_json)
    }
}
