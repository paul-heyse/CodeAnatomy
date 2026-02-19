use pyo3::prelude::*;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use tokio::runtime::Runtime;

use codeanatomy_engine::compiler::compile_contract::{
    compile_request, compile_response_to_json, CompileRequest,
};
use codeanatomy_engine::compiler::spec_helpers::{
    build_join_edges, build_transform, canonical_rulepack_profile, default_rule_intents,
    join_group_index, SemanticIrPayload,
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
        self.runtime.block_on(async {
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
        let semantic_ir: SemanticIrPayload =
            serde_json::from_str(semantic_ir_json).map_err(|err| {
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
        let executable_views = semantic_ir
            .views
            .iter()
            .filter(|view| !view.kind.eq_ignore_ascii_case("diagnostic"))
            .collect::<Vec<_>>();
        let available_inputs: HashSet<&str> =
            request.input_locations.keys().map(String::as_str).collect();
        let mut executable_views = executable_views;
        loop {
            let retained_names: HashSet<&str> = executable_views
                .iter()
                .map(|view| view.name.as_str())
                .collect();
            let prior_len = executable_views.len();
            executable_views = executable_views
                .into_iter()
                .filter(|view| {
                    view.inputs.iter().all(|input| {
                        let source = input.as_str();
                        available_inputs.contains(source) || retained_names.contains(source)
                    })
                })
                .collect();
            if executable_views.len() == prior_len {
                break;
            }
        }
        if executable_views.is_empty() {
            return Err(engine_execution_error(
                "validation",
                "SEMANTIC_IR_MISSING_EXECUTABLE_VIEWS",
                "Semantic IR does not contain executable views after filtering diagnostics",
                None,
            ));
        }
        let view_names: HashSet<&str> = executable_views
            .iter()
            .map(|view| view.name.as_str())
            .collect();
        let group_by_relationship = join_group_index(&semantic_ir.join_groups);
        let join_edges = build_join_edges(&semantic_ir.join_groups).map_err(|err| {
            engine_execution_error(
                "validation",
                "JOIN_EDGE_DERIVATION_FAILED",
                format!("Failed to derive join edges: {err}"),
                None,
            )
        })?;

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

        let view_definitions = executable_views
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
                    "transform": build_transform(view, &group_by_relationship).map_err(|err| {
                        engine_execution_error(
                            "validation",
                            "VIEW_TRANSFORM_DERIVATION_FAILED",
                            format!("Failed to derive transform for view '{}': {err}", view.name),
                            None,
                        )
                    })?,
                    "output_schema": {"columns": {}}
                }))
            })
            .collect::<PyResult<Vec<_>>>()?;

        let default_output_source = executable_views
            .iter()
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

#[cfg(test)]
mod tests {
    use super::parse_and_validate_spec;
    use codeanatomy_engine::spec::execution_spec::SPEC_SCHEMA_VERSION;
    use serde_json::json;

    fn minimal_spec_json(version: u32) -> String {
        serde_json::to_string(&json!({
            "version": version,
            "input_relations": [
                {
                    "logical_name": "input",
                    "delta_location": "memory:///input",
                    "requires_lineage": false,
                    "version_pin": null
                }
            ],
            "view_definitions": [
                {
                    "name": "view1",
                    "view_kind": "project",
                    "view_dependencies": [],
                    "transform": {
                        "kind": "Project",
                        "source": "input",
                        "columns": ["id"]
                    },
                    "output_schema": {"columns": {"id": "Int64"}}
                }
            ],
            "join_graph": {"edges": [], "constraints": []},
            "output_targets": [
                {
                    "table_name": "out",
                    "delta_location": null,
                    "source_view": "view1",
                    "columns": [],
                    "materialization_mode": "Overwrite",
                    "partition_by": [],
                    "write_metadata": {},
                    "max_commit_retries": null
                }
            ],
            "rule_intents": [],
            "rulepack_profile": "Default"
        }))
        .expect("serialize minimal spec")
    }

    #[test]
    fn parse_valid_spec_returns_ok() {
        let result = parse_and_validate_spec(minimal_spec_json(SPEC_SCHEMA_VERSION).as_str());
        assert!(result.is_ok(), "valid spec should parse: {result:?}");
    }

    #[test]
    fn parse_spec_below_min_version_returns_err() {
        let result = parse_and_validate_spec(minimal_spec_json(0).as_str());
        assert!(result.is_err());
    }

    #[test]
    fn parse_empty_view_definitions_returns_err() {
        let payload = json!({
            "version": SPEC_SCHEMA_VERSION,
            "input_relations": [],
            "view_definitions": [],
            "join_graph": {"edges": [], "constraints": []},
            "output_targets": [],
            "rule_intents": [],
            "rulepack_profile": "Default"
        });
        let json = serde_json::to_string(&payload).expect("serialize empty view spec");
        let result = parse_and_validate_spec(&json);
        assert!(result.is_err());
    }
}
