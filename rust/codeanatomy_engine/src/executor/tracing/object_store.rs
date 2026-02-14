//! Object store instrumentation wiring.

use std::collections::BTreeSet;

use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
use datafusion_common::{DataFusionError, Result};
use deltalake::ensure_table_uri;
use instrumented_object_store::instrument_object_store;
use url::{Position, Url};

use crate::spec::runtime::TracingConfig;

/// Register instrumented `file://` object store when enabled.
pub fn register_instrumented_file_store(
    ctx: &SessionContext,
    config: &TracingConfig,
) -> Result<()> {
    if !config.enabled || !config.instrument_object_store {
        return Ok(());
    }
    let file = vec!["file:///".to_string()];
    register_instrumented_stores_for_locations(ctx, config, &file)
}

/// Register instrumented stores for a set of table locations.
pub fn register_instrumented_stores_for_locations(
    ctx: &SessionContext,
    config: &TracingConfig,
    locations: &[String],
) -> Result<()> {
    if !config.enabled || !config.instrument_object_store {
        return Ok(());
    }
    if locations.is_empty() {
        return Ok(());
    }

    let mut store_urls: Vec<ObjectStoreUrl> = Vec::new();
    let mut seen = BTreeSet::new();
    for location in locations {
        let store_url = object_store_url_for_location(location)?;
        let key = store_url.to_string();
        if seen.insert(key) {
            store_urls.push(store_url);
        }
    }
    store_urls.sort_by_key(|left| left.to_string());

    for store_url in store_urls {
        register_instrumented_store(ctx, &store_url)?;
    }
    Ok(())
}

fn register_instrumented_store(ctx: &SessionContext, store_url: &ObjectStoreUrl) -> Result<()> {
    let runtime_env = ctx.runtime_env();
    let base_store = runtime_env.object_store(store_url).map_err(|error| {
        DataFusionError::Plan(format!(
            "Unable to resolve object store '{}' for tracing instrumentation: {error}",
            AsRef::<str>::as_ref(store_url)
        ))
    })?;
    let label = format!(
        "codeanatomy_store_{}",
        sanitize_label(AsRef::<str>::as_ref(store_url))
    );
    let instrumented_store = instrument_object_store(base_store, label.as_str());
    ctx.register_object_store(AsRef::<Url>::as_ref(store_url), instrumented_store);
    Ok(())
}

fn sanitize_label(value: &str) -> String {
    value
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}

fn object_store_url_for_location(location: &str) -> Result<ObjectStoreUrl> {
    let table_url = ensure_table_uri(location).map_err(|error| {
        DataFusionError::Plan(format!(
            "Invalid Delta table location '{location}': {error}"
        ))
    })?;
    validate_supported_scheme(&table_url)?;
    let base = match table_url.scheme() {
        "file" => "file:///".to_string(),
        "memory" => "memory://".to_string(),
        _ => {
            let authority = &table_url[Position::BeforeHost..Position::AfterPort];
            format!("{}://{}", table_url.scheme(), authority)
        }
    };
    ObjectStoreUrl::parse(base).map_err(|error| {
        DataFusionError::Plan(format!(
            "Invalid object store URL derived from location '{location}': {error}"
        ))
    })
}

fn validate_supported_scheme(url: &Url) -> Result<()> {
    let scheme = url.scheme().to_ascii_lowercase();
    let supported = matches!(
        scheme.as_str(),
        "file" | "memory" | "s3" | "gs" | "gcs" | "az" | "abfs" | "abfss"
    );
    if supported {
        return Ok(());
    }
    Err(DataFusionError::Plan(format!(
        "Unsupported object store scheme '{scheme}' for tracing instrumentation"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_store_url_for_location_file() {
        let url = object_store_url_for_location("/tmp/example").expect("file url");
        assert!(AsRef::<str>::as_ref(&url).starts_with("file://"));
    }

    #[test]
    fn test_object_store_url_for_location_rejects_unsupported_scheme() {
        let err = object_store_url_for_location("ftp://example.com/table")
            .expect_err("unsupported scheme must fail");
        let message = err.to_string();
        assert!(
            message.contains("Unsupported object store scheme")
                || message.contains("Invalid Delta table location"),
            "unexpected error message: {message}",
        );
    }
}
