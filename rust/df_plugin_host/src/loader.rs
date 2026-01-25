use std::mem::size_of;
use std::path::Path;

use abi_stable::library::RootModule;
use datafusion::error::{DataFusionError, Result};

use df_plugin_api::{DfPluginManifestV1, DfPluginMod_Ref, DF_PLUGIN_ABI_MAJOR};

pub struct PluginHandle {
    module: DfPluginMod_Ref,
    manifest: DfPluginManifestV1,
}

impl PluginHandle {
    pub fn manifest(&self) -> &DfPluginManifestV1 {
        &self.manifest
    }

    pub(crate) fn module(&self) -> &DfPluginMod_Ref {
        &self.module
    }
}

fn parse_major(version: &str) -> Result<u16> {
    let Some((major, _)) = version.split_once('.') else {
        return Err(DataFusionError::Plan(format!(
            "Invalid version string {version:?}"
        )));
    };
    major.parse::<u16>().map_err(|err| {
        DataFusionError::Plan(format!("Invalid version string {version:?}: {err}"))
    })
}

fn validate_manifest(manifest: &DfPluginManifestV1) -> Result<()> {
    if manifest.plugin_abi_major != DF_PLUGIN_ABI_MAJOR {
        return Err(DataFusionError::Plan(format!(
            "Plugin ABI mismatch: expected {expected} got {actual}",
            expected = DF_PLUGIN_ABI_MAJOR,
            actual = manifest.plugin_abi_major,
        )));
    }
    let expected_size = size_of::<DfPluginManifestV1>() as u32;
    if manifest.struct_size < expected_size {
        return Err(DataFusionError::Plan(format!(
            "Plugin manifest struct_size {actual} is smaller than expected {expected}",
            actual = manifest.struct_size,
            expected = expected_size,
        )));
    }
    let ffi_major = datafusion_ffi::version();
    if manifest.df_ffi_major != ffi_major {
        return Err(DataFusionError::Plan(format!(
            "Plugin FFI major mismatch: expected {expected} got {actual}",
            expected = ffi_major,
            actual = manifest.df_ffi_major,
        )));
    }
    let host_datafusion = parse_major(datafusion::DATAFUSION_VERSION)?;
    if manifest.datafusion_major != host_datafusion {
        return Err(DataFusionError::Plan(format!(
            "Plugin DataFusion major mismatch: expected {expected} got {actual}",
            expected = host_datafusion,
            actual = manifest.datafusion_major,
        )));
    }
    let host_arrow = parse_major(arrow::ARROW_VERSION)?;
    if manifest.arrow_major != host_arrow {
        return Err(DataFusionError::Plan(format!(
            "Plugin Arrow major mismatch: expected {expected} got {actual}",
            expected = host_arrow,
            actual = manifest.arrow_major,
        )));
    }
    Ok(())
}

pub fn load_plugin(path: &Path) -> Result<PluginHandle> {
    let module = DfPluginMod_Ref::load_from_file(path).map_err(|err| {
        DataFusionError::Plan(format!(
            "Failed to load DataFusion plugin {path}: {err}",
            path = path.display()
        ))
    })?;
    let manifest = (module.manifest())();
    validate_manifest(&manifest)?;
    Ok(PluginHandle { module, manifest })
}

#[cfg(test)]
mod tests {
    use super::parse_major;
    use super::validate_manifest;
    use abi_stable::std_types::{RString, RVec};
    use df_plugin_api::{DfPluginManifestV1, DF_PLUGIN_ABI_MAJOR, DF_PLUGIN_ABI_MINOR};
    use std::mem::size_of;

    #[test]
    fn parse_major_handles_semver() {
        let major = parse_major("51.0.0").expect("parse failed");
        assert_eq!(major, 51);
    }

    fn sample_manifest() -> DfPluginManifestV1 {
        DfPluginManifestV1 {
            struct_size: size_of::<DfPluginManifestV1>() as u32,
            plugin_abi_major: DF_PLUGIN_ABI_MAJOR,
            plugin_abi_minor: DF_PLUGIN_ABI_MINOR,
            df_ffi_major: datafusion_ffi::version(),
            datafusion_major: parse_major(datafusion::DATAFUSION_VERSION)
                .expect("datafusion version"),
            arrow_major: parse_major(arrow::ARROW_VERSION).expect("arrow version"),
            plugin_name: RString::from("df_plugin_test"),
            plugin_version: RString::from("0.1.0"),
            build_id: RString::from("test"),
            capabilities: 0,
            features: RVec::new(),
        }
    }

    #[test]
    fn validate_manifest_accepts_matching_versions() {
        let manifest = sample_manifest();
        validate_manifest(&manifest).expect("manifest validation");
    }

    #[test]
    fn validate_manifest_rejects_abi_major() {
        let mut manifest = sample_manifest();
        manifest.plugin_abi_major = DF_PLUGIN_ABI_MAJOR + 1;
        assert!(validate_manifest(&manifest).is_err());
    }
}
