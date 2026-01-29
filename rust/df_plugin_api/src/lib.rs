#![allow(non_camel_case_types)]

mod manifest;

use abi_stable::StableAbi;
use abi_stable::library::RootModule;
use abi_stable::sabi_types::version::VersionStrings;
use abi_stable::std_types::{ROption, RResult, RString, RStr, RVec};
use datafusion_ffi::table_provider::FFI_TableProvider;
use datafusion_ffi::udaf::FFI_AggregateUDF;
use datafusion_ffi::udf::FFI_ScalarUDF;
use datafusion_ffi::udtf::FFI_TableFunction;
use datafusion_ffi::udwf::FFI_WindowUDF;

pub use manifest::{caps, DfPluginManifestV1, DF_PLUGIN_ABI_MAJOR, DF_PLUGIN_ABI_MINOR};

pub type DfResult<T> = RResult<T, RString>;

#[repr(C)]
#[derive(StableAbi)]
pub struct DfUdfBundleV1 {
    pub scalar: RVec<FFI_ScalarUDF>,
    pub aggregate: RVec<FFI_AggregateUDF>,
    pub window: RVec<FFI_WindowUDF>,
}

#[repr(C)]
#[derive(StableAbi)]
pub struct DfTableFunctionV1 {
    pub name: RString,
    pub function: FFI_TableFunction,
}

#[repr(C)]
#[derive(StableAbi)]
pub struct DfPluginExportsV1 {
    pub table_provider_names: RVec<RString>,
    pub udf_bundle: DfUdfBundleV1,
    pub table_functions: RVec<DfTableFunctionV1>,
}

#[repr(C)]
#[derive(StableAbi)]
#[allow(non_camel_case_types)]
#[sabi(kind(Prefix(prefix_ref = DfPluginMod_Ref)))]
#[sabi(missing_field(panic))]
pub struct DfPluginMod {
    pub manifest: extern "C" fn() -> DfPluginManifestV1,
    pub exports: extern "C" fn() -> DfPluginExportsV1,
    pub udf_bundle_with_options: extern "C" fn(
        options_json: ROption<RString>,
    ) -> DfResult<DfUdfBundleV1>,
    #[sabi(last_prefix_field)]
    pub create_table_provider: extern "C" fn(
        name: RStr<'_>,
        options_json: ROption<RString>,
    ) -> DfResult<FFI_TableProvider>,
}

impl RootModule for DfPluginMod_Ref {
    abi_stable::declare_root_module_statics! {DfPluginMod_Ref}

    const BASE_NAME: &'static str = "df_plugin";
    const NAME: &'static str = "df_plugin";
    const VERSION_STRINGS: VersionStrings = abi_stable::package_version_strings!();
}
