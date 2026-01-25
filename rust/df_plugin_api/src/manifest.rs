use abi_stable::StableAbi;
use abi_stable::std_types::{RString, RVec};

pub const DF_PLUGIN_ABI_MAJOR: u16 = 1;
pub const DF_PLUGIN_ABI_MINOR: u16 = 0;

pub mod caps {
    pub const TABLE_PROVIDER: u64 = 1 << 0;
    pub const SCALAR_UDF: u64 = 1 << 1;
    pub const AGG_UDF: u64 = 1 << 2;
    pub const WINDOW_UDF: u64 = 1 << 3;
    pub const TABLE_FUNCTION: u64 = 1 << 4;
}

#[repr(C)]
#[derive(Debug, StableAbi, Clone)]
pub struct DfPluginManifestV1 {
    pub struct_size: u32,
    pub plugin_abi_major: u16,
    pub plugin_abi_minor: u16,
    pub df_ffi_major: u64,
    pub datafusion_major: u16,
    pub arrow_major: u16,
    pub plugin_name: RString,
    pub plugin_version: RString,
    pub build_id: RString,
    pub capabilities: u64,
    pub features: RVec<RString>,
}
