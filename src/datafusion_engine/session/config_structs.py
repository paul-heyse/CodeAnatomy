"""Domain-scoped runtime profile configuration structures."""

from __future__ import annotations

import msgspec

from datafusion_engine.session.runtime_profile_config import (
    CatalogConfig,
    DataSourceConfig,
    DiagnosticsConfig,
    ExecutionConfig,
    FeatureGatesConfig,
    PolicyBundleConfig,
    ZeroRowBootstrapConfig,
)


class RuntimeProfileConfig(msgspec.Struct, frozen=True):
    """Composable runtime-profile configuration payload."""

    execution: ExecutionConfig = msgspec.field(default_factory=ExecutionConfig)
    catalog: CatalogConfig = msgspec.field(default_factory=CatalogConfig)
    data_sources: DataSourceConfig = msgspec.field(default_factory=DataSourceConfig)
    zero_row_bootstrap: ZeroRowBootstrapConfig = msgspec.field(
        default_factory=ZeroRowBootstrapConfig
    )
    features: FeatureGatesConfig = msgspec.field(default_factory=FeatureGatesConfig)
    diagnostics: DiagnosticsConfig = msgspec.field(default_factory=DiagnosticsConfig)
    policies: PolicyBundleConfig = msgspec.field(default_factory=PolicyBundleConfig)


__all__ = [
    "CatalogConfig",
    "DataSourceConfig",
    "DiagnosticsConfig",
    "ExecutionConfig",
    "FeatureGatesConfig",
    "PolicyBundleConfig",
    "RuntimeProfileConfig",
    "ZeroRowBootstrapConfig",
]
