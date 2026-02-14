"""Session configuration presets and policy constants.

Re-exports from session/runtime.py to provide a focused import path for
consumers that only need configuration presets, policy constants, and
prepared-statement specifications.

Canonical definitions remain in runtime.py to avoid circular imports
(the preset instances reference types and helpers defined in runtime.py).
"""

from __future__ import annotations

# Re-exported for convenience; the canonical name is
# ``datafusion_engine.session.runtime.CST_AUTOLOAD_DF_POLICY``.
from datafusion_engine.session.runtime import (
    CACHE_PROFILES,
    CST_AUTOLOAD_DF_POLICY,
    CST_DIAGNOSTIC_STATEMENTS,
    DATAFUSION_POLICY_PRESETS,
    DEFAULT_DF_POLICY,
    DEV_DF_POLICY,
    GIB,
    INFO_SCHEMA_STATEMENT_NAMES,
    INFO_SCHEMA_STATEMENTS,
    KIB,
    MIB,
    PROD_DF_POLICY,
    SCHEMA_HARDENING_PRESETS,
    SYMTABLE_DF_POLICY,
    DataFusionConfigPolicy,
    PreparedStatementSpec,
    SchemaHardeningProfile,
)

__all__ = [
    "CACHE_PROFILES",
    "CST_AUTOLOAD_DF_POLICY",
    "CST_DIAGNOSTIC_STATEMENTS",
    "DATAFUSION_POLICY_PRESETS",
    "DEFAULT_DF_POLICY",
    "DEV_DF_POLICY",
    "GIB",
    "INFO_SCHEMA_STATEMENTS",
    "INFO_SCHEMA_STATEMENT_NAMES",
    "KIB",
    "MIB",
    "PROD_DF_POLICY",
    "SCHEMA_HARDENING_PRESETS",
    "SYMTABLE_DF_POLICY",
    "DataFusionConfigPolicy",
    "PreparedStatementSpec",
    "SchemaHardeningProfile",
]
