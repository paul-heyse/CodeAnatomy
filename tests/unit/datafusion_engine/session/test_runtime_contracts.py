# ruff: noqa: D100, D103
from __future__ import annotations

from datafusion_engine.session.contracts import (
    IdentifierNormalizationMode,
    TelemetryEnrichmentPolicy,
)
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from datafusion_engine.session.runtime_compile import effective_ident_normalization
from datafusion_engine.session.runtime_profile_config import FeatureGatesConfig


def test_identifier_normalization_mode_values_are_stable() -> None:
    assert IdentifierNormalizationMode.RAW.value == "raw"
    assert IdentifierNormalizationMode.SQL_SAFE.value == "sql_safe"
    assert IdentifierNormalizationMode.STRICT.value == "strict"


def test_effective_ident_normalization_respects_mode_and_delta_defaults() -> None:
    strict_profile = DataFusionRuntimeProfile(
        features=FeatureGatesConfig(
            identifier_normalization_mode=IdentifierNormalizationMode.STRICT,
            enable_delta_session_defaults=True,
        )
    )
    sql_safe_profile = DataFusionRuntimeProfile(
        features=FeatureGatesConfig(
            identifier_normalization_mode=IdentifierNormalizationMode.SQL_SAFE,
            enable_delta_session_defaults=True,
        )
    )

    assert effective_ident_normalization(strict_profile) is True
    assert effective_ident_normalization(sql_safe_profile) is False


def test_telemetry_contract_defaults() -> None:
    policy = TelemetryEnrichmentPolicy()

    assert policy.include_query_text is False
    assert policy.include_plan_hash is True
    assert policy.include_profile_name is True


def test_telemetry_payload_includes_identifier_mode() -> None:
    profile = DataFusionRuntimeProfile(
        features=FeatureGatesConfig(
            identifier_normalization_mode=IdentifierNormalizationMode.SQL_SAFE,
        )
    )

    payload = profile.telemetry_payload_v1()
    sql_surfaces = payload["sql_surfaces"]

    assert isinstance(sql_surfaces, dict)
    assert sql_surfaces["identifier_normalization_mode"] == "sql_safe"
