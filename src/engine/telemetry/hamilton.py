"""Hamilton telemetry and tracker configuration."""

from __future__ import annotations

from dataclasses import dataclass

from utils.env_utils import env_bool, env_int, env_value


@dataclass(frozen=True)
class HamiltonTrackerConfig:
    """Tracker configuration sourced from runtime profile or environment."""

    project_id: int | None = None
    username: str | None = None
    dag_name: str | None = None
    api_url: str | None = None
    ui_url: str | None = None

    @property
    def enabled(self) -> bool:
        """Return True when tracker configuration is complete."""
        return self.project_id is not None and self.username is not None

    @classmethod
    def from_env(cls) -> HamiltonTrackerConfig | None:
        """Build tracker config from environment variables.

        Returns
        -------
        HamiltonTrackerConfig | None
            Tracker configuration when environment provides values.
        """
        project_id = env_int("CODEANATOMY_HAMILTON_PROJECT_ID")
        if project_id is None:
            project_id = env_int("HAMILTON_PROJECT_ID")
        username = env_value("CODEANATOMY_HAMILTON_USERNAME")
        if username is None:
            username = env_value("HAMILTON_USERNAME")
        dag_name = env_value("CODEANATOMY_HAMILTON_DAG_NAME") or env_value("HAMILTON_DAG_NAME")
        api_url = env_value("CODEANATOMY_HAMILTON_API_URL") or env_value("HAMILTON_API_URL")
        ui_url = env_value("CODEANATOMY_HAMILTON_UI_URL") or env_value("HAMILTON_UI_URL")
        if (
            project_id is None
            and username is None
            and dag_name is None
            and api_url is None
            and ui_url is None
        ):
            return None
        return cls(
            project_id=project_id,
            username=username,
            dag_name=dag_name,
            api_url=api_url,
            ui_url=ui_url,
        )


@dataclass(frozen=True)
class HamiltonTelemetryProfile:
    """Telemetry profile for Hamilton tracker capture controls."""

    name: str
    enable_tracker: bool
    capture_data_statistics: bool
    max_list_length_capture: int
    max_dict_length_capture: int

    @classmethod
    def resolve(cls) -> HamiltonTelemetryProfile:
        """Resolve telemetry profile from environment.

        Returns
        -------
        HamiltonTelemetryProfile
            Resolved telemetry profile with overrides applied.
        """
        profile_name = _resolve_hamilton_telemetry_profile_name()
        enable_tracker, capture_stats, max_list, max_dict = _resolve_hamilton_profile_defaults(
            profile_name
        )
        overrides = _resolve_hamilton_telemetry_overrides()
        enable_tracker = (
            overrides.enable_tracker if overrides.enable_tracker is not None else enable_tracker
        )
        capture_stats = (
            overrides.capture_stats if overrides.capture_stats is not None else capture_stats
        )
        max_list = overrides.max_list if overrides.max_list is not None else max_list
        max_dict = overrides.max_dict if overrides.max_dict is not None else max_dict
        return cls(
            name=profile_name,
            enable_tracker=enable_tracker,
            capture_data_statistics=capture_stats,
            max_list_length_capture=max_list,
            max_dict_length_capture=max_dict,
        )


@dataclass(frozen=True)
class _HamiltonTelemetryOverrides:
    enable_tracker: bool | None
    capture_stats: bool | None
    max_list: int | None
    max_dict: int | None


def _resolve_hamilton_telemetry_profile_name() -> str:
    profile_name = (
        env_value("CODEANATOMY_HAMILTON_TELEMETRY_PROFILE")
        or env_value("HAMILTON_TELEMETRY_PROFILE")
        or env_value("CODEANATOMY_ENV")
        or "dev"
    )
    return profile_name.strip().lower()


def _resolve_hamilton_profile_defaults(
    profile_name: str,
) -> tuple[bool, bool, int, int]:
    if profile_name in {"prod", "production"}:
        return True, False, 20, 50
    if profile_name in {"ci", "test"}:
        return False, False, 5, 10
    return True, True, 200, 200


def _resolve_hamilton_telemetry_overrides() -> _HamiltonTelemetryOverrides:
    tracker_override = env_bool(
        "CODEANATOMY_HAMILTON_TRACKER_ENABLED",
        default=None,
        on_invalid="none",
    )
    if tracker_override is None:
        tracker_override = env_bool(
            "HAMILTON_TRACKER_ENABLED",
            default=None,
            on_invalid="none",
        )
    capture_override = env_bool(
        "CODEANATOMY_HAMILTON_CAPTURE_DATA_STATISTICS",
        default=None,
        on_invalid="none",
    )
    if capture_override is None:
        capture_override = env_bool(
            "HAMILTON_CAPTURE_DATA_STATISTICS",
            default=None,
            on_invalid="none",
        )
    max_list_override = env_int("CODEANATOMY_HAMILTON_MAX_LIST_LENGTH_CAPTURE", default=None)
    if max_list_override is None:
        max_list_override = env_int("HAMILTON_MAX_LIST_LENGTH_CAPTURE", default=None)
    max_dict_override = env_int("CODEANATOMY_HAMILTON_MAX_DICT_LENGTH_CAPTURE", default=None)
    if max_dict_override is None:
        max_dict_override = env_int("HAMILTON_MAX_DICT_LENGTH_CAPTURE", default=None)
    return _HamiltonTelemetryOverrides(
        enable_tracker=tracker_override,
        capture_stats=capture_override,
        max_list=max_list_override,
        max_dict=max_dict_override,
    )


__all__ = ["HamiltonTelemetryProfile", "HamiltonTrackerConfig"]
