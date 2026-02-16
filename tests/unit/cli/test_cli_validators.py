"""Tests for CLI validators."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from cli.validators import ConditionalDisabled, ConditionalRequired


class TestConditionalRequired:
    """Test ConditionalRequired validator."""

    @staticmethod
    def test_raises_when_condition_met_and_missing() -> None:
        """Should raise ValueError when condition met but required params missing."""
        validator = ConditionalRequired(
            condition_param="enable_feature",
            condition_value=True,
            required_params=("feature_path", "feature_name"),
        )
        mock_group = MagicMock()

        with pytest.raises(ValueError, match="feature_path"):
            validator(mock_group, {"enable_feature": True})

    @staticmethod
    def test_passes_when_condition_met_and_present() -> None:
        """Should pass when condition met and required params present."""
        validator = ConditionalRequired(
            condition_param="enable_feature",
            condition_value=True,
            required_params=("feature_path",),
        )
        mock_group = MagicMock()

        # Should not raise
        validator(
            mock_group,
            {"enable_feature": True, "feature_path": "/tmp/path"},
        )

    @staticmethod
    def test_passes_when_condition_not_met() -> None:
        """Should pass when condition is not met."""
        validator = ConditionalRequired(
            condition_param="enable_feature",
            condition_value=True,
            required_params=("feature_path",),
        )
        mock_group = MagicMock()

        # Should not raise - condition not met
        validator(mock_group, {"enable_feature": False})

    @staticmethod
    def test_passes_when_condition_param_missing() -> None:
        """Should pass when condition param is missing."""
        validator = ConditionalRequired(
            condition_param="enable_feature",
            condition_value=True,
            required_params=("feature_path",),
        )
        mock_group = MagicMock()

        # Should not raise - condition param missing
        validator(mock_group, {})


class TestConditionalDisabled:
    """Test ConditionalDisabled validator."""

    @staticmethod
    def test_warns_when_disabled_params_set(caplog: pytest.LogCaptureFixture) -> None:
        """Should warn when disabled params are set."""
        validator = ConditionalDisabled(
            condition_param="disable_feature",
            condition_value=True,
            disabled_params=("feature_path", "feature_name"),
        )
        mock_group = MagicMock()

        with caplog.at_level(logging.WARNING):
            validator(
                mock_group,
                {
                    "disable_feature": True,
                    "feature_path": "/tmp/path",
                },
            )

        assert "feature_path" in caplog.text
        assert "ignored" in caplog.text.lower()

    @staticmethod
    def test_no_warning_when_disabled_params_not_set(
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Should not warn when disabled params are not set."""
        validator = ConditionalDisabled(
            condition_param="disable_feature",
            condition_value=True,
            disabled_params=("feature_path",),
        )
        mock_group = MagicMock()

        with caplog.at_level(logging.WARNING):
            validator(mock_group, {"disable_feature": True})

        assert "feature_path" not in caplog.text

    @staticmethod
    def test_no_warning_when_condition_not_met(
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Should not warn when condition is not met."""
        validator = ConditionalDisabled(
            condition_param="disable_feature",
            condition_value=True,
            disabled_params=("feature_path",),
        )
        mock_group = MagicMock()

        with caplog.at_level(logging.WARNING):
            validator(
                mock_group,
                {
                    "disable_feature": False,
                    "feature_path": "/tmp/path",
                },
            )

        assert "ignored" not in caplog.text.lower()
