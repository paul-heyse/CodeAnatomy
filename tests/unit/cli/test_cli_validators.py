"""Tests for CLI validators."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from cli.validators import ConditionalDisabled, ConditionalRequired


class TestConditionalRequired:
    """Test ConditionalRequired validator."""

    def test_raises_when_condition_met_and_missing(self) -> None:
        """Should raise ValueError when condition met but required params missing."""
        validator = ConditionalRequired(
            condition_param="enable_feature",
            condition_value=True,
            required_params=("feature_path", "feature_name"),
        )
        mock_group = MagicMock()

        with pytest.raises(ValueError, match="feature_path"):
            validator(mock_group, {"enable_feature": True})

    def test_passes_when_condition_met_and_present(self) -> None:
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

    def test_passes_when_condition_not_met(self) -> None:
        """Should pass when condition is not met."""
        validator = ConditionalRequired(
            condition_param="enable_feature",
            condition_value=True,
            required_params=("feature_path",),
        )
        mock_group = MagicMock()

        # Should not raise - condition not met
        validator(mock_group, {"enable_feature": False})

    def test_passes_when_condition_param_missing(self) -> None:
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

    def test_warns_when_disabled_params_set(self, caplog: pytest.LogCaptureFixture) -> None:
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

    def test_no_warning_when_disabled_params_not_set(
        self,
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

    def test_no_warning_when_condition_not_met(
        self,
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
