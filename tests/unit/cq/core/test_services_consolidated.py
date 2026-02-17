"""Tests for flattened services module."""

from __future__ import annotations

from tools.cq.core.services import (
    CallsService,
    CallsServiceRequest,
    EntityFrontDoorRequest,
    EntityService,
    SearchService,
    SearchServiceRequest,
)


class TestServiceImports:
    """Tests for flattened service exports."""

    @staticmethod
    def test_entity_service_exists() -> None:
        """Export EntityService from consolidated services module."""
        assert EntityService is not None

    @staticmethod
    def test_calls_service_exists() -> None:
        """Export CallsService from consolidated services module."""
        assert CallsService is not None

    @staticmethod
    def test_search_service_exists() -> None:
        """Export SearchService from consolidated services module."""
        assert SearchService is not None

    @staticmethod
    def test_request_types_exist() -> None:
        """Export request contracts from consolidated services module."""
        assert EntityFrontDoorRequest is not None
        assert CallsServiceRequest is not None
        assert SearchServiceRequest is not None
