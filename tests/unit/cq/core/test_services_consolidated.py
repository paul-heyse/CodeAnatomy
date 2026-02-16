"""Tests for flattened services module."""
# ruff: noqa: D101, D102

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
    @staticmethod
    def test_entity_service_exists() -> None:
        assert EntityService is not None

    @staticmethod
    def test_calls_service_exists() -> None:
        assert CallsService is not None

    @staticmethod
    def test_search_service_exists() -> None:
        assert SearchService is not None

    @staticmethod
    def test_request_types_exist() -> None:
        assert EntityFrontDoorRequest is not None
        assert CallsServiceRequest is not None
        assert SearchServiceRequest is not None
