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
    def test_entity_service_exists(self) -> None:
        assert EntityService is not None

    def test_calls_service_exists(self) -> None:
        assert CallsService is not None

    def test_search_service_exists(self) -> None:
        assert SearchService is not None

    def test_request_types_exist(self) -> None:
        assert EntityFrontDoorRequest is not None
        assert CallsServiceRequest is not None
        assert SearchServiceRequest is not None
