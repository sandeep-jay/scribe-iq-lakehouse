"""Shared pytest fixtures for the lakehouse test suite."""

import json
from pathlib import Path

import pytest

from local.transforms.fhir_parser import FHIRBundleParser

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "sample_bundle.json"


@pytest.fixture(scope="session")
def sample_bundle() -> dict:
    """The synthetic FHIR bundle used across the suite (never real patient data)."""
    with FIXTURE_PATH.open() as f:
        return json.load(f)


@pytest.fixture()
def parser() -> FHIRBundleParser:
    """A fresh parser instance per test."""
    return FHIRBundleParser()


@pytest.fixture()
def parsed(parser: FHIRBundleParser, sample_bundle: dict) -> dict:
    """The fully parsed sample bundle: ``{logical_table: [records]}``."""
    return parser.parse_bundle(sample_bundle)
