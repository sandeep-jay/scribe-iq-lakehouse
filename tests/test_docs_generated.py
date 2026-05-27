"""Doc-as-test: generated docs must stay in sync with the code (ADR-011).

If a Silver schema or validation rule changes, DATA_DICTIONARY.md must be regenerated
(`python scripts/gen_data_dictionary.py`) or this test fails — so the dictionary can
never silently drift from the registry.
"""

from local.transforms.registry import SILVER_TABLES
from scripts.gen_data_dictionary import OUTPUT_PATH, render


def test_data_dictionary_is_current():
    assert OUTPUT_PATH.exists(), "DATA_DICTIONARY.md missing — run scripts/gen_data_dictionary.py"
    assert (
        OUTPUT_PATH.read_text() == render()
    ), "DATA_DICTIONARY.md is stale — run: python scripts/gen_data_dictionary.py"


def test_dictionary_covers_every_silver_table():
    content = render()
    for name in SILVER_TABLES:
        assert f"silver.{name}" in content, f"{name} missing from data dictionary"
    assert "silver.ingest_log" in content


def test_dictionary_documents_cdc_and_provenance():
    content = render()
    assert "enableChangeDataFeed" in content
    assert "source_file" in content and "ingest_timestamp" in content
