"""Tests for local.redaction — PHI-safe log references."""

from local.redaction import redact


def test_redact_is_stable_and_prefixed():
    a = redact("Abe604_Frami345_b8dd1798-beef.json")
    assert a == redact("Abe604_Frami345_b8dd1798-beef.json")  # deterministic
    assert a.startswith("ref:")


def test_redact_does_not_leak_original():
    name = "Abe604_Frami345_b8dd1798-beef.json"
    out = redact(name)
    assert "Abe604" not in out
    assert "b8dd1798" not in out
    assert len(out) == len("ref:") + 10


def test_redact_distinguishes_inputs():
    assert redact("patient_a.json") != redact("patient_b.json")


def test_redact_empty():
    assert redact("") == "ref:none"
    assert redact(None) == "ref:none"
