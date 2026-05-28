"""PHI-safe logging helpers.

Synthea Coherent is synthetic, but this is a production-pattern lakehouse: in a real
EHR export, file names and references routinely carry patient identifiers (names,
MRNs, UUIDs). Logging them would leak PHI. Per the project security rules
(CLAUDE.md, healthcare-data skill: "never log patient_id, encounter_id, or any PHI"),
emit a stable, non-reversible reference instead of the raw value.

``redact`` is used wherever a log line would otherwise contain an identifier-bearing
token (e.g. a Synthea bundle filename like ``Abe604_Frami345_<uuid>.json``), so the
same file is traceable across log lines without exposing who it belongs to.
"""

from __future__ import annotations

import hashlib


def redact(value: str | None) -> str:
    """Return a short, non-reversible reference for an identifier-bearing string.

    Args:
        value: A value that may contain an identifier (filename, reference, id).

    Returns:
        ``"ref:none"`` for empty input, otherwise ``"ref:<10-hex>"`` — a truncated
        SHA-256 digest. Stable for a given input, so the same file maps to the same
        reference across log lines, but the original value cannot be recovered.
    """
    if not value:
        return "ref:none"
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:10]
    return f"ref:{digest}"
