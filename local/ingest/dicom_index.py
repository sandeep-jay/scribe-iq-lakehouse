"""DICOM index — map a FHIR StudyInstanceUID to its local Bronze ``.dcm`` file (ADR-013).

Coherent DICOM file names embed the patient name, UUID, and StudyInstanceUID:

    {Given}{n}_{Family}{n}_{patientUUID}{StudyInstanceUID}.dcm

The trailing UID (everything after the 36-char patient UUID) is the join key to a FHIR
``ImagingStudy`` (see :func:`local.transforms.fhir_parser.imaging_study_uid`). Only ~299 of
the ~3,752 imaging studies have a DICOM file, so most lookups miss — that is expected.

PHI note: DICOM file names embed the synthetic patient's name, so they are
identifier-bearing and are never logged raw — this module logs counts only, and redacts
any name it must mention (ADR-010).
"""

from __future__ import annotations

import logging
from pathlib import Path

from local.redaction import redact

logger = logging.getLogger(__name__)

# A standard UUID is 36 chars (8-4-4-4-12 with hyphens); the StudyInstanceUID follows it.
_UUID_LEN = 36
# Synthea StudyInstanceUIDs are DICOM OIDs and begin with the org root "1.".
_UID_PREFIX = "1."


def study_uid_from_filename(name: str) -> str:
    """Extract the StudyInstanceUID from a Coherent DICOM file name (``""`` if unparseable)."""
    stem = name[:-4] if name.lower().endswith(".dcm") else name
    tail = stem.rsplit("_", 1)[-1]  # = patientUUID + StudyInstanceUID
    uid = tail[_UUID_LEN:]
    return uid if uid.startswith(_UID_PREFIX) else ""


class DicomIndex:
    """Index of Bronze DICOM files keyed by StudyInstanceUID.

    Built once per run from ``<bronze>/dicom/*.dcm``; the pipeline passes
    :meth:`read` as the ``dicom_resolver`` to ``FHIRBundleParser.parse_bundle``.
    """

    def __init__(self, dicom_dir: Path | str) -> None:
        """Index every ``*.dcm`` under ``dicom_dir`` by its StudyInstanceUID."""
        self.dicom_dir = Path(dicom_dir)
        self._by_uid: dict[str, Path] = {}
        if self.dicom_dir.is_dir():
            for path in sorted(self.dicom_dir.glob("*.dcm")):
                uid = study_uid_from_filename(path.name)
                if uid:
                    self._by_uid[uid] = path
                else:
                    logger.warning("Unparseable DICOM file name %s", redact(path.name))
        logger.info("DicomIndex: %d studies indexed under %s", len(self._by_uid), self.dicom_dir)

    def __len__(self) -> int:
        """Number of indexed DICOM studies."""
        return len(self._by_uid)

    def __contains__(self, study_uid: str) -> bool:
        """True if a DICOM file is indexed for the given StudyInstanceUID."""
        return study_uid in self._by_uid

    def read(self, study_uid: str) -> bytes | None:
        """Return the raw bytes of a study's DICOM file, or ``None`` if not indexed.

        Reads the whole file; the parser then parses only the header with
        ``stop_before_pixels=True`` (ADR-006) and never materializes pixel data.
        """
        path = self._by_uid.get(study_uid)
        if path is None:
            return None
        try:
            return path.read_bytes()
        except OSError:
            logger.warning("Failed to read an indexed DICOM file")
            return None
