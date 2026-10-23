"""
documents/fingerprint.py
=========================
Content fingerprinting for duplicate detection.

Why this exists
---------------
``library.add_pdf()`` derives ``doc_id`` from ``sha256(file bytes)``, which
dedupes only *byte-identical* files.  That is not enough in practice:

- Re-saving a PPTX in PowerPoint rewrites internal timestamps, the revision
  counter and the zip entry order.  Same slides, different bytes.
- Re-exporting a PDF writes a fresh ``/CreationDate``, ``/ModDate`` and
  trailer ``/ID``.  Same page, different bytes.

Either one is enough for SharePoint to bump ``lastModifiedDateTime``, which
makes the sync re-download the file, which produces a *new* ``doc_id`` and a
second copy of the same report in the library.

``content_fingerprint()`` hashes what the pipeline actually indexes — page
text and table cell values — so the fingerprint only changes when a number
or a word on the page changes.

Only pages that feed retrieval are hashed.  Pages flagged
``exclude_from_index`` (cover pages, reconstruction notes) are skipped, so a
re-export that drops a notes page still matches the earlier version.
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

# Collapse all runs of whitespace so re-flowed text doesn't change the hash.
_WS = re.compile(r"\s+")


def _norm(value) -> str:
    """Normalise one piece of extracted text for hashing."""
    if value is None:
        return ""
    return _WS.sub(" ", str(value)).strip()


def content_fingerprint(pages: list[dict]) -> str:
    """Return a stable 16-char fingerprint of a document's indexed content.

    Parameters
    ----------
    pages : list[dict]
        Page records as produced by ``ingest.ingest()`` — each with
        ``text`` and optional ``tables`` (``header`` + ``rows``).

    Returns
    -------
    str
        16 hex chars, or ``""`` when there is no indexable content (in which
        case the caller should not attempt content-based deduplication).
    """
    h = hashlib.sha256()
    saw_content = False

    for rec in pages:
        if rec.get("exclude_from_index"):
            continue

        text = _norm(rec.get("text"))
        if text:
            saw_content = True
            h.update(b"T\x00")
            h.update(text.encode("utf-8", "replace"))
            h.update(b"\x00")

        # Table values carry the numbers the business actually asks about,
        # so they are hashed explicitly rather than relying on page text.
        for table in rec.get("tables") or []:
            cells = list(table.get("header") or [])
            for row in table.get("rows") or []:
                cells.extend(row)
            flat = " | ".join(_norm(c) for c in cells).strip(" |")
            if flat:
                saw_content = True
                h.update(b"B\x00")
                h.update(flat.encode("utf-8", "replace"))
                h.update(b"\x00")

    if not saw_content:
        return ""
    return h.hexdigest()[:16]

