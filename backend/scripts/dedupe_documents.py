"""
scripts/dedupe_documents.py
============================
Find — and optionally retire — duplicate documents already in the library.

Background
----------
Until ``content_hash`` / ``source_key`` were added, the ingestion pipeline
deduped only on ``sha256(file bytes)``.  A file that was re-saved or
re-exported produced different bytes with identical content, so SharePoint
re-synced it and the library ended up with two live copies of the same
report.  Both copies answered questions and both counted toward totals.

This script computes the content fingerprint for every existing document
from the page records already stored in PostgreSQL, groups documents that
are really the same, and shows you the result.

It writes nothing unless you ask it to.

Usage
-----
    # 1. Look only — no database writes at all
    python -m scripts.dedupe_documents

    # 2. Fill in content_hash / source_key on existing rows (safe,
    #    additive — does not retire anything)
    python -m scripts.dedupe_documents --backfill

    # 3. Backfill, then retire the older copy in each duplicate group
    python -m scripts.dedupe_documents --apply

Retiring keeps the ``documents`` row (file name, title, period,
ingested_at, and ``superseded_by`` pointing at the surviving copy) but
deletes its pages, blocks, series, facts, issues and Weaviate vectors.
Every retrieval path reads those derived tables, so a retired document
stops appearing in answers and totals immediately.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.db import close_db, init_db  # noqa: E402
from documents import fingerprint, store  # noqa: E402


async def _load_documents(conn) -> list[dict]:
    """Every live document with its stored identity columns."""
    rows = await conn.fetch(
        """
        SELECT doc_id, file_name, title, as_of_key, as_of_label, pages,
               status, content_hash, source_key, superseded_by, ingested_at
        FROM documents
        WHERE superseded_by = ''
        ORDER BY ingested_at
        """
    )
    return [dict(r) for r in rows]


async def _page_records(conn, doc_id: str) -> list[dict]:
    """Rebuild the page records for a document from the ``pages`` table."""
    rows = await conn.fetch(
        "SELECT record FROM pages WHERE doc_id = $1 ORDER BY page", doc_id
    )
    out = []
    for r in rows:
        try:
            out.append(json.loads(r["record"]))
        except (json.JSONDecodeError, TypeError):
            continue
    return out


async def _sync_paths(conn) -> dict[str, str]:
    """Map doc_id -> SharePoint "folder/name" from the sync tracking table."""
    try:
        rows = await conn.fetch(
            "SELECT doc_id, name, relative_path FROM onedrive_sync_files "
            "WHERE doc_id <> ''"
        )
    except Exception:  # table may not exist in a fresh install
        return {}
    return {
        r["doc_id"]: fingerprint.source_key(r["relative_path"] or "", r["name"] or "")
        for r in rows
    }


def _group(docs: list[dict]) -> list[tuple[str, list[dict]]]:
    """Group documents that are really the same thing.

    Two rules, matching the ingestion-time logic:
      * identical indexed content (same fingerprint)
      * same source file reporting the same period
    """
    by_content: dict[str, list[dict]] = {}
    by_source: dict[tuple[str, str], list[dict]] = {}

    for d in docs:
        if d["_fingerprint"]:
            by_content.setdefault(d["_fingerprint"], []).append(d)
        if d["_source_key"] and d["as_of_key"]:
            by_source.setdefault((d["_source_key"], d["as_of_key"]), []).append(d)

    groups: list[tuple[str, list[dict]]] = []
    seen: set[frozenset[str]] = set()

    for fp, members in by_content.items():
        if len(members) > 1:
            key = frozenset(m["doc_id"] for m in members)
            seen.add(key)
            groups.append((f"identical content (fingerprint {fp})", members))

    for (src, period), members in by_source.items():
        if len(members) > 1:
            key = frozenset(m["doc_id"] for m in members)
            if key in seen:
                continue  # already reported via the content rule
            seen.add(key)
            groups.append((f"same source '{src}' restating {period}", members))

    return groups


async def run(backfill: bool, apply: bool) -> int:
    async with store.acquire() as conn:
        docs = await _load_documents(conn)
        if not docs:
            print("No live documents found.")
            return 0

        sync_paths = await _sync_paths(conn)

        print(f"Scanning {len(docs)} live document(s)...\n")
        for d in docs:
            pages = await _page_records(conn, d["doc_id"])
            d["_fingerprint"] = d["content_hash"] or fingerprint.content_fingerprint(pages)
            d["_source_key"] = (
                d["source_key"]
                or sync_paths.get(d["doc_id"])
                or fingerprint.source_key("", d["file_name"])
            )

        # ── Backfill the identity columns ────────────────────────────────
        if backfill or apply:
            filled = 0
            for d in docs:
                if (d["_fingerprint"] != d["content_hash"]
                        or d["_source_key"] != d["source_key"]):
                    await conn.execute(
                        "UPDATE documents SET content_hash = $1, source_key = $2 "
                        "WHERE doc_id = $3",
                        d["_fingerprint"], d["_source_key"], d["doc_id"],
                    )
                    filled += 1
            print(f"Backfilled content_hash / source_key on {filled} document(s).\n")

        # ── Report ───────────────────────────────────────────────────────
        groups = _group(docs)
        if not groups:
            print("No duplicates found — every live document is unique.")
            return 0

        print(f"Found {len(groups)} duplicate group(s):\n")
        # old_id -> winner_id.  A dict (not a list) because overlapping
        # groups can nominate the same document for retirement twice.
        decisions: dict[str, str] = {}

        for reason, members in groups:
            members.sort(key=lambda m: m["ingested_at"])
            winner = members[-1]  # newest ingest survives
            print(f"── {reason}")
            for m in members:
                mark = "KEEP    " if m is winner else "RETIRE  "
                print(
                    f"   {mark} {m['doc_id']}  {str(m['ingested_at'])[:19]}  "
                    f"{m['as_of_label'] or '-':>8}  {m['pages']:>3}p  {m['file_name']}"
                )
                if m is not winner:
                    decisions.setdefault(m["doc_id"], winner["doc_id"])
            print()

        # Overlapping groups can leave a winner that another group retired.
        # Follow the chain so every retired document points at a survivor.
        def survivor(doc_id: str, _depth: int = 0) -> str:
            nxt = decisions.get(doc_id)
            if nxt is None or nxt == doc_id or _depth > 10:
                return doc_id
            return survivor(nxt, _depth + 1)

        to_retire = [(old, survivor(win)) for old, win in decisions.items()]
        to_retire = [(old, win) for old, win in to_retire if old != win]

        if not apply:
            print(f"{len(to_retire)} document(s) would be retired.")
            print("Nothing was changed. Re-run with --apply to retire them.")
            return 0

        # ── Apply ────────────────────────────────────────────────────────
        print(f"Retiring {len(to_retire)} document(s)...\n")
        for old_id, winner_id in to_retire:
            await store.delete_document(conn, old_id, keep_row=True)
            await conn.execute(
                "UPDATE documents SET status = 'superseded', progress = '', "
                "superseded_by = $1 WHERE doc_id = $2",
                winner_id, old_id,
            )
            print(f"   retired {old_id} -> superseded by {winner_id}")

        print(f"\nDone. {len(to_retire)} document(s) retired.")
        return 0


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Find and optionally retire duplicate documents."
    )
    ap.add_argument(
        "--backfill", action="store_true",
        help="write content_hash / source_key onto existing rows (no retiring)",
    )
    ap.add_argument(
        "--apply", action="store_true",
        help="backfill, then retire the older copy in each duplicate group",
    )
    args = ap.parse_args()

    async def _run():
        await init_db()
        try:
            return await run(args.backfill, args.apply)
        finally:
            await close_db()

    sys.exit(asyncio.run(_run()))


if __name__ == "__main__":
    main()
