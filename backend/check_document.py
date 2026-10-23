"""
check_document.py — Dump everything the pipeline stored for one ingested
file, as JSON, so you can see exactly what ended up in PostgreSQL.

Looks up the file by name (partial match against `documents.file_name`),
then pulls:
    - documents   row (doc_id, title, doc_type, as_of, status, ...)
    - pages       rows (per-page heading/text/image/raw record)
    - blocks      rows (narrative/table/chart passages per page)
    - series      rows + their facts (metric series -> period values)
    - issues      rows (validation flags found during ingestion)

Weaviate vectors are NOT dumped here (they're just embeddings of the
same block/series text, not separate "content") — this script shows the
PostgreSQL row data, which is the actual structured content.

Usage:
    python check_document.py "09_Net_Advances_Insights"
    python check_document.py "Retail Assets - Collections Efficiency.pdf"
    python check_document.py "09_Net_Advances_Insights" --out dump.json
    python check_document.py "09_Net_Advances_Insights" --doc-id 2afe6e64d1e44f6c
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

import asyncpg

sys.path.insert(0, str(Path(__file__).resolve().parent))

from core.config import get_settings  # noqa: E402


def _row_to_dict(row: asyncpg.Record) -> dict:
    """Convert an asyncpg Record to a plain JSON-serializable dict."""
    out = {}
    for key, value in dict(row).items():
        if hasattr(value, "isoformat"):  # datetime / date
            out[key] = value.isoformat()
        else:
            out[key] = value
    return out


async def find_document(conn: asyncpg.Connection, name_fragment: str) -> list[dict]:
    """Find document rows whose file_name contains the given fragment (case-insensitive)."""
    rows = await conn.fetch(
        """
        SELECT doc_id, file_name, title, doc_type, entity, as_of_key, as_of_label,
               pages, status, progress, ingested_at
        FROM documents
        WHERE file_name ILIKE $1
        ORDER BY ingested_at DESC
        """,
        f"%{name_fragment}%",
    )
    return [_row_to_dict(r) for r in rows]


async def dump_document(conn: asyncpg.Connection, doc_id: str) -> dict:
    """Pull every stored row for a single doc_id, shaped as one JSON object."""
    doc_row = await conn.fetchrow("SELECT * FROM documents WHERE doc_id = $1", doc_id)
    document = _row_to_dict(doc_row) if doc_row else None

    pages = [
        _row_to_dict(r)
        for r in await conn.fetch(
            "SELECT doc_id, page, heading, text, image, record "
            "FROM pages WHERE doc_id = $1 ORDER BY page",
            doc_id,
        )
    ]
    # `record` is itself a JSON string (the raw per-page ingest record) —
    # parse it so the output is readable JSON instead of an escaped string.
    for p in pages:
        if p.get("record"):
            try:
                p["record"] = json.loads(p["record"])
            except (json.JSONDecodeError, TypeError):
                pass  # leave as raw string if it isn't valid JSON

    blocks = [
        _row_to_dict(r)
        for r in await conn.fetch(
            "SELECT block_id, doc_id, page, kind, section, title, text, content, "
            "       bbox, source, confidence, section_path, anchor_block_id "
            "FROM blocks WHERE doc_id = $1 ORDER BY page, block_id",
            doc_id,
        )
    ]

    series_rows = await conn.fetch(
        "SELECT series_id, doc_id, page, block_id, label, name, metric_id, "
        "       kind, unit, entity, source, confidence, path, scale "
        "FROM series WHERE doc_id = $1 ORDER BY page, series_id",
        doc_id,
    )
    series = []

    issues = [
        _row_to_dict(r)
        for r in await conn.fetch(
            "SELECT issue_id, doc_id, page, kind, ref, message, suggestion "
            "FROM issues WHERE doc_id = $1 ORDER BY page",
            doc_id,
        )
    ]

    return {
        "document": document,
        "pages": pages,
        "blocks": blocks,
        "series": series,
        "issues": issues,
        "counts": {
            "pages": len(pages),
            "blocks": len(blocks),
            "series": len(series),
            "facts": sum(len(s["facts"]) for s in series),
            "issues": len(issues),
        },
    }


async def main():
    parser = argparse.ArgumentParser(
        description="Dump the stored content of one ingested document as JSON."
    )
    parser.add_argument("name", nargs="?", help="File name (or fragment) to search for")
    parser.add_argument("--doc-id", help="Look up by exact doc_id instead of file name")
    parser.add_argument("--out", help="Write JSON to this file instead of stdout")
    args = parser.parse_args()

    if not args.name and not args.doc_id:
        parser.error("Provide a file name fragment or --doc-id")

    settings = get_settings()
    dsn = (settings.postgres_dsn or "").strip()
    if not dsn:
        print("ERROR: postgres_dsn could not be resolved (check POSTGRES_HOST in .env "
              "and AWS Secrets Manager access)")
        sys.exit(1)

    conn = await asyncpg.connect(dsn)
    try:
        if args.doc_id:
            result = await dump_document(conn, args.doc_id)
            if result["document"] is None:
                print(f"No document found with doc_id={args.doc_id}")
                return
            matches = [result["document"]]
        else:
            matches = await find_document(conn, args.name)
            if not matches:
                print(f"No document found matching '{args.name}'")
                return
            if len(matches) > 1:
                print(f"{len(matches)} documents match '{args.name}':\n")
                for i, m in enumerate(matches, 1):
                    print(f"  {i}. {m['file_name']}  (doc_id={m['doc_id']}, "
                          f"ingested_at={m['ingested_at']})")
                print("\nRe-run with --doc-id <id> to pick one, or narrow your search.")
                return
            result = await dump_document(conn, matches[0]["doc_id"])

        output = json.dumps(result, indent=2, ensure_ascii=False, default=str)

        if args.out:
            Path(args.out).write_text(output, encoding="utf-8")
            print(f"Wrote {len(output):,} bytes to {args.out}")
            print(f"Counts: {result['counts']}")
        else:
            print(output)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
