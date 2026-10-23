"""
Read-only diagnostic: show how a document's series/facts were stored.

Usage (from the backend folder, with the venv active):
    python scripts/inspect_doc_series.py
    python scripts/inspect_doc_series.py --doc-id 80072a5aee09e5f9 --term "grand total" --limit 15

Only runs SELECT queries. Nothing is modified.
"""
import argparse
import asyncio
import sys
from pathlib import Path

import asyncpg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from core.config import get_settings  # noqa: E402


def cut(value, n=300):
    s = "" if value is None else str(value).replace("\n", " ")
    return s if len(s) <= n else s[:n] + f"... [+{len(s) - n} chars]"


async def main(doc_id: str, term: str, limit: int) -> None:
    dsn = get_settings().postgres_dsn
    conn = await asyncpg.connect(dsn=dsn, command_timeout=60)
    try:
        print("=" * 80)
        print("1) DOCUMENT")
        d = await conn.fetchrow(
            "SELECT doc_id, file_name, title, doc_type, as_of_key, as_of_label, "
            "pages, status, progress, source_key, superseded_by "
            "FROM documents WHERE doc_id = $1", doc_id)
        if not d:
            print(f"  No document with doc_id={doc_id}")
            return
        for k, v in d.items():
            print(f"  {k}: {cut(v)}")

        print("=" * 80)
        print("2) ROW COUNTS")
        for t in ("pages", "blocks", "series", "facts", "issues"):
            n = await conn.fetchval(f"SELECT count(*) FROM {t} WHERE doc_id = $1", doc_id)
            print(f"  {t}: {n}")

        print("=" * 80)
        print(f"3) SERIES matching '{term}' (label/name/entity/path)")
        like = f"%{term}%"
        rows = await conn.fetch(
            """
            SELECT s.series_id, s.page, s.label, s.name, s.entity, s.unit, s.kind,
                   s.metric_id, s.path,
                   (SELECT count(*) FROM facts f WHERE f.series_id = s.series_id) AS n_facts
            FROM series s
            WHERE s.doc_id = $1
              AND (s.label ILIKE $2 OR s.name ILIKE $2 OR s.entity ILIKE $2 OR s.path ILIKE $2)
            ORDER BY s.series_id
            LIMIT $3
            """, doc_id, like, limit)
        print(f"  {len(rows)} row(s) shown (limit {limit})")
        for r in rows:
            print("  " + "-" * 76)
            print(f"  series_id={r['series_id']} page={r['page']} unit={r['unit']!r} "
                  f"kind={r['kind']!r} n_facts={r['n_facts']}")
            print(f"  label : {cut(r['label'])}")
            print(f"  name  : {cut(r['name'])}")
            print(f"  entity: {cut(r['entity'])}")
            print(f"  path  : {cut(r['path'], 600)}")

        print("=" * 80)
        print(f"4) FACTS for the first matching series (up to {limit})")
        if rows:
            facts = await conn.fetch(
                "SELECT fact_id, period_key, period_label, value, as_printed, validated, note "
                "FROM facts WHERE series_id = $1 ORDER BY fact_id LIMIT $2",
                rows[0]["series_id"], limit)
            for f in facts:
                print(f"  fact_id={f['fact_id']} period={f['period_label']!r} "
                      f"value={f['value']} printed={f['as_printed']!r} "
                      f"validated={f['validated']} note={cut(f['note'], 80)!r}")

        print("=" * 80)
        print("5) SERIES mentioning 'insignia' (first few)")
        ins = await conn.fetch(
            """
            SELECT s.series_id, s.page, s.label, s.name, s.entity, s.path,
                   (SELECT count(*) FROM facts f WHERE f.series_id = s.series_id) AS n_facts
            FROM series s
            WHERE s.doc_id = $1
              AND (s.label ILIKE '%insignia%' OR s.name ILIKE '%insignia%'
                   OR s.entity ILIKE '%insignia%' OR s.path ILIKE '%insignia%')
            ORDER BY s.series_id LIMIT 8
            """, doc_id)
        print(f"  {len(ins)} row(s)")
        for r in ins:
            print(f"  series_id={r['series_id']} page={r['page']} n_facts={r['n_facts']}")
            print(f"    name  : {cut(r['name'], 200)}")
            print(f"    entity: {cut(r['entity'], 200)}")
            print(f"    path  : {cut(r['path'], 400)}")

        print("=" * 80)
        print("6) LONGEST LABELS (shows whether headers were flattened into one string)")
        longest = await conn.fetch(
            "SELECT series_id, length(label) AS n, left(label, 200) AS head "
            "FROM series WHERE doc_id = $1 ORDER BY length(label) DESC LIMIT 5", doc_id)
        for r in longest:
            print(f"  series_id={r['series_id']} len={r['n']}: {cut(r['head'], 200)}")
    finally:
        await conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--doc-id", default="80072a5aee09e5f9")
    ap.add_argument("--term", default="grand total")
    ap.add_argument("--limit", type=int, default=10)
    args = ap.parse_args()
    asyncio.run(main(args.doc_id, args.term, args.limit))
