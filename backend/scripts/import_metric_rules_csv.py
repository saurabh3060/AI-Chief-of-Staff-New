"""
scripts/import_metric_rules_csv.py
==================================
Load ``metric_rules.csv`` (exported from the old SQLite library.db) into the
PostgreSQL ``metric_rules`` table used by the documents bot.

* Connects directly to ``--database`` (default ``rbl_ceo_new``) using the app's
  DSN (``core.config`` settings) with only the database name swapped, or ``--dsn``.
  Refuses to write to ``rbl_ceo``.
* UPSERT by ``rule_id`` (INSERT ... ON CONFLICT DO UPDATE). Nothing is deleted or
  truncated, so it is safe to re-run.
* ``version`` and ``updated_at`` from the CSV are preserved as-is.
* Shows which database it is connected to and asks for confirmation (``--yes`` skips).

Usage (from ``backend/``)::

    python -m scripts.import_metric_rules_csv --dry-run
    python -m scripts.import_metric_rules_csv
    python -m scripts.import_metric_rules_csv "D:\\other\\metric_rules.csv" --yes

Naive timestamps in the CSV (e.g. ``2026-10-08 12:29:26``) are interpreted in the
timezone given by ``--tz`` (default ``UTC``).
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from zoneinfo import ZoneInfo

PROTECTED_DB = "rbl_ceo"  # source database: never written to by this script
DEFAULT_CSV = r"C:\Users\C59248\Desktop\libraryNewGithun\doctr-model-test\export\export\metric_rules.csv"

# Column order = INSERT order. Must match documents/db_schema.py `metric_rules`.
COLUMNS = [
    "rule_id", "source_name", "source_page", "source_heading", "document_pattern",
    "metric_family", "metric_name", "definition", "applies_to", "example",
    "operation", "compiled_spec", "aliases", "routing_spec", "interpretation_spec",
    "output_type", "enabled", "status", "review_reason", "version", "updated_at",
]
TEXT_DEFAULTS = {  # NOT NULL DEFAULT values used when the CSV cell is empty
    "compiled_spec": "{}", "aliases": "[]", "routing_spec": "{}",
    "interpretation_spec": "{}", "status": "draft",
}
JSON_COLUMNS = ("compiled_spec", "aliases", "routing_spec", "interpretation_spec")
INT_COLUMNS_NOT_NULL = {"enabled": 1, "version": 1}


def parse_row(raw: dict, tz: ZoneInfo, line: int, warnings: list[str]) -> tuple:
    out: dict = {}
    for col in COLUMNS:
        val = raw.get(col)
        val = "" if val is None else val
        if col == "source_page":
            out[col] = int(val) if val.strip() else None
        elif col in INT_COLUMNS_NOT_NULL:
            out[col] = int(val) if val.strip() else INT_COLUMNS_NOT_NULL[col]
        elif col == "updated_at":
            out[col] = (datetime.fromisoformat(val.strip()).replace(tzinfo=tz)
                        if val.strip() else datetime.now(tz))
        else:
            out[col] = val if val != "" else TEXT_DEFAULTS.get(col, "")
    for col in JSON_COLUMNS:  # stored as TEXT, but flag anything that is not valid JSON
        try:
            json.loads(out[col])
        except json.JSONDecodeError:
            warnings.append(f"line {line} rule_id={out['rule_id']!r}: {col} is not valid JSON")
    if not out["rule_id"].strip():
        raise ValueError(f"line {line}: empty rule_id")
    return tuple(out[c] for c in COLUMNS)


def load_csv(path: Path, tz: ZoneInfo) -> tuple[list[tuple], list[str]]:
    warnings: list[str] = []
    rows: list[tuple] = []
    seen: dict[str, int] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        missing = [c for c in COLUMNS if c not in (reader.fieldnames or [])]
        if missing:
            raise ValueError(f"CSV is missing columns: {missing}")
        for raw in reader:
            line = reader.line_num
            row = parse_row(raw, tz, line, warnings)
            if row[0] in seen:
                raise ValueError(f"duplicate rule_id {row[0]!r} at lines {seen[row[0]]} and {line}")
            seen[row[0]] = line
            rows.append(row)
    return rows, warnings


def upsert_sql() -> str:
    placeholders = ", ".join(f"${i + 1}" for i in range(len(COLUMNS)))
    updates = ", ".join(f"{c}=EXCLUDED.{c}" for c in COLUMNS if c != "rule_id")
    return (f"INSERT INTO metric_rules ({', '.join(COLUMNS)}) VALUES ({placeholders}) "
            f"ON CONFLICT (rule_id) DO UPDATE SET {updates}")


def build_dsn(database: str, dsn: str | None) -> str:
    """Return a DSN pointing at ``database``.

    Uses ``--dsn`` if given, otherwise the app's DSN (core.config settings, which
    resolves credentials the same way the app does) with only the database name
    swapped. The password is never printed.
    """
    if not dsn:
        from core.config import get_settings
        dsn = get_settings().postgres_dsn
    if not dsn:
        raise SystemExit("No DSN available: POSTGRES_HOST is not set and --dsn was not given.")
    parts = urlsplit(dsn)
    return urlunsplit(parts._replace(path="/" + database))


async def run(rows: list[tuple], *, database: str, dsn: str | None, dry_run: bool, yes: bool) -> int:
    import asyncpg

    if database == PROTECTED_DB:
        print(f"ERROR: refusing to write to {PROTECTED_DB!r} (treated as read-only).")
        return 2
    # statement_cache_size=0 keeps this working if the port is a PgBouncer pooler
    conn = await asyncpg.connect(build_dsn(database, dsn), statement_cache_size=0)
    try:
        if True:
            db = await conn.fetchval("SELECT current_database()")
            host = await conn.fetchval("SELECT inet_server_addr()::text")
            if db != database:
                print(f"ERROR: connected to {db!r}, expected {database!r}. Stopping.")
                return 2
            exists = await conn.fetchval("SELECT to_regclass('public.metric_rules') IS NOT NULL")
            if not exists:
                print("ERROR: table metric_rules does not exist in this database. "
                      "Start the app once (init_db) or run documents.db_schema first.")
                return 2
            before = await conn.fetchval("SELECT count(*) FROM metric_rules")
            existing = {r["rule_id"] for r in await conn.fetch(
                "SELECT rule_id FROM metric_rules WHERE rule_id = ANY($1::text[])",
                [r[0] for r in rows])}
            print(f"Database : {db}  (server {host})")
            print(f"CSV rows : {len(rows)}   already in table: {len(existing)} (will be updated)   "
                  f"new: {len(rows) - len(existing)}")
            print(f"metric_rules rows before: {before}")

            if dry_run:
                print("Dry run: nothing written.")
                return 0
            if not yes:
                answer = input(f"Type the database name ({db}) to write {len(rows)} rows: ")
                if answer.strip() != db:
                    print("Aborted. Nothing written.")
                    return 1

            async with conn.transaction():  # all-or-nothing
                for i in range(0, len(rows), 500):
                    await conn.executemany(upsert_sql(), rows[i:i + 500])

            after = await conn.fetchval("SELECT count(*) FROM metric_rules")
            in_csv = await conn.fetchval(
                "SELECT count(*) FROM metric_rules WHERE rule_id = ANY($1::text[])",
                [r[0] for r in rows])
            print(f"metric_rules rows after : {after}")
            print(f"CSV rule_ids present in table: {in_csv}/{len(rows)}")
            if in_csv != len(rows):
                print("WARNING: not every CSV rule_id is present after import.")
                return 3
            print("Done.")
            return 0
    finally:
        await conn.close()


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("csv_path", nargs="?", type=Path, default=Path(DEFAULT_CSV))
    p.add_argument("--dry-run", action="store_true", help="Parse CSV and compare with the table; write nothing")
    p.add_argument("--yes", action="store_true", help="Skip the confirmation prompt")
    p.add_argument("--tz", default="UTC", help="Timezone for naive updated_at values (default UTC)")
    p.add_argument("--database", default="rbl_ceo_new", help="Target database name (default rbl_ceo_new)")
    p.add_argument("--dsn", default=None,
                   help="Full DSN to use instead of the app's POSTGRES_DSN; its database name is replaced by --database")
    args = p.parse_args()

    if not args.csv_path.is_file():
        p.error(f"File not found: {args.csv_path}")
    rows, warnings = load_csv(args.csv_path, ZoneInfo(args.tz))
    print(f"Parsed {len(rows)} rows from {args.csv_path}")
    for w in warnings[:20]:
        print("  warning:", w)
    if len(warnings) > 20:
        print(f"  ... {len(warnings) - 20} more warnings")
    return asyncio.run(run(rows, database=args.database, dsn=args.dsn,
                           dry_run=args.dry_run, yes=args.yes))


if __name__ == "__main__":
    sys.exit(main())
