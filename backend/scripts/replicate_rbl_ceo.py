#!/usr/bin/env python
"""
scripts/replicate_rbl_ceo.py
============================
Create a new PostgreSQL database (default ``rbl_ceo_new``) as a replica of an
existing one (default ``rbl_ceo``): same schema, same data.

Pure Python (asyncpg, already in requirements.txt). No pg_dump / pg_restore /
psql needed.

Safety
------
* The SOURCE database is only ever touched inside a ``BEGIN READ ONLY``
  ``REPEATABLE READ`` transaction, and only with SELECT / COPY ... TO.
  Postgres itself rejects any write there. It also gives a consistent snapshot.
* Refuses to run if the target database already exists. It never drops,
  truncates or overwrites anything.
* Shows host + source + target names and asks you to type the target name
  to confirm (skip with --yes).
* If CREATE DATABASE fails (e.g. PgBouncer on port 6543), it stops and asks
  you to supply the direct PostgreSQL port with --port. It does not work around it.
* The password is read from the PGPASSWORD env var (or prompted). Never
  hard-code it and never put it in the log.

Usage (PowerShell)::

    $env:PGPASSWORD = "<password>"
    python -m scripts.replicate_rbl_ceo --dry-run     # read-only preview
    python -m scripts.replicate_rbl_ceo               # asks for confirmation
    python -m scripts.replicate_rbl_ceo --port 5432   # direct Postgres port

What is replicated
------------------
extensions, schemas, enum + domain types, sequences (with current values),
functions/procedures, tables (columns, defaults, identity, generated columns,
collations, UNLOGGED), all rows (COPY), PK/UNIQUE/CHECK/EXCLUDE/FK constraints,
indexes, views, materialized views, triggers, table/column comments.

Not replicated (like pg_restore --no-owner --no-privileges): owners, GRANTs,
roles. Partitioned tables, foreign tables, composite types, aggregates and rules
are detected and reported as warnings and are NOT copied.
"""
from __future__ import annotations

import argparse
import asyncio
import getpass
import logging
import os
import sys

import asyncpg

DEFAULT_HOST = "rbl-common-applications-prod-nlb-c520789d68a02bed.elb.ap-south-1.amazonaws.com"
LOG_FILE = "replicate_rbl_ceo.log"

logger = logging.getLogger("replicate")


# --------------------------------------------------------------------------- helpers
def qi(name: str) -> str:
    """Quote an SQL identifier."""
    return '"' + name.replace('"', '""') + '"'


def qn(schema: str, name: str) -> str:
    return f"{qi(schema)}.{qi(name)}"


def lit(value: str) -> str:
    """Quote an SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


# user objects only: not information_schema, not pg_*
NS = "n.nspname <> 'information_schema' AND n.nspname !~ '^pg_'"


def not_ext(classname: str, oid_expr: str) -> str:
    """SQL fragment excluding objects owned by an extension."""
    return (
        "NOT EXISTS (SELECT 1 FROM pg_depend dx "
        f"WHERE dx.classid = '{classname}'::regclass AND dx.objid = {oid_expr} AND dx.deptype = 'e')"
    )


# --------------------------------------------------------------------------- metadata (source, read-only)
async def gather_metadata(src: asyncpg.Connection) -> dict:
    """Read the whole catalog description of the source. SELECT only."""
    md: dict = {}

    md["extensions"] = await src.fetch(
        """SELECT e.extname, n.nspname FROM pg_extension e
           JOIN pg_namespace n ON n.oid = e.extnamespace
           WHERE e.extname <> 'plpgsql' ORDER BY e.extname"""
    )
    md["schemas"] = await src.fetch(
        f"""SELECT n.nspname FROM pg_namespace n
            WHERE {NS} AND n.nspname <> 'public'
              AND {not_ext('pg_namespace', 'n.oid')} ORDER BY n.nspname"""
    )
    md["enums"] = await src.fetch(
        f"""SELECT n.nspname, t.typname,
                   array_agg(e.enumlabel ORDER BY e.enumsortorder) AS labels
            FROM pg_type t JOIN pg_enum e ON e.enumtypid = t.oid
            JOIN pg_namespace n ON n.oid = t.typnamespace
            WHERE {NS} AND {not_ext('pg_type', 't.oid')}
            GROUP BY 1, 2 ORDER BY 1, 2"""
    )
    md["domains"] = await src.fetch(
        f"""SELECT t.oid, n.nspname, t.typname,
                   format_type(t.typbasetype, t.typtypmod) AS base,
                   t.typnotnull, pg_get_expr(t.typdefaultbin, 0) AS def
            FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
            WHERE t.typtype = 'd' AND {NS} AND {not_ext('pg_type', 't.oid')}
            ORDER BY t.oid"""
    )
    md["domain_checks"] = await src.fetch(
        """SELECT contypid, conname, pg_get_constraintdef(oid) AS def
           FROM pg_constraint WHERE contypid <> 0 ORDER BY oid"""
    )
    md["sequences"] = await src.fetch(
        f"""SELECT n.nspname, c.relname, format_type(s.seqtypid, NULL) AS typ,
                   s.seqstart, s.seqincrement, s.seqmax, s.seqmin, s.seqcache,
                   s.seqcycle, pg_sequence_last_value(c.oid) AS lv
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_sequence s ON s.seqrelid = c.oid
            WHERE c.relkind = 'S' AND {NS}
              AND NOT EXISTS (SELECT 1 FROM pg_depend d
                              WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid
                                AND d.deptype IN ('i', 'e'))
            ORDER BY 1, 2"""
    )
    md["seq_owned_by"] = await src.fetch(
        f"""SELECT sn.nspname AS sschema, sc.relname AS seq,
                   tn.nspname AS tschema, tc.relname AS tbl, a.attname
            FROM pg_depend d
            JOIN pg_class sc ON sc.oid = d.objid AND d.classid = 'pg_class'::regclass
            JOIN pg_namespace sn ON sn.oid = sc.relnamespace
            JOIN pg_class tc ON tc.oid = d.refobjid AND d.refclassid = 'pg_class'::regclass
            JOIN pg_namespace tn ON tn.oid = tc.relnamespace
            JOIN pg_attribute a ON a.attrelid = tc.oid AND a.attnum = d.refobjsubid
            WHERE d.deptype = 'a' AND sc.relkind = 'S' AND d.refobjsubid > 0
              AND sn.nspname <> 'information_schema' AND sn.nspname !~ '^pg_'"""
    )
    md["identity_values"] = await src.fetch(
        f"""SELECT n.nspname, c.relname, a.attname, pg_sequence_last_value(d.objid) AS lv
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_depend d ON d.refobjid = c.oid AND d.refobjsubid = a.attnum
                 AND d.deptype = 'i' AND d.classid = 'pg_class'::regclass
                 AND d.refclassid = 'pg_class'::regclass
            WHERE a.attidentity <> '' AND NOT a.attisdropped AND c.relkind = 'r' AND {NS}"""
    )
    md["functions"] = await src.fetch(
        f"""SELECT n.nspname, p.proname, pg_get_functiondef(p.oid) AS def
            FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE p.prokind IN ('f', 'p') AND {NS} AND {not_ext('pg_proc', 'p.oid')}
            ORDER BY p.oid"""
    )
    md["tables"] = await src.fetch(
        f"""SELECT c.oid, n.nspname, c.relname, c.relpersistence
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'r' AND NOT c.relispartition AND {NS}
              AND {not_ext('pg_class', 'c.oid')}
            ORDER BY n.nspname, c.relname"""
    )
    cols: dict[int, list] = {}
    for t in md["tables"]:
        cols[t["oid"]] = await src.fetch(
            """SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS typ,
                      a.attnotnull, a.attidentity, a.attgenerated,
                      pg_get_expr(d.adbin, d.adrelid) AS def,
                      CASE WHEN a.attcollation <> t.typcollation
                           THEN quote_ident(cn.nspname) || '.' || quote_ident(co.collname) END AS coll
               FROM pg_attribute a
               JOIN pg_type t ON t.oid = a.atttypid
               LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
               LEFT JOIN pg_collation co ON co.oid = a.attcollation
               LEFT JOIN pg_namespace cn ON cn.oid = co.collnamespace
               WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped
               ORDER BY a.attnum""",
            t["oid"],
        )
    md["columns"] = cols
    md["constraints"] = await src.fetch(
        f"""SELECT n.nspname, c.relname, con.conname, con.contype,
                   pg_get_constraintdef(con.oid) AS def
            FROM pg_constraint con JOIN pg_class c ON c.oid = con.conrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE con.contype IN ('p', 'u', 'c', 'x', 'f') AND c.relkind = 'r'
              AND NOT c.relispartition AND {NS} AND {not_ext('pg_class', 'c.oid')}
            ORDER BY (con.contype = 'f'), n.nspname, c.relname, con.conname"""
    )
    md["views"] = await src.fetch(
        f"""SELECT n.nspname, c.relname, c.relkind, pg_get_viewdef(c.oid, true) AS def
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('v', 'm') AND {NS} AND {not_ext('pg_class', 'c.oid')}
            ORDER BY c.oid"""
    )
    md["indexes"] = await src.fetch(
        f"""SELECT n.nspname, c.relname AS tbl, i.relname AS idx,
                   pg_get_indexdef(i.oid) AS def
            FROM pg_index x JOIN pg_class i ON i.oid = x.indexrelid
            JOIN pg_class c ON c.oid = x.indrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('r', 'm') AND NOT c.relispartition AND {NS}
              AND {not_ext('pg_class', 'c.oid')}
              AND NOT EXISTS (SELECT 1 FROM pg_constraint k
                              WHERE k.conindid = x.indexrelid AND k.contype IN ('p', 'u', 'x'))
            ORDER BY 1, 2, 3"""
    )
    md["triggers"] = await src.fetch(
        f"""SELECT n.nspname, c.relname, t.tgname, pg_get_triggerdef(t.oid, true) AS def
            FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE NOT t.tgisinternal AND c.relkind = 'r' AND {NS}
              AND {not_ext('pg_class', 'c.oid')}
            ORDER BY 1, 2, 3"""
    )
    md["comments"] = await src.fetch(
        f"""SELECT n.nspname, c.relname, c.relkind, d.objsubid, a.attname, d.description
            FROM pg_description d
            JOIN pg_class c ON c.oid = d.objoid AND d.classoid = 'pg_class'::regclass
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = d.objsubid
            WHERE c.relkind IN ('r', 'v', 'm') AND {NS} AND {not_ext('pg_class', 'c.oid')}"""
    )
    md["unsupported"] = await src.fetch(
        f"""SELECT 'partitioned/foreign/composite relation' AS kind, n.nspname || '.' || c.relname AS name
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE (c.relkind IN ('p', 'f', 'c') OR c.relispartition) AND {NS}
              AND {not_ext('pg_class', 'c.oid')}
            UNION ALL
            SELECT 'aggregate', n.nspname || '.' || p.proname
            FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE p.prokind = 'a' AND {NS} AND {not_ext('pg_proc', 'p.oid')}
            UNION ALL
            SELECT 'rule', schemaname || '.' || tablename || ' (' || rulename || ')'
            FROM pg_rules WHERE schemaname <> 'information_schema' AND schemaname !~ '^pg_'"""
    )
    return md


# --------------------------------------------------------------------------- DDL builders
def create_table_sql(t, columns) -> str:
    parts = []
    for c in columns:
        s = f"{qi(c['attname'])} {c['typ']}"
        if c["coll"]:
            s += f" COLLATE {c['coll']}"
        if c["attgenerated"] == "s":
            s += f" GENERATED ALWAYS AS ({c['def']}) STORED"
        elif c["attidentity"] == "a":
            s += " GENERATED ALWAYS AS IDENTITY"
        elif c["attidentity"] == "d":
            s += " GENERATED BY DEFAULT AS IDENTITY"
        elif c["def"] is not None:
            s += f" DEFAULT {c['def']}"
        if c["attnotnull"]:
            s += " NOT NULL"
        parts.append(s)
    unlogged = "UNLOGGED " if t["relpersistence"] == "u" else ""
    return f"CREATE {unlogged}TABLE {qn(t['nspname'], t['relname'])} (\n  " + ",\n  ".join(parts) + "\n)"


def create_sequence_sql(s) -> str:
    return (
        f"CREATE SEQUENCE {qn(s['nspname'], s['relname'])} AS {s['typ']} "
        f"INCREMENT BY {s['seqincrement']} MINVALUE {s['seqmin']} MAXVALUE {s['seqmax']} "
        f"START WITH {s['seqstart']} CACHE {s['seqcache']} "
        f"{'CYCLE' if s['seqcycle'] else 'NO CYCLE'}"
    )


# --------------------------------------------------------------------------- target helpers
async def run(dst: asyncpg.Connection, sql: str, what: str) -> None:
    logger.info("%s", what)
    logger.debug("SQL: %s", sql)
    try:
        await dst.execute(sql)
    except asyncpg.PostgresError as exc:
        logger.error("FAILED: %s\n  SQL: %s\n  ERROR: %s", what, sql[:500], exc)
        raise


async def copy_table(src: asyncpg.Connection, dst: asyncpg.Connection, schema: str,
                     table: str, cols: list[str]) -> None:
    """Stream one table source -> target using COPY (text format), bounded memory."""
    if not cols:
        return
    col_sql = ", ".join(qi(c) for c in cols)
    query = f"SELECT {col_sql} FROM {qn(schema, table)}"
    queue: asyncio.Queue = asyncio.Queue(maxsize=64)

    async def sink(chunk: bytes) -> None:
        await queue.put(chunk)

    async def produce() -> None:
        try:
            await src.copy_from_query(query, output=sink, format="text")
        finally:
            await queue.put(None)

    async def chunks():
        while True:
            item = await queue.get()
            if item is None:
                return
            yield item

    producer = asyncio.create_task(produce())
    try:
        await dst.copy_to_table(table, schema_name=schema, columns=cols, source=chunks(), format="text")
    except BaseException:
        producer.cancel()
        raise
    await producer  # surfaces any source-side error


# --------------------------------------------------------------------------- main flow
async def main(args: argparse.Namespace) -> int:
    password = os.environ.get("PGPASSWORD") or getpass.getpass("Password: ")
    base = dict(host=args.host, port=args.port, user=args.user, password=password,
                statement_cache_size=0, command_timeout=None)  # statement_cache_size=0: PgBouncer-safe

    # ---- connect to source
    src = await asyncpg.connect(database=args.source_db, **base)
    logger.info("Connected to source %s on %s:%s", args.source_db, args.host, args.port)
    logger.info("Server: %s", await src.fetchval("SELECT version()"))

    # ---- check target does not exist
    admin = await asyncpg.connect(database="postgres", **base)
    exists = await admin.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", args.target_db)
    if exists:
        logger.error("Database %r already exists. Refusing to touch it. Choose another name "
                     "or remove it yourself.", args.target_db)
        return 3

    # ---- read-only snapshot of the source
    tx = src.transaction(isolation="repeatable_read", readonly=True)
    await tx.start()
    try:
        await src.execute("SET LOCAL search_path TO pg_catalog")  # force schema-qualified names
        if await src.fetchval("SHOW transaction_read_only") != "on":
            logger.error("Source transaction is not read-only. Aborting.")
            return 5
        md = await gather_metadata(src)

        logger.info("Source summary: %d tables, %d views, %d sequences, %d functions, "
                    "%d indexes, %d triggers, %d extensions",
                    len(md["tables"]), len(md["views"]), len(md["sequences"]), len(md["functions"]),
                    len(md["indexes"]), len(md["triggers"]), len(md["extensions"]))
        for u in md["unsupported"]:
            logger.warning("NOT COPIED (unsupported): %s %s", u["kind"], u["name"])

        if args.dry_run:
            for t in md["tables"]:
                n = await src.fetchval(f"SELECT count(*) FROM {qn(t['nspname'], t['relname'])}")
                logger.info("  %-50s %12d rows", f"{t['nspname']}.{t['relname']}", n)
            logger.info("Dry run complete. Nothing was created.")
            return 0

        # ---- confirmation
        print("\n" + "=" * 70)
        print(f"  Host         : {args.host}:{args.port}")
        print(f"  Source (RO)  : {args.source_db}")
        print(f"  NEW database : {args.target_db}")
        print("=" * 70)
        if not args.yes:
            answer = input(f"Type the new database name ({args.target_db}) to create it, anything else aborts: ")
            if answer.strip() != args.target_db:
                logger.info("Aborted by user. Nothing was created.")
                return 1

        # ---- create database (no workaround if it fails)
        try:
            await admin.execute(f"CREATE DATABASE {qi(args.target_db)}")
        except (asyncpg.PostgresError, OSError) as exc:
            logger.error("CREATE DATABASE failed: %s", exc)
            logger.error("Port %s may be a PgBouncer pooler. Re-run with the direct PostgreSQL "
                         "port, e.g. --port 5432. Stopping without trying any workaround.", args.port)
            return 4
        logger.info("Created database %s", args.target_db)

        dst = await asyncpg.connect(database=args.target_db, **base)

        # ---- pre-data DDL
        for e in md["extensions"]:
            try:
                await run(dst, f"CREATE EXTENSION IF NOT EXISTS {qi(e['extname'])} "
                               f"WITH SCHEMA {qi(e['nspname'])}", f"extension {e['extname']}")
            except asyncpg.PostgresError:
                logger.warning("Extension %s could not be created (privileges/availability?). "
                               "Objects that depend on it will fail.", e["extname"])
        for s in md["schemas"]:
            await run(dst, f"CREATE SCHEMA IF NOT EXISTS {qi(s['nspname'])}", f"schema {s['nspname']}")
        for e in md["enums"]:
            labels = ", ".join(lit(x) for x in e["labels"])
            await run(dst, f"CREATE TYPE {qn(e['nspname'], e['typname'])} AS ENUM ({labels})",
                      f"enum {e['nspname']}.{e['typname']}")
        for d in md["domains"]:
            sql = f"CREATE DOMAIN {qn(d['nspname'], d['typname'])} AS {d['base']}"
            if d["def"] is not None:
                sql += f" DEFAULT {d['def']}"
            if d["typnotnull"]:
                sql += " NOT NULL"
            await run(dst, sql, f"domain {d['nspname']}.{d['typname']}")
            for ck in md["domain_checks"]:
                if ck["contypid"] == d["oid"]:
                    await run(dst, f"ALTER DOMAIN {qn(d['nspname'], d['typname'])} "
                                   f"ADD CONSTRAINT {qi(ck['conname'])} {ck['def']}",
                              f"domain constraint {ck['conname']}")
        for s in md["sequences"]:
            await run(dst, create_sequence_sql(s), f"sequence {s['nspname']}.{s['relname']}")
        for f in md["functions"]:
            async with dst.transaction():
                await dst.execute("SET LOCAL check_function_bodies = off")
                await run(dst, f["def"], f"function {f['nspname']}.{f['proname']}")
        for t in md["tables"]:
            await run(dst, create_table_sql(t, md["columns"][t["oid"]]),
                      f"table {t['nspname']}.{t['relname']}")
        for o in md["seq_owned_by"]:
            await run(dst, f"ALTER SEQUENCE {qn(o['sschema'], o['seq'])} OWNED BY "
                           f"{qn(o['tschema'], o['tbl'])}.{qi(o['attname'])}",
                      f"sequence ownership {o['sschema']}.{o['seq']}")

        # ---- data (same snapshot), source counts taken in the same snapshot
        src_counts: dict[str, int] = {}
        for t in md["tables"]:
            key = f"{t['nspname']}.{t['relname']}"
            copy_cols = [c["attname"] for c in md["columns"][t["oid"]] if c["attgenerated"] != "s"]
            src_counts[key] = await src.fetchval(f"SELECT count(*) FROM {qn(t['nspname'], t['relname'])}")
            logger.info("COPY %-50s %12d rows", key, src_counts[key])
            await copy_table(src, dst, t["nspname"], t["relname"], copy_cols)
    finally:
        await tx.rollback()  # read-only txn: nothing to commit
        await src.close()

    # ---- post-data DDL
    for c in md["constraints"]:
        await run(dst, f"ALTER TABLE {qn(c['nspname'], c['relname'])} "
                       f"ADD CONSTRAINT {qi(c['conname'])} {c['def']}",
                  f"constraint {c['relname']}.{c['conname']}")

    pending = list(md["views"])
    for _ in range(10):  # views may depend on each other: retry until stable
        failed = []
        for v in pending:
            kind = "MATERIALIZED VIEW" if v["relkind"] == "m" else "VIEW"
            body = v["def"].rstrip().rstrip(";")
            sql = f"CREATE {kind} {qn(v['nspname'], v['relname'])} AS {body}"
            if v["relkind"] == "m":
                sql += " WITH DATA"
            try:
                await dst.execute(sql)
                logger.info("%s %s.%s", kind.lower(), v["nspname"], v["relname"])
            except asyncpg.PostgresError as exc:
                failed.append((v, exc))
        if not failed:
            pending = []
            break
        if len(failed) == len(pending):
            for v, exc in failed:
                logger.error("FAILED view %s.%s: %s", v["nspname"], v["relname"], exc)
            raise RuntimeError("Could not create some views (see errors above).")
        pending = [v for v, _ in failed]

    for i in md["indexes"]:
        await run(dst, i["def"], f"index {i['nspname']}.{i['idx']}")
    for tr in md["triggers"]:  # after data load so they do not fire during COPY
        await run(dst, tr["def"], f"trigger {tr['relname']}.{tr['tgname']}")

    for cm in md["comments"]:
        kind = {"r": "TABLE", "v": "VIEW", "m": "MATERIALIZED VIEW"}[cm["relkind"]]
        target = qn(cm["nspname"], cm["relname"])
        if cm["objsubid"] == 0:
            sql = f"COMMENT ON {kind} {target} IS {lit(cm['description'])}"
        else:
            sql = f"COMMENT ON COLUMN {target}.{qi(cm['attname'])} IS {lit(cm['description'])}"
        try:
            await dst.execute(sql)
        except asyncpg.PostgresError as exc:
            logger.warning("comment skipped: %s", exc)

    # ---- sequence values
    for s in md["sequences"]:
        if s["lv"] is not None:
            await dst.execute(f"SELECT setval({lit(qn(s['nspname'], s['relname']))}::regclass, {int(s['lv'])}, true)")
    for i in md["identity_values"]:
        if i["lv"] is not None:
            await dst.execute(
                f"SELECT setval(pg_get_serial_sequence({lit(qn(i['nspname'], i['relname']))}, "
                f"{lit(i['attname'])}), {int(i['lv'])}, true)")

    # ---- validation
    logger.info("Validating row counts ...")
    bad = 0
    print(f"\n{'table':<50} {'source':>12} {'target':>12}  status")
    for key, n_src in src_counts.items():
        schema, table = key.split(".", 1)
        n_dst = await dst.fetchval(f"SELECT count(*) FROM {qn(schema, table)}")
        ok = n_src == n_dst
        bad += 0 if ok else 1
        print(f"{key:<50} {n_src:>12} {n_dst:>12}  {'OK' if ok else 'MISMATCH'}")
    n_tbl = await dst.fetchval(
        "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE c.relkind='r' AND n.nspname <> 'information_schema' AND n.nspname !~ '^pg_'")
    print(f"\nTarget tables: {n_tbl} (source: {len(md['tables'])}); row-count mismatches: {bad}")
    await dst.close()
    await admin.close()
    return 0 if bad == 0 and n_tbl == len(md["tables"]) else 6


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Replicate a PostgreSQL database (schema + data) into a new one.")
    p.add_argument("--host", default=os.environ.get("PGHOST", DEFAULT_HOST))
    p.add_argument("--port", type=int, default=int(os.environ.get("PGPORT", "6543")))
    p.add_argument("--user", default=os.environ.get("PGUSER", "dbadmin"))
    p.add_argument("--source-db", default="rbl_ceo", help="Read-only source (default: rbl_ceo)")
    p.add_argument("--target-db", default="rbl_ceo_new", help="New database to create (default: rbl_ceo_new)")
    p.add_argument("--dry-run", action="store_true", help="Read source only, print plan + row counts, create nothing")
    p.add_argument("--yes", action="store_true", help="Skip the typed confirmation prompt")
    return p.parse_args()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(LOG_FILE, encoding="utf-8")],
    )
    ns = parse_args()
    if ns.source_db == ns.target_db:
        sys.exit("source and target database must differ")
    try:
        sys.exit(asyncio.run(main(ns)))
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception as exc:  # noqa: BLE001
        logger.error("Stopped: %s", exc)
        logger.error("The partially created target (if any) was NOT dropped. Inspect it, "
                     "and remove it yourself before re-running.")
        sys.exit(1)
