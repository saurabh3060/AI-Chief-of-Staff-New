"""
scripts/populate_sync_files.py
================================
Populate the ``onedrive_sync_files`` table for 24 documents whose doc_ids
are already known, so the daily OneDrive sync skips them and does not
re-ingest.

The script:
1. Verifies all 24 doc_ids exist in the ``documents`` table.
2. Lists files from all configured SharePoint sites via Graph API.
3. Matches them (case-insensitive) to FILE_DOC_MAP entries.
4. Shows a dry-run summary, then asks for confirmation before upserting.

Usage:
    cd backend
    python -m scripts.populate_sync_files
"""

from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

# Ensure backend/ is on sys.path when running as a script.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")


# ---------------------------------------------------------------------------
# Known file-name → doc_id mapping (24 entries)
# ---------------------------------------------------------------------------

FILE_DOC_MAP = {
    "01_Book_Growth_Analysis_4786.pdf": "6a086da43f56d203",
    "02_Overall_and_Segment_wise_MAB_5144.pdf": "894e6179aa5dc8c2",
    "03_Interest_Rate_Sensitivity_Deposits_Maturity_4354.pdf": "32ca2a293790fe66",
    "04_New_Originations_Month_Wise_1924.pdf": "dfe9c6ee8a13d400",
    "05_Disb_MIS_3363.pdf": "a2d68cc31a6da47f",
    "06_WB_Balance_Sheet_9623.pdf": "fa2376ada2b40e80",
    "07_Product_Wise_9556.pdf": "00575def516c24a2",
    "08_SMA_and_Collection_Efficiency_5659.pdf": "682b8dd772b21215",
    "09_Net_Advances_Insights_7222.pdf": "3b056a9be4b3b965",
    "10_Credit_Portfolio_Overview_2564.pdf": "4c338c520c0edd15",
    "2025-12-03 - Review - HL-LAP.pptx": "cdd6bbe92cdc5d1b",
    "2026-02-02 - MD Sir Review - Agri - Jan 2026.pptx": "a14996b7449cbeb0",
    "2026-03-06 - Review - Feb 2026 BBG.pptx": "594c59f72dab5dae",
    "2026-03-06 - Review - Mortgages - Feb26.pptx": "7a5287864f5b57f9",
    "2026-03-06 - Review - RBG - Feb 2026.pptx": "4511f8c90ad69fbd",
    "2026-03-06 - Review - RVF - Feb 2026.pptx": "462482762d97da30",
    "2026-03-06 - Review - Wholesale Banking (1).pptx": "48f29228ffe7f902",
    "CC12.pdf": "2a7323461ad9438b",
    "Collection Daily Refresh..pdf": "eba5ceb37604385e",
    "Liablity Summary.pdf": "791a6ce4b83f0c08",
    "MFI.pdf": "9a2109927e5060a3",
    "Overall Advances.pdf": "2f7b0e39c77a7c6e",
    "Retail Assets - Collections Efficiency.pdf": "d941c0a2ab773bcf",
    "Treasury_Review_Dec_2025.pptx": "d8ac3484d995c843",
}

assert len(FILE_DOC_MAP) == 24, f"Expected 24 entries, got {len(FILE_DOC_MAP)}"

# Build a lower-case lookup for case-insensitive matching
_NAME_LOWER_MAP: dict[str, tuple[str, str]] = {
    name.lower(): (name, doc_id) for name, doc_id in FILE_DOC_MAP.items()
}


async def main() -> None:
    from core.config import get_settings
    from core.db import init_db, close_db, get_pool

    settings = get_settings()

    if not settings.onedrive_sync_enabled:
        print("[ERROR] ONEDRIVE_SYNC_ENABLED is not true in .env")
        return

    sites = settings.sharepoint_sites_list
    if not sites:
        print("[ERROR] No SharePoint folders configured (SHAREPOINT_SITES / SHAREPOINT_SITE).")
        return

    # ── 1. Connect to PostgreSQL ──
    await init_db()

    try:
        pool = get_pool()
        if pool is None:
            print("[ERROR] PostgreSQL pool not available after init_db().")
            return

        # ── 2. Verify doc_ids exist in documents table ──
        all_doc_ids = list(FILE_DOC_MAP.values())
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT doc_id FROM documents WHERE doc_id = ANY($1::text[])",
                all_doc_ids,
            )
        found_ids = {r["doc_id"] for r in rows}
        missing_ids = [d for d in all_doc_ids if d not in found_ids]

        print(f"\n[DOC-ID CHECK] Found {len(found_ids)}/24 doc_ids in documents table.")
        if missing_ids:
            print(f"[WARNING] {len(missing_ids)} doc_id(s) NOT in documents table:")
            for mid in missing_ids:
                fname = next(k for k, v in FILE_DOC_MAP.items() if v == mid)
                print(f"  - {mid}  ({fname})")
            print("[WARNING] Those files may still get re-synced due to the _doc_missing check.\n")

        # ── 3. Get a valid bearer token ──
        from auth.token_store import get_latest_tokens, refresh_access_token

        tokens = await get_latest_tokens(settings)
        if tokens is None:
            print("[ERROR] No tokens found. A user must sign in via /auth/login first.")
            return

        bearer_token = await refresh_access_token(row=tokens, settings=settings)
        if bearer_token is None:
            print("[ERROR] Could not refresh the access token. Sign in again via /auth/login.")
            return

        # ── 4. List files from every configured SharePoint folder ──
        from core.graph_client.client import GraphClient

        client = GraphClient(settings)
        all_items: list[dict] = []

        for idx, site_url in enumerate(sites, 1):
            print(f"[INFO] Listing folder {idx}/{len(sites)}: {site_url[:100]}{'…' if len(site_url) > 100 else ''}")
            try:
                files, _drive_id, _root_id = await client.list_sharepoint_files_recursive(
                    sharing_url=site_url,
                    bearer_token=bearer_token,
                    file_extensions=settings.onedrive_extensions_tuple,
                )
                all_items.extend(files)
                print(f"  → {len(files)} file(s)")
            except Exception as exc:
                print(f"  [ERROR] Failed to list folder: {exc}")

        print(f"\n[INFO] Total SharePoint files found: {len(all_items)}\n")

        # ── 5. Match by name (case-insensitive) ──
        matched: list[dict] = []
        matched_names_lower: set[str] = set()

        for item in all_items:
            item_name = item.get("name", "")
            key = item_name.lower()
            if key in _NAME_LOWER_MAP:
                orig_name, doc_id = _NAME_LOWER_MAP[key]
                matched.append({
                    "item_id": item["id"],
                    "name": item_name,
                    "relative_path": item.get("_relative_path", ""),
                    "last_modified": item.get("lastModifiedDateTime", ""),
                    "size": item.get("size", 0),
                    "doc_id": doc_id,
                })
                matched_names_lower.add(key)

        unmatched = [
            (name, doc_id)
            for name, doc_id in FILE_DOC_MAP.items()
            if name.lower() not in matched_names_lower
        ]

        # ── 6. Dry-run display ──
        print(f"{'═' * 100}")
        print(f"  MATCHED ({len(matched)}/24):")
        print(f"{'═' * 100}")
        print(f"  {'#':>3}  {'SharePoint item_id':<50}  {'File Name':<45}  {'doc_id'}")
        print(f"  {'─' * 3}  {'─' * 50}  {'─' * 45}  {'─' * 20}")
        for i, m in enumerate(matched, 1):
            print(f"  {i:>3}  {m['item_id']:<50}  {m['name']:<45}  {m['doc_id']}")

        if unmatched:
            print(f"\n{'═' * 100}")
            print(f"  UNMATCHED ({len(unmatched)}/24):")
            print(f"{'═' * 100}")
            for name, doc_id in unmatched:
                print(f"  - {name}  (doc_id: {doc_id}) — not found in SharePoint")

        if not matched:
            print("\n[INFO] Nothing to insert — no matches found.")
            return

        # ── 7. Check existing rows ──
        matched_item_ids = [m["item_id"] for m in matched]
        async with pool.acquire() as conn:
            existing_rows = await conn.fetch(
                "SELECT item_id FROM onedrive_sync_files WHERE item_id = ANY($1::text[])",
                matched_item_ids,
            )
        existing_ids = {r["item_id"] for r in existing_rows}
        if existing_ids:
            print(f"\n[NOTE] {len(existing_ids)} row(s) already exist in onedrive_sync_files and will be updated (upserted):")
            for eid in existing_ids:
                print(f"  - {eid}")

        # ── 8. Confirmation prompt ──
        print()
        answer = input(f"Proceed with inserting/updating {len(matched)} rows into onedrive_sync_files? (y/n) ")
        if answer.strip().lower() != "y":
            print("[ABORTED] No changes made.")
            return

        # ── 9. Insert/upsert ──
        now = datetime.now(timezone.utc)
        async with pool.acquire() as conn:
            async with conn.transaction():
                for m in matched:
                    await conn.execute(
                        """
                        INSERT INTO onedrive_sync_files
                            (item_id, name, relative_path, last_modified, size, doc_id, synced_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        ON CONFLICT (item_id) DO UPDATE SET
                            name          = EXCLUDED.name,
                            relative_path = EXCLUDED.relative_path,
                            last_modified = EXCLUDED.last_modified,
                            size          = EXCLUDED.size,
                            doc_id        = EXCLUDED.doc_id,
                            synced_at     = EXCLUDED.synced_at
                        """,
                        m["item_id"],
                        m["name"],
                        m["relative_path"],
                        m["last_modified"],
                        m["size"],
                        m["doc_id"],
                        now,
                    )

        print(f"\n[DONE] {len(matched)} rows inserted/updated in onedrive_sync_files.")

    except Exception as exc:
        print(f"\n[ERROR] {exc}")
        raise

    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
