## Goal

Remove the orphaned "Sign-off on ₹8,500 Cr top-20 wholesale pipeline composition" item (PD5) from the executive dashboard's "Awaiting Your Decision" card, since it doesn't correspond to any action item, ledger entry, or workspace open item.

## Change

**File:** `src/context/AppState.tsx`

Delete the `PD5` entry from the `SEED_PENDING` array (line 197):

```ts
{ id: "PD5", text: "Sign-off on ₹8,500 Cr top-20 wholesale pipeline composition", urgency: "overdue", source: "MoM", team: "Wholesale", dueDate: "Past due", status: "open" },
```

## Leave untouched

- Chat responses in `src/data/mockData.ts` (lines 163, 180, 195) and the seeded CRO/Wholesale questions still reference the ₹8,500 Cr pipeline as context — that's fine, it's narrative background, not a tracked item.
- DR question context for "Status of the ₹8,500 Cr top-20 pipeline" in `DRWorkspace.tsx` stays — it's a discussion prompt, not a decision card.
- `WholesaleWorkspace.tsx` past meetings already mention the pipeline as a reviewed item; no change needed.

## Verification

After the change, the "Awaiting Your Decision" card on the executive dashboard will no longer show the PD5 overdue card, and the overdue-decision count will decrement by 1. Nothing else in the app references PD5 by id.