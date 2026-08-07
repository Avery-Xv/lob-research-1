---
name: v4-lob-sh-sz-semantics
description: Interpret and audit Shanghai/Shenzhen A-share event_depth10_v4 LOB data and factor code. Use when reading V4 parquet schemas, deriving active/passive order identity, handling Shanghai trade-before-remainder and Shenzhen add-before-trade sequences, maintaining point-in-time pre-books, reviewing LOB SQL/Python, diagnosing order conservation or crossed-book states, or preparing factor jobs that must avoid ETF, forward-link, field-name, and event-order errors.
---

# V4 LOB SH/SZ Semantics

Apply a schema-first, exchange-aware audit before interpreting or computing V4 LOB factors.

## Workflow

1. Read `references/schema-and-semantics.md` before changing event projections, order joins, passive/active classification, or pre-book logic.
2. Inspect the actual parquet schema; never infer V4 fields from V3 code or documentation.
3. Run `scripts/audit_v4_projection.py` on every changed implementation. Treat any error as blocking.
4. Trace at least one Shanghai trade-before-add remainder and one Shenzhen add-before-trade order on real data.
5. Require `(source_side, order_id)` in every order key, join, deduplication, and distinct count.
6. Preserve source `row_id` order and post-event snapshot semantics. Never reorder events to make the book appear valid.
7. For path features, retain the last valid uncrossed pre-book across missing, locked, or crossed rows; reset at date and session boundaries.
8. Run the repository Q001-Q008 preflight before creating a production factor manifest.

## Hard failures

Stop and fix the implementation when any condition holds:

- It reads `source_order_id` or `source_trade_id` from V4.
- It uses `source_link_status`, FULL/PARTIAL, or forward recid linkage in a point-in-time feature.
- It identifies an order only by numeric ID without `source_side`.
- It counts trade rows as active-order count.
- It treats every `ORDER_ADD` as a passive submission.
- It uses the current post-event book as the event's pre-book.
- A locked/crossed/missing row overwrites the last valid pre-book.
- It scans an unrestricted V4 month glob for a stock factor.
- It silently combines SH and SZ before exchange-specific QC passes.

## Minimal command

```bash
conda_lob/bin/python .agents/skills/v4-lob-sh-sz-semantics/scripts/audit_v4_projection.py \
  --parquet /hdd_data/lob/event_depth10_v4/202601/SH600000.parquet \
  --implementation scripts/factors/example.py
```

Use repository scripts under `scripts/audits/` for real event traces and `scripts/pipelines/preflight.py` to gate production.

## Interpretation boundary

Treat raw, non-neutralized factor results as primary unless the research specification explicitly requests a secondary neutralized robustness result. Keep engineering QC in Q records, not R experiment numbering.
