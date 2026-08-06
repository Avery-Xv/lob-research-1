# Order-shape mechanism reproduction

This module validates the six mechanism claims behind
`docs/订单簿形态与非对称自刺激_增量因子手册.md`. It does not run a return
backtest or fit OI01--OI08 prediction models.

## I/O contract

- Consume events in the order stored by V4 and require `row_id` to increase
  strictly inside each stock-day.
- Scan every eligible stock-month Parquet at most once.
- Project only event, trade, and top-of-book price fields in warmup months.
- Add side-specific order IDs, source price, and bid/ask volume arrays only in
  the target month.
- Never read `bid_ordvol`, `ask_ordvol`, `bid_cnt`, `ask_cnt`, or
  `source_link_status` for this mechanism-only experiment.
- Persist stock-day sufficient statistics instead of event- or order-level
  copies of the LOB.

Future-event labels use bounded queues. Passive orders are finalized at each
trading-day end. Multiple horizons, decay half-lives, and depth definitions
share the same target-month stream.

## Safety gates

The CLI requires an A-share manifest whose metadata certifies zero ETFs. The
run manifest freezes the input-list hashes, month split, field projections, and
parameters. Existing shards are reused only when the fingerprint matches.

Start with `--dry-run`. After code review, use `--audit-symbols` and
`--audit-dates` for a two-symbol audit. Do not submit the full-market job until
the audit trace, order conservation, serial/parallel equality, and zero-ETF
checks pass.

Proposed audit command (not to be run before approval):

```bash
conda_lob/bin/python scripts/factors/order_shape_mechanism/reproduce_mechanisms_v4.py \
  --file-list data/manifests/v4_a_share_stock_paths_202510_202601.txt \
  --universe-metadata data/manifests/v4_a_share_stock_paths_202510_202601.metadata.json \
  --warmup-months 202510 202511 202512 \
  --target-month 202601 \
  --audit-symbols SH600000 SZ000001 \
  --audit-dates 20260105 20260106 \
  --workers 1 \
  --batch-size 1 \
  --shard-dir data/cache/order_shape_mechanism/audit_202601
```
