# Repository Guidelines

## Project Structure & Module Organization

- `scripts/factors/<factor_name>/` contains each factor’s daily and intraday calculators; put new factors in their own named subdirectory.
- `scripts/backtests/` contains cross-sectional backtests.
- `data/cache/` stores reproducible intermediate tables; `data/processed/` stores full-market factor outputs.
- `data/manifests/`, `data/recovery/`, and `data/samples/` contain input lists, recovery runs, and small validation datasets.
- `results/daily/` and `results/intraday/` contain generated reports.
- Raw LOB parquet data is external under `/hdd_data/lob`; do not copy it into this repository.

There is currently no dedicated automated test directory. Add tests under `tests/`, mirroring the script area being tested, when introducing reusable logic.

## Build, Test, and Development Commands

Use the repository environment rather than system Python:

```bash
conda_lob/bin/python -m compileall -q scripts/factors scripts/backtests
conda_lob/bin/python scripts/factors/active_take_midprice/intraday_window_factor.py --help
conda_lob/bin/python scripts/backtests/backtest_open_to_open.py --help
```

Run a small factor smoke test before a full-market job:

```bash
conda_lob/bin/python scripts/factors/active_take_midprice/active_take_midprice_ratio_v3.py \
  /hdd_data/lob/event_full_depth_v3/202601/SH600000.parquet \
  --output /tmp/lob_factor_smoke.csv
```

## Coding Style & Naming Conventions

Use Python 3 type hints, four-space indentation, `snake_case` names, and `UPPER_CASE` module constants. Keep SQL in readable multiline strings with explicit time boundaries. Resolve repository paths from `Path(__file__)`; do not assume the caller's working directory. No formatter or linter is configured, so keep changes consistent with nearby code and run `py_compile`.

Name generated files by factor, window, universe, and horizon, for example `intraday_factor_1000_1030_202601.csv`. Place outputs in the appropriate `data/` or `results/` subdirectory, never at repository root.

## Data Universe Guidelines

Unless a task explicitly studies ETFs or other fund products, factor calculations
and backtests in this repository should use an A-share stock universe and must not
mix ETF LOB files into the input. In particular, do not pass an unrestricted
`event_depth10_v4/<month>/*.parquet` glob directly to a stock factor job because
the v4 directories include ETF files. Build the input list from point-in-time
security master data with an explicit stock-type whitelist; code-prefix filters
may be used only as a temporary, documented fallback. Do not rely on a later
return, market-cap, or risk-data join to remove ETFs implicitly. Record the
universe rule in generated artifacts and validate that the stock output contains
zero ETF symbols before a full-market run.

## Testing Guidelines

For factor changes, validate event counts and values on one file, then compare serial and parallel outputs on a small identical sample. For backtests, verify factor time, entry time, and exit time explicitly. Never filter using future return-window information. Review `data/README.md` and the Shanghai `FULL`/`PARTIAL` linkage notes in `README.md` before using order-record links.

## Shanghai/Shenzhen Immediate-Fill Handling

Treat every V3/V4 LOB row as a post-event book snapshot, but preserve the
exchange-specific publication order for a marketable order that is immediately
partially filled:

- Shanghai commonly publishes TRADE row(s) before an ORDER_ADD for the
  unexecuted remainder. A fully immediately filled order may have no ORDER_ADD.
  Reconstruct original submitted quantity as the immediate trade quantity plus
  any later remainder for the same side and active order ID. Do not use
  forward-linked FULL/PARTIAL record fields as point-in-time features.
- Shenzhen commonly publishes the full submitted quantity as ORDER_ADD,
  followed by child TRADE row(s). The unmatched quantity remains in the book
  without a second remainder ORDER_ADD. Intermediate post-event snapshots may
  be locked or crossed while the marketable order is expanded.
- Never reorder either exchange's events. For Shenzhen book-path, impact, depth,
  and quote-lifecycle features, retain the latest valid uncrossed pre-book
  across a temporary invalid chain and compare it with the first subsequent
  valid book. Count missing, locked, and crossed rows separately in QC; do not
  let them overwrite the valid pre-book.
- Count one logical active order by (side, active_order_id), not by TRADE rows.
  Under the default order-arrival classification, an immediately filled order
  and its remainder are one active order, and the remainder is not counted
  again as a passive submission. Any quantity-slice alternative must be
  explicitly labeled as a sensitivity specification.

Changes to reusable event-state logic must include Shanghai trade-before-add
and Shenzhen add-before-trade tests, including a temporarily crossed chain,
session reset, and serial/parallel equality on the same sample.

## Research Evaluation Defaults

By default, evaluate and report factor results without neutralization. Treat raw,
non-neutralized results as the primary research output. Run style, industry, LOB,
or other neutralized variants only when the user explicitly requests them or as a
clearly labeled secondary robustness check after the raw result; do not let a
neutralized specification silently replace the raw baseline.

## Commit & Pull Request Guidelines

No Git history is present, so no repository-specific convention can be inferred. Use concise imperative commits such as `Fix point-in-time universe filter`. Pull requests should describe the factor definition, time boundaries, data universe, leakage controls, validation commands, and any regenerated artifacts. Include compact before/after metrics for behavioral changes; screenshots are unnecessary for this non-UI project.
