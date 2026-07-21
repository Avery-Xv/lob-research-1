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

## Testing Guidelines

For factor changes, validate event counts and values on one file, then compare serial and parallel outputs on a small identical sample. For backtests, verify factor time, entry time, and exit time explicitly. Never filter using future return-window information. Review `data/README.md` and the Shanghai `FULL`/`PARTIAL` linkage notes in `README.md` before using order-record links.

## Commit & Pull Request Guidelines

No Git history is present, so no repository-specific convention can be inferred. Use concise imperative commits such as `Fix point-in-time universe filter`. Pull requests should describe the factor definition, time boundaries, data universe, leakage controls, validation commands, and any regenerated artifacts. Include compact before/after metrics for behavioral changes; screenshots are unnecessary for this non-UI project.
