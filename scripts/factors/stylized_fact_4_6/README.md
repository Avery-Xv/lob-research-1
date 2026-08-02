# Stylized Fact 4–6 Factor Reproduction

This directory is the centralized implementation path for reproducing the
daily factors documented in
`docs/stylized-fact-4-6_日频因子手册.md`.

Planned factor groups:

1. `D01`–`D03`: order, cancel, and trade mid-price shocks;
2. `D04`–`D06`: active large-order flow factors;
3. `D07`–`D09`: retained impact and passive-repair states;
4. `D10`–`D11`: liquidity-conditioned and residual-impact factors.

Implement each group so that all primary definitions and robustness variants
share one read of each input LOB parquet. Use a point-in-time A-share stock
manifest and validate that factor outputs contain no ETF symbols.

Generated intermediate data belongs under
`data/cache/stylized_fact_4_6/`; full-market daily outputs belong under
`data/processed/stylized_fact_4_6/`.

## Implemented

`reproduce_d01_d03.py` computes the 09:30-close, 10:00-close, and
10:00-10:30 O/C/T primitives from one v4 LOB read per input file. It supports
multiple single-threaded DuckDB workers, atomically persists every stable batch
to `--shard-dir`, validates a configuration manifest, and automatically skips
completed shards when the identical command is restarted. Final primitive and
factor files are written atomically after all shards are present.
