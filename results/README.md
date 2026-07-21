# Results Directory

- `daily/` contains open-to-open daily cross-sectional backtests.
- `intraday/` contains intraday IC, decile, long-only, and horizon-decay output.

Current intraday reports are exploratory historical artifacts. Reports using
monthly file-size universes or future-horizon limit-hit filters inherit the
point-in-time issues documented in `data/README.md` and must be rerun before
being interpreted as tradable out-of-sample results.
