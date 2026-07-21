# Data Directory

This directory contains generated research data. Raw LOB parquet files remain
under `/hdd_data/lob` and are not copied into this workspace.

## Subdirectories

- `cache/`: intermediate factor windows and minute-return joins that can be
  regenerated.
- `manifests/`: input parquet path lists and missing-file recovery lists.
- `processed/`: full-market daily factor outputs used as backtest inputs.
- `recovery/`: outputs from successive missing-file recomputation passes.
- `samples/`: small one-file, ten-file, twenty-file, and database check outputs.

## Historical Artifacts

The following files are retained for reproducibility but should not be treated
as clean point-in-time production inputs:

- names containing `size_le_p80` use the earlier, incorrect interpretation of
  the activity filter;
- `size_ge_p20` keeps the largest 80% of monthly files, but monthly final file
  size uses future activity when applied to an earlier intraday signal;
- minute-return files containing `filtered` used realized limit-hit information
  through the return horizon and therefore contain look-ahead filtering.

For a clean rerun, build the active universe from information available by the
signal timestamp and apply limit-state filters only up to that timestamp.
