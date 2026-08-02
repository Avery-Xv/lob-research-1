import csv
from pathlib import Path

from scripts.factors.passive_large_gap_ratio.build_typical_spread import build_theta


def test_theta_uses_exact_previous_five_market_days(tmp_path: Path) -> None:
    raw = tmp_path / "raw.csv"
    output = tmp_path / "theta.csv"
    with raw.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["symbol", "date", "daily_typical_spread_raw", "valid_spread_snapshots"])
        for index, date in enumerate(
            ["20251201", "20251202", "20251203", "20251204", "20251205", "20251208"]
        ):
            writer.writerow(["SH600000", date, index + 1, 100])
        writer.writerow(["SZ000001", "20251208", 2, 100])

    build_theta(str(raw), str(output), {"202512"})
    rows = {(row["symbol"], row["date"]): row for row in csv.DictReader(output.open())}
    assert rows[("SH600000", "20251208")]["theta_5d_raw"] == "3.0"
    assert rows[("SH600000", "20251208")]["history_days"] == "5"
    assert rows[("SZ000001", "20251208")]["theta_5d_raw"] == ""
    assert rows[("SZ000001", "20251208")]["history_days"] == "0"
