# Active-Take Midprice Factor

当前母单/非母单日内初筛先阅读
[`docs/HANDOFF_INTRADAY_STRUCTURE_PILOT.md`](docs/HANDOFF_INTRADAY_STRUCTURE_PILOT.md)：
固定 `[10:00, 10:30)`、10:30 信号和 raw 未中性化主结果。

研究资产现已按因子 `F001...` 与研究实验 `R001...` 两条管线管理。新任务先查看
[`docs/RESEARCH_PIPELINES.md`](docs/RESEARCH_PIPELINES.md) 和 `research/` 注册表；
计算提交前还必须通过 [`docs/COMPUTE_PREFLIGHT.md`](docs/COMPUTE_PREFLIGHT.md) 的不可变审计凭证；
旧 D/M/PB/AG/NP 编号仅作为迁移别名。

This workspace implements a daily factor from full-depth event LOB parquet files:

```text
active_take_mid_gap = sum(abs(delta_mid))
```

where `delta_mid` is kept only when the adjacent snapshot change is inferred to be
caused by active consumption of the opposite best-side queue.

## Project Layout

```text
scripts/
  factors/
    active_take_midprice/  active-take midprice factor entry points
  backtests/     backtest entry points
data/
  cache/         reproducible intermediate factor and return tables
  manifests/     parquet path lists and recovery manifests
  processed/     full-market factor outputs
  recovery/      missing-file recomputation outputs
  samples/       small development and consistency-check samples
results/
  daily/         daily open-to-open backtest reports
  intraday/      intraday IC, portfolio, and horizon reports
```

`README.md` stays at the project root. Python environments (`conda_lob` and
`.venv`) are runtime dependencies and are intentionally not mixed with source
or research data.

## Input Assumption

Each parquet row is an event-after full-depth book snapshot:

```text
date, time,
bid_px, bid_vol, bid_cnt, bid_ordvol,
ask_px, ask_vol, ask_cnt, ask_ordvol
```

`bid_ordvol` and `ask_ordvol` are flattened per-order queues. Use `*_cnt` to split
them back by price level.

## Inference Rule

The script uses only continuous auction time:

```text
09:30:00.000 <= time < 11:30:00.000
13:00:00.000 <= time < 14:57:00.000
```

For each adjacent snapshot on the same date:

* compute `mid = (best_bid + best_ask) / 2`;
* ignore rows where `mid` does not change;
* if `mid` rises, require the ask best to move up because old ask levels were
  consumed from the front of the ask queue;
* if `mid` falls, require the bid best to move down because old bid levels were
  consumed from the front of the bid queue.

Queue consumption is checked with FIFO semantics. For example:

```text
[1000, 500] -> [700, 500]  keeps 300 consumed
[1000, 500] -> [200]       keeps 1300 consumed
[200, 800, 400] -> [200, 400] rejects, because it removes the middle order
```

## Output

Run:

```bash
conda_lob/bin/python scripts/factors/active_take_midprice/active_take_midprice_factor.py \
  '/hdd_data/lob/event_full_depth/202501/SH688184.parquet' \
  -o data/samples/active_take_midprice_factor_sample.csv
```

Output columns:

```text
symbol,date,
active_take_mid_gap,
active_take_mid_gap_signed,
active_take_events,
active_take_qty,
all_mid_gap,
mid_moves,
ambiguous_mid_moves,
transitions,
rows,
active_mid_move_share
```

Prices are divided by `10000` in the output factor columns.

## Dataset Version Notes

The limitation below applies to the legacy `event_full_depth` dataset:

- it has no trade ID, order ID, execution price, or raw event type;
- a full removal of the best price level can be either an execution or a
  cancellation;
- factors computed from it are order-book inferred factors rather than
  ground-truth trade classifications.

The historical implementation below uses `event_full_depth_v3`; current event-level
research uses `event_depth10_v4` under the repository V4 semantics skill. In addition to the full-depth
book snapshot, v3 contains source event fields such as:

```text
source_kind, source_action, source_recid,
source_order_id, source_trade_id,
source_buy_order_id, source_sell_order_id,
source_buy_order_recid, source_sell_order_recid,
source_side, source_price, source_volume,
source_link_status
```

These fields support order-lifecycle reconstruction, but Shanghai and Shenzhen
must not be reconstructed with the same event-order assumptions.

## Shanghai Pre-Match Publication Semantics

For Shanghai securities, a marketable incoming order can be matched before its
unexecuted remainder is published as an `ORDER_ADD` event. A single order may
therefore appear in v3 in the following order:

```text
TRADE (aggressive execution)
TRADE (aggressive execution)
ORDER_ADD (unexecuted remainder enters the visible book)
TRADE / CANCEL (later lifecycle of the resting remainder)
```

The immediate executions are not absent from the order-book data. Every
immediate match produces a `TRADE`-triggered v3 row, and the full-depth snapshot
on that row is the book state after that individual match. If one incoming
order sweeps several resting orders, v3 therefore contains several consecutive
`TRADE`-triggered book updates. What may be absent or delayed is the incoming
order's `ORDER_ADD`, not the book updates caused by its executions.

An observed example in `SH600004` on `20260105` uses order ID `296124`:

```text
row_id 1146  TRADE      sell 500 @ 9.46
row_id 1147  TRADE      sell 200 @ 9.46
row_id 1148  TRADE      sell 100 @ 9.46
row_id 1149  ORDER_ADD  sell 2700 @ 9.46
row_id 1178  TRADE      remaining 2700 @ 9.46
```

The original incoming quantity can be inferred as:

```text
800 pre-add executed + 2700 published remainder = 3500
```

Consequences:

- The first occurrence of a Shanghai aggressive order may be a `TRADE`, not an
  `ORDER_ADD`.
- Immediate executions and their book effects must be read from the `TRADE`
  trigger rows; each row's snapshot is post-match, not pre-match.
- An `ORDER_ADD` after one or more trades can represent only the matched
  order's remainder, not its original submitted quantity.
- A Shanghai order that is fully executed immediately may never have an
  `ORDER_ADD`.
- Passive resting orders can generally be reconstructed through
  `ORDER_ADD -> TRADE(s) -> CANCEL/full execution`.
- Order-arrival counts, cancellation rates, fill rates, and order lifetimes
  based only on `ORDER_ADD` are not directly comparable between Shanghai and
  Shenzhen.

## `FULL` and `PARTIAL` Link Status

For Shanghai trades, `source_link_status` describes whether the v3 builder was
eventually able to link both sides to order records. It does not prove that both
order records were already observable when the trade occurred.

- `PARTIAL` commonly means the aggressive order was fully executed immediately,
  so only the passive side can be linked to an `ORDER_ADD`.
- `FULL` can mean both orders were already resting, but it can also mean the
  aggressive order had a remainder that produced a later `ORDER_ADD`; the v3
  builder then linked earlier trades forward to that later record.
- For an aggressive buy with a partial link, `source_buy_order_recid` is
  typically missing.
- For an aggressive sell with a partial link, `source_sell_order_recid` is
  typically missing.

Always compare the referenced order event's `row_id` with the trade `row_id`.
Do not assume that a non-null `source_buy_order_recid` or
`source_sell_order_recid` points backward in event time.

## Point-in-Time Safety

`source_link_status`, `source_buy_order_recid`, and `source_sell_order_recid`
are post-processed linkage fields. For real-time or point-in-time backtests,
they may contain information obtained from a later `ORDER_ADD`.

Do not use these fields directly as signal-time features unless the referenced
record satisfies:

```text
referenced_order_row_id <= current_row_id
```

Safer point-in-time reconstruction rules are:

- process events strictly by `(date, row_id)`;
- treat the first Shanghai `TRADE` for an unseen aggressive order ID as an
  implicit order arrival;
- aggregate its executions until a same-ID `ORDER_ADD` appears;
- interpret that later `ORDER_ADD` quantity as the unexecuted remainder;
- never use future link availability to classify an order at an earlier time.

The current `active_take_mid_gap` factors use `source_action`, `source_side`,
and adjacent book changes. They do not use the post-processed order-record
links, so the Shanghai forward-link issue does not directly affect their
calculation.
