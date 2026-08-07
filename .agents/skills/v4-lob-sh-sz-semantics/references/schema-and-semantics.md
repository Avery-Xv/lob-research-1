# V4 schema and exchange semantics

## Actual `event_depth10_v4` schema

Both inspected SH and SZ files expose:

```text
date INTEGER                 time BIGINT
row_id BIGINT                source_action VARCHAR
source_recid BIGINT          source_buy_order_id BIGINT
source_sell_order_id BIGINT  source_side VARCHAR
source_price BIGINT          source_volume BIGINT
source_link_status VARCHAR
bid_px BIGINT[]  bid_vol BIGINT[]  bid_cnt BIGINT[]  bid_ordvol BIGINT[]
ask_px BIGINT[]  ask_vol BIGINT[]  ask_cnt BIGINT[]  ask_ordvol BIGINT[]
```

V4 does not contain `source_order_id` or `source_trade_id`. Derive the direction-specific order ID:

```sql
CASE WHEN source_side='B' THEN source_buy_order_id
     WHEN source_side='S' THEN source_sell_order_id END
```

Use `source_recid` to deduplicate event/trade records, falling back to `row_id` only when null. Use `(source_side, derived_order_id)` as the logical order key. Buy and sell numeric ID spaces must be treated as independent.

## Snapshot timing

Every row contains the book after its source event. To obtain an event pre-book, carry the prior valid snapshot within the same symbol, date, and continuous-auction session. A valid book has positive sides and `ask1 > bid1`. Count missing, locked, and crossed books separately.

Do not let an invalid row replace the last valid pre-book. Reset state at lunch, date change, and outside continuous auction.

## Shanghai

A marketable order commonly appears as one or more `TRADE` rows followed by an `ORDER_ADD` for its unexecuted remainder. A fully immediate order can have no `ORDER_ADD`.

For one `(side, active_order_id)`, original submitted quantity equals immediate trade quantity plus the later remainder. The later add is part of the active arrival. Exclude it from passive submission and quote-arrival metrics.

## Shenzhen

A marketable order commonly appears as a full submitted `ORDER_ADD` followed by child `TRADE` rows. The unmatched quantity remains without a second remainder add. The expansion may create temporary locked or crossed post-event books.

Require total child active trade quantity not to exceed submitted quantity. For price paths, compare the last valid pre-chain book with the first subsequent valid book; do not reorder the temporary chain.

## Point-in-time restrictions

`source_link_status` describes post-processed linkage and is forbidden in point-in-time features. The same restriction applies to FULL/PARTIAL status and forward record linkage. These may be offline audit labels only.

Future quote/trade/recovery behavior is a label, not an input available at the trigger time. Online episode termination must use only already-observed timeout, reversal, reset, or session-end information.

## Universe

V4 month directories include ETFs. Use a manifest generated from point-in-time security master data with `SecuCategory=1`, SH/SZ market whitelist, listing/termination dates, and certified `output_etf_symbols=0`. Never use an unrestricted monthly glob for a stock factor.
