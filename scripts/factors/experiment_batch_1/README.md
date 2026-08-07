# experiment_batch_1

统一扫描点时 A 股 V4 Event-LOB 文件，在每个 `stock-month` 的一次
`[10:00, 10:30)` 读取中输出：

- `signals.csv`：NPE、MSI、D07–D11 可复用的股票—日 primitive；
- `active_order_chains.csv`：按 `(side, active_order_id)` 的可观测执行链；
- `quote_lifecycles.csv`：价差内新增最优挡位的深度、存活、再命中和恢复；
- `quality.csv`：事件数、缺失订单 ID、删失量和盘口质量。

NP01–NP05 继续读取已有 Batch A 缓存，不在这里重读原始 LOB。第一版不读取
`source_link_status`、订单 recid 链接和逐笔队列数组，避免事后链接泄漏并控制 IO。

V2 对深圳可成交委托的 `ORDER_ADD + TRADE(s)` 展开使用 safe pre-book：
临时锁定或交叉 post-book 只进入质量统计，不覆盖最近有效盘口；盘口冲击按
事件链前最后有效盘口到链后首个有效盘口计算一次。上海原有事件顺序不重排。

正式运行：

```bash
conda_lob/bin/python scripts/factors/experiment_batch_1/run.py \
  --file-list data/manifests/v4_a_share_stock_paths_202601_202604.txt \
  --universe-metadata data/manifests/v4_a_share_stock_paths_202601_202604.metadata.json \
  --months 202601 202602 \
  --exchange ALL \
  --workers 8 --batch-size 1 --memory-limit 1GB \
  --shard-dir data/cache/experiment_batch_1/intraday_1000_1030_202601_202602_v1
```

正式产物必须保持 raw、不做评估中性化为第一顺位。因子定义内部的历史残差化
需要在后续派生层严格滞后实现。
