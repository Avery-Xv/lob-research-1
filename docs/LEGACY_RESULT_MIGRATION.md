# 旧结果审计与迁移清单

审计日期：2026-08-07。当前注册表共有 **17 组因子（F001–F017）**和
**25 个研究实验（R001–R025）**。本清单只认可当前 F/R/P 主键；旧编号仅作为
`research/idea_lineage.json` 中的别名和来源线索。

## 可以直接继承

- **F013 / R001**：M1–M6、medium300、202601 的 75/75 批次完整；300 只股票、
  5,981 个 stock-days，ETF 为 0，重复成交和 fill-over-submit 均为 0。旧版与当前
  `order_shape_mechanism/engine.py` 哈希一致。该结果是机制/直接目标基准，不应扩张
  解释为未来收益结论。
- **P002**：旧 Batch A 的 150/150 批次完整。迁移时主动删除受沪深挂单语义影响的
  `aggressive_add_buy`、`aggressive_add_sell`、`quote_aggressive_net`，剩余列可供
  F014 的 NP01–NP05 实现使用，但不能重建报价到达主动性。

## 只迁移为部分证据

- **F001**：继承 SH、202601–202602 的安全 pre-book 输出（77,425 行、2,306 只股票、
  ETF 为 0）；SZ 旧任务未完成，R002 所需收益期限与扩月也未完成。
- **P001 / F008 / F010 / F011 / R005 / R007 / R008**：继承 SZ、202601–202602 的
  5,764/5,764 stock-month 第一层机制输出（97,791 stock-days、ETF 为 0）。仍需补 SH、
  atomic-chain 或注册表指定的收益/条件比较，因此 manifest 明确标为 `partial`。

## 不迁移，需重算或首次计算

- 已提交过但不能沿用：F002（定义未冻结）、F003（旧 side/order key 错）、F004
  （依赖 F002 且修复任务不完整）、F005（修复任务不完整）。
- 当前定义未完成过：F006；定义尚待冻结的 F007/F009/F012；尚未实现的
  F014–F017。
- 研究实验除 R001 完整、R005/R007/R008 部分外，都没有满足当前 R 规格的完整结果。
  R002 虽有 F001 的沪市因子输入，但跨月收益实验本身尚未完成。

机器可读的逐类清单见 `research/legacy_compute_inventory.json`；实体迁移和哈希见
`runs/legacy_migration_summary.json` 及对应 `runs/{factors,research,data_products}/...`
manifest。所有迁移均为复制，旧目录未删除；失败的首次尝试临时副本已在正式迁移和哈希复核后清理。

## Idea 溯源结论

旧目录 `docs/` 下 13 份 Markdown 已逐份枚举。当前 17 个 F 和 25 个 R 均有文档章节
来源，注册表覆盖检查要求两边 ID 恰好完全覆盖。OI01–OI08、OD01–OD07 没有遗漏：
其中可继续的问题已吸收到 M/NP/NPE，M2/M3 原方向和若干大价差硬交集规格已明确停止，
不再创建重复编号。完整处置表见 `research/idea_lineage.json`，其
`unmapped_legacy_ideas` 必须保持为空。
