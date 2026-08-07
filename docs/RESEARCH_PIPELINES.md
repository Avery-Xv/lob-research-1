# 因子与实验双管线

新仓库不沿用旧项目的 D、M、PB、AG、NP 等编号作为主键。旧编号只保存在注册表的 `legacy_aliases` 中，用于查找旧文档、代码和结果。

## 编号规则

- `F001...`：可独立定义、计算和版本化的因子或公共 primitive。
- `E001...`：有明确假设、输入因子、样本与结果的实证任务。
- 编号不编码月份、交易所、窗口或修复批次；这些属于 run manifest。
- 因子定义变化提升 `definition_version`，实验设计变化提升 `spec_version`，不随意换 ID。

唯一总账：

- `research/factors.json`
- `research/experiments.json`

## 逻辑链路

```text
理论文档
  -> F 因子定义与实现
  -> 因子 run manifest（月份/交易所/窗口/股票池/代码版本）
  -> E 实证计划（绑定一个或多个因子 run）
  -> 实证结果与 supersedes 映射
```

任务顺序按逻辑闸门组织：

1. `E001` 验证沪深事件语义、safe pre-book、ETF=0、边界重置与串并行一致性。
2. `E002-E006` 完成单因子复现、定义审计和 raw 检验。
3. `E007` 固化跨市场公共事件 primitive。
4. `E008-E011` 进行 Event-LOB、非母单、深档撤单和母单结构机制研究。
5. `E012` 只接收机制通过且定义版本冻结的候选，统一做 raw 收益与稳健性检查。

## 使用方式

查看因子和实验总账：

```bash
conda_lob/bin/python scripts/pipelines/factor_pipeline.py status
conda_lob/bin/python scripts/pipelines/factor_pipeline.py show F001
conda_lob/bin/python scripts/pipelines/experiment_pipeline.py status
conda_lob/bin/python scripts/pipelines/experiment_pipeline.py show E001
```

为一次计算建立不可覆盖的计划：

```bash
conda_lob/bin/python scripts/pipelines/factor_pipeline.py plan F001 \
  --run-id 202601_202602_sh_safe_v2 \
  --months 202601 202602 --exchange SH --window 1000_1030 \
  --manifest data/manifests/v4_a_share_stock_paths_202601_202602.txt
```

为实验绑定实际因子 run：

```bash
conda_lob/bin/python scripts/pipelines/experiment_pipeline.py plan E002 \
  --run-id 202601_202602_sh_safe_v2 \
  --factor-run F001=runs/factors/F001/202601_202602_sh_safe_v2/manifest.json
```

`plan` 只固化任务，不自动消耗资源。检查 manifest 后，再从当前仓库提交实际计算。建议目录：

```text
runs/factors/<factor_id>/<run_id>/manifest.json
data/processed/factors/<factor_id>/<definition_version>/<run_id>/...
runs/experiments/<experiment_id>/<run_id>/manifest.json
results/experiments/<experiment_id>/<spec_version>/<run_id>/...
```

正式结果至少包含 `metadata.json`、质量统计、主结果、结论和必要的 `supersedes.json`。旧目录结果可用于 sanity check，但不要求完整复用，也不能作为新计算的隐式依赖。

## 旧编号迁移摘要

| 新编号 | 新实体 | 旧编号/名称 |
|---|---|---|
| F001 | 主动成交价格冲击 | ACTIVE_GAP、active_gap |
| F002 | 被动大价差流动性供给 | PASSIVE_LARGE_GAP |
| F003 | 订单行为强度与量比 | ORDER_BEHAVIOR、vr_log、cr_log |
| F004 | 价差供给与订单行为交互 | JOINT_LARGE_GAP_ORDER_BEHAVIOR |
| F005 | 委托撤单成交的中间价响应 | D01-D03 |
| F006 | 主动大单流与历史意外 | D04-D05、ID05 |
| F007 | 成交冲击的保留与恢复 | D07、SF46I07 |
| F008 | 统一事件链与报价生命周期原语 | BATCH01 |
| F009 | 订单形态基准原语 | M1-M6 |
| F010-F014 | 待定义/部分完成的日内冲击状态 | D06、D08-D11 |
| E001 | 沪深事件语义正确性闸门 | PB01、PB01-SH |
| E002-E005 | 四项受修复影响的复现/审计 | PB01-A/B/C/D、AG01 |
| E006 | 主动大单流 raw 检验 | ID05 |
| E007 | 公共事件 primitive 跨市场对照 | BATCH01 |
| E008-E012 | 机制深化与收益闸门 | ELOB、NP/NPE、DR、MSI、RG01 |

完整逐项别名以两个 JSON 注册表为准。
