# F018与非母单收益深化：下一 Session 交接

> 更新日期：2026-08-11。本文供新的研究 session 直接接手。目标是复用已审计的全市场产物，优先回答尚未解决的实证问题，不重复扫描V4，不重复已经完成的实验。

## 0. 一分钟启动

```bash
cd /home/avery/lob-research-1
git status -sb
sed -n '1,280p' docs/HANDOFF_F018_NEXT_RESEARCH_SESSION.md
sed -n '1,260p' research/candidate_factors/F018_minus_flow_to_opponent_depth/README.md
```

检查后台任务：

```bash
ps -eo pid,etime,stat,rss,cmd | rg '[p]ython .*order_shape|[p]ython .*f018|[c]lickhouse-client'
```

交接时状态：

- 分支：`main`；
- 已推送基线提交：`b653e9c Add audited non-parent factor research`；
- 该提交已通过`168 passed`；
- 没有正在运行的F018或非母单计算任务；
- 新运行必须创建新run ID，不覆盖任何现有结果、缓存或manifest。

## 1. 当前决策

F018“对手盘深度归一化主动流反转”是**单月条件型候选因子**，尚未晋级生产候选。

- 原始F018是所有版本必须报告的基线；
- 当前连续复合候选主版本是`F018-CL = B × L`；
- 最低流动性三分位压零的连续软阈值已经检验并降级，不替代`B × L`；
- 现有证据只有2026年1月，不能宣称跨月稳定；
- 用户当前暂不做跨月，跨月任务必须再次获得明确授权；
- 不要继续在2026年1月搜索阈值、幂次、方向或成功分域。

用户已把以下问题留给后续确认：

> 为什么30分钟Rank IC低于10/15分钟，但D10-D1更高？它是否是“极端分位延迟反转”，而不是全截面排序能力？

当前只有Rank IC和D10-D1，还没有5/10/15/30分钟的完整十档收益曲线。因此，“尾部延迟反转”只是待检验假说，不是既成结论。下一批首项实验应该直接画完整期限十档曲线，不要先发明新的复合公式。

## 2. 冻结定义

### 2.1 原始F018

信号窗口：`[10:25:00,10:30:00)`；信号截止：10:30。盘口使用最近5分钟有效前三档的持续时间加权均值。缺失、锁盘、交叉盘不能覆盖最近有效盘口。

```text
direction = sign(ActiveBuyVolume5m - ActiveSellVolume5m)

direction > 0:
    F018 = -log1p(ActiveBuyVolume5m / AskDepth3TWAP5m)

direction < 0:
    F018 = +log1p(ActiveSellVolume5m / BidDepth3TWAP5m)

direction = 0:
    F018 = 0
```

高值表示卖压相对买盘承接过强，预测随后反弹；低值表示买压相对卖盘承接过强，预测随后回落。

### 2.2 当前连续复合候选

在每个“交易日×冻结九分域”内：

```text
B = 2 × PctRank(F018) - 1
L = PctRank(equal_weight(tight_spread, depth3, active_volume, active_order_count))
F018-CL = B × L
```

流动性质量`L`只改变F018方向的置信强度，不单独提供多空方向。

### 2.3 收益标签

- 信号截止：10:30:00；
- 入场：10:31分钟收盘价；
- 退出：10:35、10:40、10:45、11:00和15:00分钟收盘价；
- 每个期限独立处理标签缺失，不能要求其他未来期限同时存在；
- 10:45标签已单独建立，100,837个股票日全部匹配。

## 3. 已完成证据：直接复用，不要重算

### 3.1 原始F018期限衰减

主结果为raw、未中性化、冻结九分域内逐日排名后聚合。

| 预测区间 | Rank IC | IC t值 | D10-D1 | 尾差t值 | IC正日期占比 |
|---|---:|---:|---:|---:|---:|
| 10:31–10:35 | 0.0314 | 2.60 | 0.81bp | 0.61 | — |
| 10:31–10:40 | 0.0396 | 2.57 | 2.51bp | 1.11 | 80% |
| 10:31–10:45 | 0.0315 | 1.96 | 2.70bp | 0.95 | 65% |
| 10:31–11:00 | 0.0232 | 2.15 | 4.28bp | 1.36 | — |
| 10:31–15:00 | 0.0113 | 0.74 | 2.53bp | 0.36 | — |

正确解读：全截面排序在10分钟左右最稳定；15分钟仍为正但已经衰减；30分钟IC继续下降而尾差点估计扩大。30分钟D10-D1的t值只有1.36，不能仅凭4.28bp认定稳定尾部alpha。

### 3.2 流动性条件启用

冻结高流动性三分位中，原始F018的5/10/30分钟IC为`0.0384 / 0.0497 / 0.0399`。30分钟高减低状态IC差约0.0399，逐日配对`t=3.90`。这说明流动性是有效条件变量，但高流动性组三十分钟D10-D1约8.50bp、`t=1.64`，仍不是成熟交易策略。

### 3.3 连续复合

| 版本 | 5分钟IC | 10分钟IC | 30分钟IC | 30分钟D10-D1 |
|---|---:|---:|---:|---:|
| 原始F018 | 0.0314 | 0.0396 | 0.0232 | 4.28bp，t=1.36 |
| 当前`B×L` | 0.0309 | 0.0400 | 0.0242 | 5.66bp，t=1.78 |
| `B×L²` | 0.0300 | 0.0395 | 0.0240 | 5.10bp，t=1.64 |
| `B×(0.25+0.75L)` | 0.0313 | 0.0401 | 0.0243 | 5.74bp，t=1.76 |

`B×L`只获得小幅、不显著的IC增量，但尾差有所改善，因此登记为组合工程候选，不能宣传为显著alpha升级。

### 3.4 连续软阈值

冻结规格为`t = clip((L - 1/3) / (2/3), 0, 1)`，主版本`B × t`，稳健版本`B × t² × (3 - 2t)`。最低33.39%样本被压成零。软折线5/10/30分钟IC为0.0295/0.0401/0.0253，30分钟D10-D1降至4.92bp。结论是**不采用**，不得继续用本月数据调阈值。

### 3.5 增量控制

- 完整控制残差5/10/30分钟IC约0.0077/0.0108/0.0143；
- 原始强度约65%可被Flow5、前收益和日内状态解释；
- 仍存在小幅连续排序增量，但极端分组收益不够强；
- F018更适合条件化连续权重，而不是独立尾部交易因子。

### 3.6 买卖侧和涨跌停

- 卖压后反弹5分钟D10-D1约+5.46bp，`t=2.31`；
- 买压后回落明显较弱，但双边连续因子仍是主版本；
- 排除信号、入场、退出及持有期涨跌停影响后，卖压反弹结论仍保留；
- 不支持仅包装卖压反弹的单边生产因子。

## 4. 下一批首项实验：完整十档期限形状

### 4.1 可证伪问题

1. 30分钟较高D10-D1是否来自D1和D10继续分离？
2. D2–D9是否随期限变平或失去单调性，从而解释Rank IC下降？
3. 这一形状是否只来自少数日期、少数九分域或卖压分支？

### 4.2 冻结规格

主因子只测试原始F018和当前连续候选`B×L`。期限固定为5/10/15/30分钟，10:31入场，10:35/10:40/10:45/11:00退出。不得新增阈值、幂次、单域公式或方向翻转。

建议新产物：

```text
scripts/backtests/analyze_f018_horizon_decile_shape.py
tests/backtests/test_f018_horizon_decile_shape.py
runs/research/R017/f018_horizon_decile_shape_full_market_202601_v1/
results/research/R017/f018_horizon_decile_shape_full_market_202601_v1/
```

最低输出：

- 每日×九分域×期限×因子×D1–D10的等权平均收益；
- 九域排名聚合后的完整十档曲线；
- 每档相对全样本平均收益；
- D10-D1、D10-D9、D2-D1；
- 档位与平均收益的Spearman单调性；
- 每日D10-D1及累计贡献，检查少数日期驱动；
- 九个分域完整结果；
- 买压/卖压分支仅作冻结辅助拆解；
- 原始F018与`B×L`并排报告。

### 4.3 判定规则

只有同时看到以下结构，才可称为“尾部延迟反转”：

1. 30分钟D10-D1高于10/15分钟；
2. 增量主要来自D1或D10，而不是中间档随机跳动；
3. 逐日尾差不是一两个交易日贡献；
4. 至少多个预冻结域同向，不能只由一个域决定；
5. 原始F018和`B×L`方向一致。

如果十档整体仍单调，只是收益噪声扩大，应解释为“弱化的全截面效应”。如果只有单个尾档异常，应记录为探索性尾部效应，不升级候选。

## 5. 后续课题优先级

| 优先级 | 课题 | 启动条件 |
|---:|---|---|
| 1 | 5/10/15/30分钟完整十档曲线 | 可立即使用现成输入 |
| 2 | 尾部日贡献和九域稳定性 | 与优先级1同次完成 |
| 3 | 换手、入场价和简单成本诊断 | 尾部形状通过后 |
| 4 | 高流动性条件下十档曲线 | 主规格完成后作为预注册辅助结果 |
| 5 | 跨月复现 | 当前暂不做；需用户再次授权 |

不要优先做新阈值搜索、继续调整`L`幂次、按成功分域设计公式，或在十档形状确认前训练复杂模型。

## 6. 可直接复用的输入

完成审计的因子产物：

```text
runs/factors/F014/non_parent_window_path_5m_30m_full_market_202601_v1/completion.json
runs/factors/F014/non_parent_candidates_fixed_1030_full_market_202601_v3_lineage/completion.json
data/processed/order_shape_non_parent/window_path_5m_30m_full_market_202601_v1/window_paths.parquet
data/processed/order_shape_non_parent/f014_fixed_1030_full_market_202601_v2/candidates.csv
```

质量收据：

```text
audits/Q008/q008_non_parent_window_path_5m_30m_full_market_202601_v1/preflight_receipt.json
```

该收据证明点时A股白名单、沪深覆盖、Q001–Q008和`ETF=0`。

收益标签：

```text
data/cache/order_shape_non_parent/fixed_1030_minute_returns_full_market_202601_v1/minute_prices.csv
data/cache/order_shape_non_parent/fixed_1030_minute_returns_full_market_202601_v1/manifest.json
data/cache/order_shape_non_parent/fixed_1030_minute_returns_15m_full_market_202601_v1/minute_prices_1031_1045.csv
data/cache/order_shape_non_parent/fixed_1030_minute_returns_15m_full_market_202601_v1/manifest.json
```

15分钟缓存先固定100,837个信号股票日，再按10:45价格自身可用性匹配，本月无缺失。

## 7. 代码与证据索引

权威档案：

```text
research/candidate_factors/F018_minus_flow_to_opponent_depth/README.md
research/candidate_factors/F018_minus_flow_to_opponent_depth/factor_spec.json
docs/CANDIDATE_FACTOR_RESEARCH_BUFFER.md
```

关键实现：

```text
scripts/backtests/analyze_f018_incremental_controls.py
scripts/backtests/analyze_f018_liquidity_conditioning.py
scripts/backtests/analyze_f018_continuous_liquidity_composite.py
scripts/backtests/analyze_f018_continuous_soft_threshold.py
scripts/backtests/analyze_f018_raw_15m_horizon.py
scripts/backtests/backtest_non_parent_direct_targets.py
```

关键结果目录：

```text
results/research/R017/f018_incremental_controls_full_market_202601_v1/
results/research/R017/f018_liquidity_conditioning_full_market_202601_v1/
results/research/R017/f018_continuous_liquidity_composite_full_market_202601_v1/
results/research/R017/f018_continuous_soft_threshold_full_market_202601_v1/
results/research/R017/f018_raw_15m_horizon_full_market_202601_v1/
results/research/R017/flow_to_opponent_depth_buy_sell_symmetry_5m_full_market_202601_v1/
results/research/R017/sell_pressure_reversal_5m_limit_impact_full_market_202601_v1/
```

优先读取各目录的`performance_summary.csv`，需要诊断时再读取`performance_by_slice.csv`。

## 8. 快速复核命令

```bash
rg '^domain_rank_aggregate,all_nine_domains' results/research/R017/f018_raw_15m_horizon_full_market_202601_v1/performance_summary.csv
rg '^domain_rank_aggregate,all_nine_domains' results/research/R017/f018_continuous_liquidity_composite_full_market_202601_v1/performance_summary.csv
```

测试：

```bash
conda_lob/bin/python -m pytest -q --import-mode=importlib \
  tests/backtests/test_f018_incremental_controls.py \
  tests/backtests/test_f018_liquidity_conditioning.py \
  tests/backtests/test_f018_continuous_liquidity_composite.py \
  tests/backtests/test_f018_continuous_soft_threshold.py \
  tests/backtests/test_f018_raw_15m_horizon.py
```

画十档曲线不需要重新运行F014全市场V4扫描，直接读取已完成审计的窗口产物和标签缓存。

## 9. 强制边界

1. 主结果必须是raw、未中性化、冻结九分域；全市场和沪深只作诊断。
2. 九个分域全部报告，不能只合并成功域。
3. 股票池在读取未来标签前固定，ETF必须为0。
4. 每个收益期限独立处理标签缺失，不能预先取共同非空样本。
5. 不能用10:30后的价格、成交或盘口构造信号。
6. 不允许用2026年1月继续选择阈值、幂次、方向或成功分域。
7. V4相关工作先读`.agents/skills/v4-lob-sh-sz-semantics/SKILL.md`，并做沪深真实文件审计。
8. 不得使用`source_link_status`、FULL/PARTIAL或未来recid链接作为点时特征。
9. 新输出、缓存和manifest使用新ID并拒绝覆盖已有路径。
10. 尾差必须结合t值、每日贡献、成本和容量解释。

## 10. 不要混入本项的课题

- R018撤单课题仍为**暂缓**：深圳V4撤单行`source_price=0`，必须先回溯挂单状态恢复原价；旧结果作废。
- 科创板做市商解释目前只使用代理变量，不能当作已确认的因果机制。
- 母单结构R021–R025是独立研究线；F018可以作主动流基准，但期限十档实验不应混入母单结构变量。
- `FlowMinusBook`固定残差、Flow5×承接能力切换、补单吸收和软阈值均已降级，重启条件见研究缓冲区。

## 11. 下一 Session 完成标准

- 新的append-only R017 run manifest；
- 原始F018与`B×L`的5/10/15/30分钟完整十档表；
- 九域聚合、九域逐一和沪深辅助诊断；
- 每日尾差贡献与异常日期清单；
- 在“尾部延迟反转”“弱化的全截面效应”“少数样本噪声”之间作明确选择；
- 代码测试、JSON校验和无未来信息检查通过；
- 更新F018候选档案和机器规格；
- 如果不支持尾部效应，明确停止，不继续调档宽或阈值。

完成后再由用户决定是否进入成本/换手、高流动性条件十档或跨月复现。
