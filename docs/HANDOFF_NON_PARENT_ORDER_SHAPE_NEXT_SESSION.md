# 非母单订单簿因子研究：下一 Session 交接

> 当前初筛执行口径已更新：固定 10:00–10:30、10:30 信号，只报告 raw 未中性化直接目标，不迁移风格暴露缓存。先读 [`HANDOFF_INTRADAY_STRUCTURE_PILOT.md`](HANDOFF_INTRADAY_STRUCTURE_PILOT.md)；下文中性化结果仅作为历史证据，不再是本轮继续门槛。

## 1. 任务范围

本交接用于后续 session 继续实现“剥离母单结构之后”的订单簿候选因子。明确排除：

- 同订单 ID 链方向；
- 单子链/多子链结构；
- 隐藏母单聚类；
- 执行生命周期、链节奏和频域母单研究。

普通主动流 M1 只作为基准和控制变量，不作为新的研究主题。母单方向另见 [`母单结构与执行阶段_衍生因子研究手册.md`](母单结构与执行阶段_衍生因子研究手册.md)。

本 session 的首要目标不是直接生产收益因子，而是利用现有 Batch A 缓存完成三个非母单方向的机制筛选和未来收益准备：

1. M6 成交机会惊奇；
2. 盘口—成交流背离；
3. 对称撤单强度与未来波动。

## 2. 必读

1. 仓库根目录 `AGENTS.md`；
2. `README.md` 的沪市 `FULL/PARTIAL` 和事件顺序说明；
3. `data/README.md` 的未来标签与缓存复用边界；
4. [`订单簿形态与非对称自刺激_增量因子手册.md`](订单簿形态与非对称自刺激_增量因子手册.md)；
5. [`order_shape_mechanism_v4_compute_audit.md`](order_shape_mechanism_v4_compute_audit.md)；
6. [`batch_a_direct_target_findings.md`](../results/intraday/order_shape_mechanism/batch_a_lob4_ex_size_and_nonlinear_202601_v1/batch_a_direct_target_findings.md)；
7. [`incremental_findings.md`](../results/intraday/order_shape_mechanism/batch_a_incremental_lob4_ex_size_and_nonlinear_202601_v1/incremental_findings.md)；
8. `/home/avery/.codex/skills/lob-domain-neutralization/SKILL.md` 及其 `references/protocol.md`。

## 3. 已完成实验与结论

样本为 2026 年 1 月 300 只按时点口径筛选的 A 股、20 个交易日、每日 21 个固定时点、九个预冻结市值 × 价格/板块域，ETF 为 0。

| 方向 | 已有结果 | 当前判断 |
|---|---|---|
| 普通主动流 M1 | 域中性未来 10 分钟订单流 IC 0.1412 | 强基准，仅作控制 |
| M6 成交机会差 | 原始 IC 0.0788；控制三次 M1 后 IC 0.0093、t=2.40 | 小幅、域依赖的探索性增量 |
| 盘口不平衡 | 预测未来盘口 IC 0.2272；预测未来主动流 IC -0.1536 | 盘口状态持续但主动流反向响应 |
| 带方向撤单差 | 预测未来实现波动 IC 0.2820 | 数值强但方向解释可疑，必须镜像 |
| M4 强度—深度 | 总体与“高强度薄簿”相反，低价非 STAR 例外 | 改为吸收/承接能力研究 |
| M5 热度—深度 | 总体 +0.048 log depth；5/9 域稳健 | 只能做域条件状态 |
| M2/M3 原方向 | 未复现，事件时钟下 M3 反向大幅缩小 | 不按原假设实现 |

这些结果只验证订单流、活跃度、波动和盘口直接目标，尚未验证未来收益。

## 4. 可复用输入

### 4.1 Batch A 信号缓存

```text
data/cache/order_shape_mechanism/batch_a_medium300_202601_v1/
```

关键文件：

```text
manifest.json
batch_*/signals.csv
batch_*/quality.csv
```

缓存包含 124,980 条完整信号、20 日 × 21 时点，原始 LOB 不需要重读。主字段：

| 类别 | 字段 |
|---|---|
| 标识 | `symbol`, `date`, `signal_seconds`, `signal_time` |
| 普通主动流 | `active_buy_volume`, `active_sell_volume`, `active_buy_count`, `active_sell_count`, `active_net_share` |
| 挂单成交机会 | `pred_fill_buy`, `pred_fill_sell`, `fill_history_buy`, `fill_history_sell` |
| 新增委托 | `aggressive_add_buy`, `aggressive_add_sell` |
| 近端撤单 | `near_cancel_buy`, `near_cancel_sell` |
| 盘口 | `spread_bps`, `bid_depth3`, `ask_depth3`, `book_imbalance3` |
| 未来直接目标 | `future_buy_volume`, `future_sell_volume`, `future_buy_count`, `future_sell_count`, `future_event_count`, `future_realized_vol_bps` |
| 未来盘口 | `end_spread_bps`, `end_bid_depth3`, `end_ask_depth3` |

### 4.2 字段语义陷阱

1. `aggressive_add_*` 和 `near_cancel_*` 是过去 60 秒的**量**，不是笔数。
2. `near_cancel_buy` 是买侧近端撤单量，`near_cancel_sell` 是卖侧近端撤单量。
3. 缓存字段 `fill_opportunity_diff` 定义为：

   ```text
   pred_fill_buy - pred_fill_sell
   ```

   但已回测的、正值对应未来主动买压的 `execution_pressure` 定义为：

   ```text
   pred_fill_sell - pred_fill_buy
   ```

   两者符号相反。后续必须显式生成 `execution_pressure`，不能直接把 `fill_opportunity_diff` 当成同方向因子。
4. 过去特征严格使用 `(t-60s,t)`，未来标签使用 `[t,t+10m)`；时点 `t` 的事件只进入未来标签。
5. M6 概率模型使用扩展的前一交易日及以前被动订单结果，当日订单在日终后才进入后续日期模型。
6. `future_realized_vol_bps` 是未来窗口中间价逐事件平方变化和的平方根，不是未来收益绝对值。

### 4.3 域与风格

```text
data/manifests/order_shape_medium300_domains_202601.csv
data/cache/cne5_style_full_202512_202601.csv
```

日内风格使用前一交易日暴露。当前用户固定主规格为：

```text
LOB4-ex-size-and-nonlinear-size
momentum, liquidity, beta, residual_volatility
```

`size` 和 `non_linear_size` 不进入回归，但必须输出原始 Rank 暴露。协议默认的 `LOB5-ex-size` 可作为单独敏感性结果，不能覆盖主规格。

## 5. 第一批候选因子

## NP01：M6 成交机会惊奇

### 原始定义

$$
ExecutionPressure_t=
P(\text{卖侧挂单成交})-
P(\text{买侧挂单成交})
$$

### 增量定义

在日期 × 时点 × 域内估计：

$$
NP01_t=
ExecutionPressure_t-
\widehat{ExecutionPressure}_t
(M1,M1^2,M1^3,Styles)
$$

第一版必须精确复现现有域中性订单流 IC 约 0.0093，再接收益标签。若复现失败，不继续调参。

### 直接目标

- 未来 10 分钟主动成交净占比；
- 未来主动成交量和事件数；
- 未来 1/5/10 分钟收益只作为第二阶段。

## NP02：总体可成交性

### 定义

$$
FillabilityLevel_t=
\frac{pred\_fill\_buy+pred\_fill\_sell}{2}
$$

以及 logit 均值稳健版本。它没有方向，主要预测未来活动、成交量、波动、点差和深度变化。

### 必要控制

- 当前主动成交总量和笔数；
- 买卖两侧历史样本数；
- 点差、三档总深度、日内时段和域；
- 前一交易日四风格。

## NP03：M1—M6 确认与冲突

保留连续交互和离散状态两种版本：

$$
Confirmation_t=M1_t\times ExecutionPressure_t
$$

离散状态至少包括：

- 两者同向且均为强；
- M1 弱、M6 强；
- M1 强、M6 反向；
- 两者均弱。

状态阈值使用历史或当期截面预注册分位数，不能按未来表现选择。优先检验“M1 强、M6 反向”是否对应订单流衰减或价格反转。

## NP04：盘口—成交流背离

### 基础变量

$$
BookImbalance3_t=
\frac{BidDepth3-AskDepth3}{BidDepth3+AskDepth3}
$$

不要预设盘口不平衡与未来主动流同号。现有结果恰好是盘口自身持续、主动流反向。

预注册以下两个版本：

1. 日期 × 时点 × 域内的 `M1` 与 `BookImbalance3` 二维状态；
2. 将未来主动流对盘口的历史关系作为基准，构造当前主动流相对盘口预期的残差。

候选状态包括：

- 主动买强且买盘厚；
- 主动买强且卖盘厚；
- 主动卖强且卖盘厚；
- 主动卖强且买盘厚。

先检验未来订单流、未来盘口和波动，再检验收益。

## NP05：对称撤单强度

### 定义

$$
CancelIntensity_t=
\frac{near\_cancel\_buy+near\_cancel\_sell}
{bid\_depth3+ask\_depth3+\varepsilon}
$$

$$
AbsCancelImbalance_t=
\frac{|near\_cancel\_sell-near\_cancel\_buy|}
{near\_cancel\_sell+near\_cancel\_buy+\varepsilon}
$$

同时保留买侧和卖侧深度归一化冲击：

$$
BuyCancelShock_t=
\frac{near\_cancel\_buy}{bid\_depth3+\varepsilon}
$$

卖侧对称。

### 目标与镜像闸门

主目标是未来实现波动、点差扩大和深度下降，不是涨跌方向。必须验证：

- 买卖交换后对称量保持不变；
- 带方向版本严格反号；
- 总量和方向差分别进入模型，避免把撤单总量误解释成方向；
- 九域均报告，不接受单域 headline。

## 6. 第二批候选：需要新增一次投影读取

现有 Batch A 缓存没有完整保存过去窗口中间价路径、全部事件到达数和深度恢复轨迹。以下方向需要新的一次性投影读取，但应在第一批完成后再做。

| 因子族 | 需要新增的过去窗口原始量 | 主要目标 |
|---|---|---|
| 流动性吸收 | 过去中间价变化、主动量、深度损失 | 后续冲击延续/反转 |
| 冲击效率 | 单位主动量价格变化、穿透档位 | 价格脆弱性 |
| 历史恢复能力 | 已完成冲击后的补单和恢复事件数 | 点差、深度、成本 |
| 日内热度惊奇 | 截至时点事件数、成交量及历史同时段基线 | 活跃度、波动 |
| 时钟差异 | 秒时钟和事件时钟的同一状态量 | 活动压缩与波动 |

推荐目录：

```text
scripts/factors/order_shape_non_parent/
data/cache/order_shape_non_parent/
results/intraday/order_shape_non_parent/
tests/factors/order_shape_non_parent/
```

每个 stock-month 在同一版本中最多读取一次，并在一次扫描内输出全部第二批共享原始量。

## 7. 回测口径

### 7.1 主证据顺序

1. 九域逐一报告；
2. 域内中性化、域内排名后合并；
3. 未分域全市场四风格中性结果；
4. 原始全市场与风格暴露诊断。

不得只挑成功域合并。域中性和全市场异号时，以九域结构为主要解释，并明确该信号是域依赖候选。

### 7.2 时间与标签

- 固定信号时点先于所有未来标签；
- 每个未来期限独立处理缺失标签，不预先取交集；
- 日内收益必须明确 entry quote/mid、exit quote/mid 和是否可成交；
- 相邻 10 分钟标签不得当作大量独立样本，先在日内聚合再按日期推断；
- 收益标签未接入前，不得将订单流或波动 IC 称为 alpha。

### 7.3 最低输出

- `performance_by_slice.csv`；
- `performance_summary.csv`；
- `exposure_by_slice.csv`；
- `exposure_summary.csv`；
- `manifest.json`；
- 中文结果报告。

manifest 至少记录 universe 规则、ETF 数、输入路径和哈希、特征/标签边界、风格规格、域规则、缺失标签政策和因子版本。

## 8. 验证顺序

1. 编译和纯合成测试；
2. 验证 M6 符号：`execution_pressure = pred_fill_sell - pred_fill_buy`；
3. 验证撤单字段是量而非笔数；
4. 验证 09:40 边界事件只进入未来标签；
5. 验证风格使用前一交易日；
6. 验证 ETF 为 0；
7. 相同小样本串行与并行逐字段比较；
8. 第一批只读缓存全量计算；
9. 审计九域、21 时点和20日期覆盖；
10. 用户审阅后再接收益或启动第二批 LOB 读取。

## 9. 可复用代码与结果

### 9.1 代码

- [`batch_a_engine.py`](../scripts/factors/order_shape_mechanism/batch_a_engine.py)：固定时点与缓存字段定义；
- [`reproduce_batch_a_v4.py`](../scripts/factors/order_shape_mechanism/reproduce_batch_a_v4.py)：单扫描、分片、断点续跑；
- [`backtest_order_shape_batch_a_domains.py`](../scripts/backtests/backtest_order_shape_batch_a_domains.py)：四风格九域直接目标回测；
- [`backtest_order_shape_batch_a_incremental.py`](../scripts/backtests/backtest_order_shape_batch_a_incremental.py)：控制 M1 的条件增量回测；
- [`test_batch_a_engine.py`](../tests/factors/order_shape_mechanism/test_batch_a_engine.py)：时间边界测试；
- [`test_order_shape_batch_a_domains.py`](../tests/backtests/test_order_shape_batch_a_domains.py)；
- [`test_order_shape_batch_a_incremental.py`](../tests/backtests/test_order_shape_batch_a_incremental.py)。

### 9.2 结果

- [`mechanism_findings.md`](../results/intraday/order_shape_mechanism/medium300_202601_v1/mechanism_findings.md)；
- [`batch_a_direct_target_findings.md`](../results/intraday/order_shape_mechanism/batch_a_lob4_ex_size_and_nonlinear_202601_v1/batch_a_direct_target_findings.md)；
- [`incremental_findings.md`](../results/intraday/order_shape_mechanism/batch_a_incremental_lob4_ex_size_and_nonlinear_202601_v1/incremental_findings.md)。

## 10. 建议实现顺序

### Phase A：不重读 LOB

1. 新增一个只读 Batch A 缓存的非母单分析脚本；
2. 实现 NP01--NP05 的原始量和预注册版本；
3. 首先复现已知结果和符号；
4. 对每个候选做四风格中性、M1 线性和 M1 三次控制；
5. 输出订单流、活动、波动和盘口直接目标；
6. 形成继续/停止清单。

建议脚本：

```text
scripts/backtests/backtest_order_shape_non_parent_batch_b.py
```

建议输出：

```text
results/intraday/order_shape_non_parent/batch_b_medium300_202601_v1/
```

### Phase B：收益标签

仅将 Phase A 通过的候选接入未来 1/5/10 分钟收益。Batch A 缓存不包含收益标签，必须先审计可用分钟行情或从 V4 构造严格时点后的中间价/可成交报价。不得假设仓库中某个既有分钟缓存覆盖全部 21 个信号时点。

### Phase C：新增一次 LOB 投影读取

只有吸收、恢复、热度和时钟差异确有必要时，才新增单扫描原始量计算器。提交中等或全市场任务前，先完成单文件、约 12 只股票、小样本串并行一致性和 I/O 预计。

## 11. 继续与停止标准

| 候选 | 继续标准 | 停止或降级条件 |
|---|---|---|
| NP01 M6 惊奇 | 复现约 0.009 的域中性订单流增量，跨月多数域同号 | 仅单域、全市场与域内长期冲突且跨月不稳 |
| NP02 可成交性 | 控制当前活跃度后仍预测未来活动/波动 | 只复制当前成交量和点差 |
| NP03 确认/冲突 | 状态差跨域且对收益或衰减有清晰排序 | 依赖目标月阈值或样本过少 |
| NP04 盘口流背离 | 同时解释未来盘口与订单流，收益方向样本外稳定 | 只是盘口不平衡的重标度 |
| NP05 撤单强度 | 对称版本跨域预测波动，镜像测试通过 | 只有带方向旧版本有效 |

一个月显著只能决定“值得继续”，不能确认生产因子。任何收益结论至少需要跨月样本、预注册方向、多重检验说明和成本后结果。

## 12. 下一 Session 可直接使用的任务

> 阅读 `docs/HANDOFF_NON_PARENT_ORDER_SHAPE_NEXT_SESSION.md`、仓库 `AGENTS.md` 及其中列出的必读文档。严格排除母单链和隐藏母单变量，只复用 `batch_a_medium300_202601_v1` 信号缓存实现 NP01--NP05。先核对字段方向，尤其是 `execution_pressure = pred_fill_sell - pred_fill_buy`，并确认撤单字段为过去60秒撤单量。主规格沿用 `LOB4-ex-size-and-nonlinear-size`，日内使用前一交易日四风格；分别报告九域、域中性汇总和全市场诊断。先完成合成测试、时间边界、ETF为零和输出schema检查，再跑300只股票直接目标实验。不要重读LOB，不要接收益标签，直到直接目标结果和继续/停止清单经审阅。
