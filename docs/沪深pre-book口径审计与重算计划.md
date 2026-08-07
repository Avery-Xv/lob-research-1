# 沪深 pre-book 与主动余量口径审计及重算计划

更新日期：2026-08-07

状态：PRECOMPUTE_AUDIT_DONE。沪深主动单发布顺序、剩余委托、方向联合键、真实样本串并行确定性以及任务门禁已经完成；正式全市场因子与研究实验重算尚未提交。

正式审计证据：

- `audits/Q003/q003_202601_12x3_v2/summary.json`：6 沪+6 深、3 个交易日，PASS。
- `audits/Q003/q003_202601_12x3_v2/raw_traces_20x2.csv`：沪深各 20 个原始订单链。
- `audits/Q006/q006_202601_shsz_v1/summary.json`：order_behavior、passive_large_gap、joint_large_gap 真实样本串行/两进程结果一致。
- `audits/Q003/q003_202601_12x3_v2/preflight_receipt.json`：Q001-Q008 PASS，并认证两套 ETF=0 股票清单。
- v1 失败证据保留在 `audits/Q003/q003_202601_12x3_v1/`；失败仅因“样本必须出现同号异向数字 ID”这一不合理条件，v2 将键安全交由强制回归测试验证。

深市已完成：

- 期间与窗口：2026-01～02、10:00–10:30。
- 范围：5,764 个深市 A 股 stock-month，ETF=0。
- 版本：experiment_batch_1_v2_safe_prebook_20260807。
- 输出：data/cache/experiment_batch_1/intraday_1000_1030_202601_202602_sz_v2_safe_prebook/。
- 日志：results/intraday/experiment_batch_1/rerun_sz_v2_202601_202602.log。
- 进度：5,764/5,764 个 stock-month，23,056 个分片 CSV。
- 第一层结果：results/intraday/experiment_batch_1/mechanism_analysis_202601_202602_sz_v2_safe_prebook/findings.md。
- 最终特征：97,776 个完整股票日、2,884 只股票、34 个交易日，ETF=0。
- atomic chain：249,488,596 条，1,579 条歧义链和 360 条未解析链被保守排除。

## 1. 背景与目标

experiment_batch_1 第一层分析发现：

- 上海样本的盘口冲击方向基本符合定义。
- 上海可成交委托常按 TRADE(s) 后再发布未成交余量 ORDER_ADD。Batch01 V1
  未排除同一主动订单的后置余量，将其误计为独立被动新增挡位。
- 6 个上海股票月的小样本对照中，V1 的 15,211 条新增挡位有 7,054 条
  被识别为主动订单余量并排除，占 46.4%；同时 258,918 条主动订单链和
  120 行非报价股票日字段在 V1/V2 间完全一致。
- 深圳约 36% 的事件落在缺失、锁定或交叉盘口状态；该比例不能直接解释为全部都是临时交叉盘口，但现有审计表明，V4 深圳可成交委托展开为委托与子成交事件时，会产生较多临时无效的 post-book 快照。
- 如果因子直接使用原始相邻行的 lag(bid)、lag(ask) 或 lag(mid)，临时无效行可能覆盖真实的事前盘口，造成路径错配、样本遗漏或交易所间方向差异。

本轮包含两条独立修复线：

1. 深圳修复临时无效 post-book 覆盖真实 pre-book 的路径问题。
2. 上海修复主动成交后发布的 ORDER_ADD 余量被重复解释为独立被动委托的问题。

目标不是推翻全部既有日内结果，而是统一审计依赖相邻盘口路径或
ORDER_ADD 被动性解释的因子。修复后仅在差异达到预先登记的触发条件时，
继续重算其下游组合和回测。

## 2. 统一修正版口径

所有纳入本计划的脚本按以下规则实现和验证：

1. 使用点时点 A 股股票清单生成输入，因子计算前排除 ETF；最终产物验证 ETF 数量为 0。
2. 保留源文件事件顺序，不为得到更平滑的盘口而重排事件。
3. 分交易日、证券和交易时段维护 last_valid_book。
4. 路径型因子的有效盘口定义为 bid > 0 且 ask > bid；缺失、锁定和交叉状态分别计数并输出质量指标。
5. 临时无效盘口不得覆盖 last_valid_book。下一次需要事前盘口时，使用最近的有效且非交叉盘口。
6. 午间、隔日和无连续状态的边界必须重置，不允许跨时段沿用盘口。
7. 深圳另做 atomic-chain 稳健性版本：从一段临时无效事件链之前的最后有效盘口，比较到事件链之后的第一个有效盘口。
8. 上海按 TRADE(s) 后发布 ORDER_ADD 余量的原始顺序处理，不重排。对同一
   (side, active_order_id)，已经出现主动成交的后置 ORDER_ADD 不得再计为
   独立被动新增挡位或被动提交量。
9. 上海原始提交量按“前置立即成交量 + 后置余量”还原；完全立即成交订单
   可以没有 ORDER_ADD。FULL/PARTIAL 和前向 recid 链接不得作为点时特征。
10. genuine passive add、aggressive-order remainder 和未分类 ORDER_ADD
    必须分别计数；数量切片口径只能作为单独标注的敏感性版本。
11. 因子只能使用信号时点已经可见的信息；不得用未来收益窗口筛选样本。
12. 主报告默认不做中性化。任何中性化结果只能作为单独标注的二级稳健性结果。
13. 上海、深圳先分别报告。只有在修复后定义一致、方向一致且质量指标通过时，才给出合并结果。

## 3. 必须重算的范围

### PB01-A：active_gap / active_take_midprice

涉及脚本：

- scripts/factors/active_take_midprice/intraday_window_factor.py
- scripts/factors/active_take_midprice/active_take_midprice_ratio_v3.py
- scripts/factors/active_take_midprice/daily_factor_v4_1000_close.py

原因：现有实现直接读取原始相邻行的 lag(mid/bid/ask)，最容易受临时无效盘口覆盖影响。

重算内容：

- old_active_abs、old_active_ratio、old_active_signed。
- 10 分钟、15 分钟预测期的原始非中性化结果。
- 极端组宽截断版本。
- 先重算 2026-01 至 2026-02、10:00–10:30，与 experiment_batch_1 对齐。
- 若达到第 6 节的“实质变化”条件，再扩展到 AG01 原计划的 2026-01 至 2026-04，并替换相关汇总。

### PB01-B：stylized-fact-4-6 D01–D03

涉及脚本：

- scripts/factors/stylized_fact_4_6/reproduce_d01_d03.py

原因：

- 深圳：当前逻辑会把无效盘口置空，但 raw lag 不会保存最近有效事前盘口；
  事件链中的状态转换可能被遗漏或错配。
- 上海：D01–D03 将所有 ORDER_ADD 归入委托冲击。数值仍对应数据事件类型，
  但若解释为独立被动委托冲击，会混入主动订单余量。

重算内容：

- D01、D02、D03 的事件统计、分组结果和日内因子版本。
- 上海 ORDER_ADD 冲击拆分为 genuine passive add、aggressive-order remainder
  和未分类三类；同时保留全 ORDER_ADD 机械事件口径用于与旧结果对照。
- 2026-01 至 2026-02、10:00–10:30。
- 沪深分别输出事件覆盖、无效状态占比、方向和原始非中性化表现。

### PB01-C：stylized-fact-4-6 D07

涉及脚本：

- scripts/factors/stylized_fact_4_6/reproduce_d07.py

原因：当前实现直接使用 lag(mid)，并要求交易事件盘口有效，可能在深圳形成路径错配和选择偏差。

重算内容：

- 保留原 D07 定义和已有预测期，新增 safe pre-book 版本。
- 同时报告逐事件版本和 atomic-chain 稳健性版本。
- 沪深分开给出样本保留率、方向、分位数组合和原始非中性化收益。

### PB01-SH：experiment_batch_1 上海报价生命周期

涉及脚本：

- scripts/factors/experiment_batch_1/engine.py
- scripts/factors/experiment_batch_1/analyze_mechanisms.py
- scripts/factors/order_shape_mechanism/m1_quote_engine.py
- scripts/factors/order_shape_mechanism/batch_a_engine.py

原因：Batch01 V1 将上海主动订单成交后发布的余量 ORDER_ADD 识别为被动新增
挡位。6 个上海股票月中有 46.4% 的旧新增挡位被修正版排除，已达到直接重算
条件，不再等待第 6 节阈值判断。

重算内容：

- experiment_batch_1 上海 2026-01 至 2026-02 的 4,607 个 stock-month。
- 重建 quote_lifecycles、被动改善量、相对深度、存活、再命中、成交移除和
  恢复原盘口指标。
- 保留主动订单链和非报价字段的 V1/V2 一致性核对；若这些字段发生变化，
  视为实现异常而不是预期修复。
- 重做上海第一层机制报告，再与完成后的深市 safe-prebook 结果分别报告。
- M1-Q 的 add_total、add_aggressive、passive_adds 重新计算。
- Batch A 的 quote_aggressive_net 字段重新计算；其余字段先做一致性审计。
- NP01–NP05 只有在实际读取 quote_aggressive_net 时才重跑；按当前预注册定义
  使用的 M6、盘口、成交和撤单字段不自动重跑。

## 4. 先审计、再决定是否全量重算

### PB01-D：passive_large_gap 与 joint_large_gap

涉及脚本：

- scripts/factors/passive_large_gap_ratio/intraday_window_factor.py
- scripts/factors/passive_large_gap_ratio/passive_large_gap_ratio.py
- scripts/factors/joint_large_gap_order_behavior/compute_v4.py

判断：这些因子主要识别稳定盘口中的被动挂单，风险低于 active_gap；但其委托进入时仍可能读取到深圳临时无效的上一行盘口。上海主动余量通常产生负的 initial_gap，不进入大正 gap 核心分子，因此预计对核心因子影响较低；匹配率和订单分类诊断仍需核对。

执行方式：

1. 先在小样本和 2026-01 至 2026-02上同时计算旧口径与 safe pre-book 口径。
2. 若未达到实质变化条件，保留旧结果并附审计说明。
3. 若达到实质变化条件，重算 passive_large_gap、joint_large_gap 以及直接依赖它们的回测。

### PB01-E：large_gap 与 vr_log 的组合及尾部实验

以下项目不先于基础因子审计执行：

- large_gap_* 与 vr_log 的联合使用。
- 极端组宽截断、match 过滤和其他尾部稳健性。
- 使用 passive_large_gap 或 joint_large_gap 缓存的派生因子和回测。

只有 PB01-D 触发基础因子替换时，才重跑这些下游项目；否则沿用旧结果并登记依赖版本。

## 5. 本次问题无需重算的项目

以下项目不依赖错误的相邻盘口路径，也不把上海主动余量重新当作独立被动
委托，因此不自动重算：

- D04/D05 的订单流版本，包含 scripts/factors/stylized_fact_4_6/reproduce_intraday_d05.py。
- order_behavior_ratio 的纯订单笔数、数量和订单 ID 统计。
- M1–M6 主动成交链主干及日终排除 active_order_id 后的被动成交模型。
- Batch A 中除 quote_aggressive_net 外的主动流、成交链、盘口、撤单和被动
  成交模型字段；quote_aggressive_net 已归入 PB01-SH。
- NP01–NP05 当前预注册定义使用的 M6、盘口、成交和撤单字段；若实现阶段
  引用 quote_aggressive_net，则对应候选改为重算。
- 分钟收益标签和预测期收益标签。

D06 当前不列入立即重算范围；若后续确认其输入引用了被替换的 D04 产物或响应变量定义发生变化，再单独登记。

## 6. 旧口径是否失效的预登记判据

以下阈值作为建议默认值，在正式运行前固定，不根据结果回改。按交易所和月份分别比较旧口径与 safe pre-book 口径：

- 股票截面 Rank IC 或因子秩相关低于 0.99。
- 分位数组别一致率低于 95%。
- 顶部或底部 10% 股票集合的 Jaccard 相似度低于 0.90。
- 有效股票日覆盖率变化超过 2 个百分点。
- 原始非中性化多空收益或极端组差值改变超过 20%，或符号发生变化。

上海报价生命周期已由小样本确认 46.4% 的旧候选属于主动余量，因此
PB01-SH 自动触发全量重算，不再用上述阈值决定是否启动。其验收重点为：

- 主动订单链数量、成交量、拆单字段和非报价股票日字段应与 V1 一致。
- genuine passive add 数量必须等于全部 ORDER_ADD 候选扣除主动余量和未分类项。
- quote-lifecycle 的深度、再命中、恢复和移除统计必须重新生成，不沿用 V1。

任一核心指标触发即定义为“实质变化”：

- 对必重算项目，safe pre-book 版本成为新的主口径，并重跑直接下游。
- 对条件审计项目，转为全量重算。
- 未触发时，保留旧结果，但在元数据中记录已通过 pre-book 审计。

## 7. 执行阶段

### 阶段 0：实现与测试

- 抽出可复用的有效事前盘口状态机。
- 增加合成深圳 ORDER_ADD 与子 TRADE 临时交叉事件链测试。
- 增加合成上海 TRADE(s) 后发布 ORDER_ADD 余量的测试，验证余量不进入
  被动新增挡位和被动提交量。
- 验证无效行不覆盖 last_valid_book。
- 验证午间、隔日、10:30 边界重置和预测期截尾。
- 验证串行与并行小样本输出完全一致。

### 阶段 1：小量验证

- 上海 6 只、深圳 6 只股票，至少覆盖 3 个交易日。
- 同时输出逐事件匹配明细、旧口径、safe pre-book 和 atomic-chain 版本。
- 人工复核至少 20 条深圳临时无效盘口事件链。
- 人工复核至少 20 条上海 TRADE(s) 后发布余量 ORDER_ADD 的事件链。
- 比较上海全部 ORDER_ADD、主动余量、genuine passive add 三类数量及盘口效果。
- 确认股票清单中 ETF 数量为 0。

### 阶段 2：对齐批次审计

- 期间：2026-01 至 2026-02。
- 因子窗口：10:00–10:30。
- 主结果：原始、非中性化。
- 沪深分别分析，再判断是否适合合并。
- 深市当前任务完成后，启动上海 4,607 个 stock-month 的 Batch01 报价生命
  周期修正版；两个交易所均完成后再重做第一层报告。

### 阶段 3：条件扩展

- active_gap 若发生实质变化，扩展到 2026-01 至 2026-04，并重做 AG01 对应预测期、尾部和宽截断报告。
- passive/joint large_gap 若发生实质变化，重算 large_gap × vr_log 及相关派生实验。
- 上海 Batch01 报价修复为必做项，不受条件扩展限制；M1-Q 和 Batch A
  quote_aggressive_net 随其一并审计。
- 其他未触发项目不扩大计算范围。

## 8. 输出与版本管理

新结果不得覆盖旧产物。建议统一输出到：

- data/processed/prebook_rerun/
- results/intraday/prebook_rerun/

每项至少包含：

- metadata.json：代码版本、输入清单、窗口、交易所、有效盘口定义和 universe 规则。
- quality_by_exchange_month.csv：缺失、锁定、交叉、atomic-chain、上海主动
  余量、genuine passive add 数量和覆盖率。
- old_vs_safe_factor.csv：因子相关、分组一致率、尾部 Jaccard 和覆盖变化。
- raw_performance.csv：不做中性化的 IC、分位数组合和多空差。
- findings.md：是否触发重算、哪些旧报告被保留或被替代。
- supersedes.json：新旧产物和下游依赖的明确映射。

## 9. 完成标准

- 所有必重算项目完成 2026-01 至 2026-02 对齐审计。
- experiment_batch_1 上海 4,607 个 stock-month 完成主动余量修复，上海
  quote_lifecycles 和第一层机制报告重新生成。
- 所有条件审计项目均得到“保留旧口径”或“转全量重算”的明确结论。
- 上海与深圳质量指标、因子方向和原始表现均单独可追溯。
- 股票产物中 ETF 数量为 0。
- 所有被替代的旧结论都有新产物路径；未被替代的旧结果有审计依据。
- 本计划状态从 GATED 更新为 DONE 或拆分后的后续实验状态。

## 10. 当前相关证据

- 第一层结果：results/intraday/experiment_batch_1/mechanism_analysis_202601_202602_v1/findings.md
- 第一层分析脚本：scripts/factors/experiment_batch_1/analyze_mechanisms.py
- 上海 6 个股票月 V1/V2 审计：V1 15,211 条新增挡位，V2 保留 8,157 条，
  排除主动余量 7,054 条（46.4%）；主动链及非报价字段一致。
- 总实验台账：docs/EXPERIMENT_PLAN.md
