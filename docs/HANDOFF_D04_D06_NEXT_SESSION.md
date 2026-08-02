# D04–D06 下一 Session 交接

## 目标

复现 `stylized-fact-4-6_日频因子手册.md` 的第二组因子：

- D04：残差主动大单流入；
- D05：主动大单流入惊奇与持续性；
- D06：主动大单—价格反应不足。

先做少量日期冒烟测试，确认口径和负载后再提交后台增量计算。不要直接启动全市场任务。

## 必读

1. `AGENTS.md`
2. `README.md`：沪市撮合、`FULL/PARTIAL` 和前视语义
3. `data/README.md`：禁止复用带未来筛选的历史缓存
4. `docs/stylized-fact-4-6_日频因子手册.md`：2.4、D04–D06、5.1–5.2、6–7节
5. `docs/量比与订单行为因子手册.md`：字段、主动/被动、大单、订单状态和回测规范
6. `/home/avery/.codex/skills/lob-domain-neutralization/SKILL.md`
7. `results/lob5_ex_size/existing_factors_202601/analysis_and_next_replication.md`

## 已固定口径

- 原始数据：`/hdd_data/lob/event_depth10_v4/<YYYYMM>/*.parquet`，但目录包含ETF，禁止直接用无限制 glob 作为股票池。
- 必须使用点时点A股股票 manifest，并验证输出中ETF为零。
- 每个 parquet 每组只读一次；在一次读取中落下所有共享原始量和稳健性版本。
- 采用批次原子落盘、manifest指纹和断点续跑；可参考 `scripts/factors/stylized_fact_4_6/reproduce_d01_d03.py`。
- 项目环境：`conda_lob/bin/python`。
- 4 workers此前负载正常，但新脚本仍必须先比较相同小样本的串行/并行输出。
- 日频主版本可计算 `09:30–close` 和 `10:00–close`；重点版本为 `10:00–close`。
- 如果做日内扩展，暂用 `10:00–10:30`，但手册的D04–D06主定义是日频，日内版本必须单独写清定义。
- v4每笔成交触发的LOB更新已经复原，不需要再次补造该笔成交的盘口更新。

## 大单定义

主阈值：

```text
0.5 × 个股过去20日平均单笔委托量
```

阈值只能使用 `d-1` 及以前数据。稳健性版本至少保留：

- `1.0 × 过去20日平均单笔委托量`；
- 过去20日单笔委托量80%和90%分位数；
- 固定成交金额版本。

沪市主动订单可能先成交、后发布剩余委托。日频收盘因子可以使用截至当日收盘已知的完整当日生命周期；日内10:30版本不能使用10:30之后才出现的剩余量、链接或成交来回填原订单规模。

## 推荐实现分层

### 第一层：只读LOB，输出原始量

建议新脚本：

```text
scripts/factors/stylized_fact_4_6/reproduce_d04_d06.py
```

一次读取至少输出：

- 主动大买/大卖量及笔数；
- 主动大单相对流入 `ALF`；
- 买卖两侧分别的历史阈值所需统计量；
- 事件数、有效盘口数、订单连接质量；
- 09:30–close、10:00–close，以及可选10:00–10:30窗口；
- 各个大单阈值稳健性版本。

原始量放在：

```text
data/cache/stylized_fact_4_6/
```

### 第二层：不重读LOB，计算D04–D06

- D04：将ALF对当日收益、过去收益、流动性、波动率、市值、行业和板块等控制变量做截面残差化。
- D05：先得到D04，再计算60日惊奇、3/20加速度、5日持续性及方向一致天数。
- D06：使用D04与当日价格响应构造反应不足；优先二维分组，不使用两个Z-score相除。

D05需要至少60个交易日历史。目标期输出前必须准备足够的D04历史，历史只用于滚动统计且当前日必须按定义排除或包含。

全市场正式因子放在：

```text
data/processed/stylized_fact_4_6/
```

## 中性化与回测

报告优先级：

1. 九个预定义分域分别检验；
2. 域内中性化、域内标准化后合并的全市场汇总；
3. 未分域的全市场中性化结果；
4. 原始全市场表现和暴露诊断。

域内默认中性化为 `LOB5-ex-size`：

```text
non_linear_size, momentum, liquidity, beta, residual_volatility
```

线性 `size` 不进入域内回归，因为市值已经分层。日内因子使用前一交易日风格暴露；日频收盘因子只能预测信号形成后的收益。

D04–D06至少检验：次日隔夜、次日日内、open-to-open、第2–5日和第5–10日收益。不能用某一未来期限的标签可得性筛选其他期限的股票池。

## 验证顺序

1. 单文件核对事件数、订单量、买卖方向和阈值；
2. 约12只股票、2–3日小样本；
3. 相同样本串行与4-worker逐字段比较；
4. 检查ETF数量为零；
5. 检查阈值只依赖过去数据；
6. 检查D05滚动窗口边界；
7. 检查每批原子落盘、manifest不匹配拒绝续跑；
8. 再提交后台全市场任务。

## 当前可参考产物

- D01–D03实现：`scripts/factors/stylized_fact_4_6/reproduce_d01_d03.py`
- D01–D03原始量：`data/cache/stylized_fact_4_6/g1_d01_d03_primitives_202512_202601_history20_v3.csv`
- D01–D03因子：`data/processed/stylized_fact_4_6/g1_d01_d03_factors_202512_202601_history20_v3.csv`
- 五风格回测：`results/lob5_ex_size/existing_factors_202601/`
- 五风格分析脚本：`scripts/backtests/analyze_existing_factors_lob5_ex_size.py`

## 可直接给新 Session 的任务

> 阅读 `docs/HANDOFF_D04_D06_NEXT_SESSION.md`、仓库 `AGENTS.md` 及其中列出的必读文档。先检查v4实际字段和可用月份，明确D04–D06的原始量、历史窗口及控制变量。参考D01–D03的分批落盘和断点续跑结构，实现D04–D06两层计算：第一层每个LOB parquet只读一次并输出共享原始量，第二层不重读LOB计算D04–D06。先完成单文件和小样本验证、串行与4-worker一致性、ETF为零及无前视检查；在汇报预计时间和内存后，再提交后台全市场任务。
