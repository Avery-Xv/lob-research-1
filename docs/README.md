# 文档目录

本目录同时保存研究素材、跨 session 交接、进度看板和工程规范。文件暂不按子目录搬迁，因为 `research/*.json`、代码、测试及文档之间已有稳定路径引用；本页作为统一分类入口。

## 首选入口

| 目的 | 文档 | 权威性 |
|---|---|---|
| 查看所有正式课题的内容、方法、进展、结论与依赖 | [RESEARCH_TOPICS_STATUS_METHODS_AND_DEPENDENCIES.md](RESEARCH_TOPICS_STATUS_METHODS_AND_DEPENDENCIES.md) | 当前研究总览；正式状态仍以 `research/experiments.json` 和 `research/factors.json` 为准 |
| 查看因子、研究、数据产品和质量门如何流转 | [RESEARCH_PIPELINES.md](RESEARCH_PIPELINES.md) | 当前流程规范 |
| 提交全市场计算前检查 | [COMPUTE_PREFLIGHT.md](COMPUTE_PREFLIGHT.md) | 当前工程门禁 |
| 接续非母单研究 | [HANDOFF_NON_PARENT_ORDER_SHAPE_NEXT_SESSION.md](HANDOFF_NON_PARENT_ORDER_SHAPE_NEXT_SESSION.md) | 当前非母单交接 |

## 研究素材与因子手册

这些文档定义经济问题、变量、公式、标签、机制假说和否证方式。它们是研究设计素材，不代表其中每个候选都已完成实证。

| 文档 | 内容 | 当前用途 |
|---|---|---|
| [Event_LOB最小实证实验方案.md](Event_LOB最小实证实验方案.md) | imbalance、spread、多跳响应、兑现时间和事件 IRF | R010–R013 设计依据 |
| [stylized-fact-4-6_日频因子手册.md](stylized-fact-4-6_日频因子手册.md) | D01–D11 的定义、机制和回测边界 | F005–F012 / R003–R009 依据 |
| [大价差占比因子逻辑文档.md](大价差占比因子逻辑文档.md) | 被动挂单初始 gap、深层成交及条件因子 | F002、F004 依据 |
| [量比与订单行为因子手册.md](量比与订单行为因子手册.md) | VR、CR、单笔规模及主动/被动订单统计 | F003 依据 |
| [订单簿形态与非对称自刺激_增量因子手册.md](订单簿形态与非对称自刺激_增量因子手册.md) | M 系列和非母单机制的理论来源 | F013、F014 及 R001、R014–R018 依据 |
| [母单结构与执行阶段_衍生因子研究手册.md](母单结构与执行阶段_衍生因子研究手册.md) | 订单链、推断执行片段、节奏、冲击和恢复 | F017 / R021–R025 依据 |
| [近端大单遮挡与深档撤单响应因子设计文档.md](近端大单遮挡与深档撤单响应因子设计文档.md) | 近端大单遮挡、深档撤单和生命周期 | F016 / R020 依据 |

## Handoff 交接文档

这些文档面向下一次工作 session，记录已冻结口径、可复用输入、首个动作、验收顺序和停止条件。它们不是全局状态总账。

| 文档 | 交接范围 | 使用时机 |
|---|---|---|
| [HANDOFF_NON_PARENT_ORDER_SHAPE_NEXT_SESSION.md](HANDOFF_NON_PARENT_ORDER_SHAPE_NEXT_SESSION.md) | NP01–NP05、P002缓存、九分域直接目标 | 接续非母单订单状态研究 |
| [HANDOFF_D04_D06_NEXT_SESSION.md](HANDOFF_D04_D06_NEXT_SESSION.md) | D04–D06 定义、历史窗口和实现分层 | 接续主动大单历史意外研究 |
| [HANDOFF_INTRADAY_STRUCTURE_PILOT.md](HANDOFF_INTRADAY_STRUCTURE_PILOT.md) | 非母单与执行结构初筛边界 | 启动结构研究 pilot |

## 进度看板与执行计划

| 文档 | 性质 | 说明 |
|---|---|---|
| [RESEARCH_TOPICS_STATUS_METHODS_AND_DEPENDENCIES.md](RESEARCH_TOPICS_STATUS_METHODS_AND_DEPENDENCIES.md) | 当前总览看板 | 覆盖 R001–R025、现有结论、下一步和依赖图 |
| [EXPERIMENT_PLAN.md](EXPERIMENT_PLAN.md) | 历史实验总账 | 保留旧编号、旧批次和迁移来源；不再作为正式状态注册表 |
| [沪深pre-book口径审计与重算计划.md](沪深pre-book口径审计与重算计划.md) | 专项执行看板 | 跟踪安全 pre-book、上海主动余量和重算范围 |

正式机器可读状态以 `research/experiments.json`、`research/factors.json`、`research/data_products.json` 和 `research/quality_gates.json` 为准。

## 工程规范、审计与迁移

| 文档 | 内容 |
|---|---|
| [COMPUTE_PREFLIGHT.md](COMPUTE_PREFLIGHT.md) | 因子任务提交前审计、manifest 和完成门禁 |
| [RESEARCH_PIPELINES.md](RESEARCH_PIPELINES.md) | F/R/P/Q 双管线、产物状态与编号规则 |
| [order_shape_mechanism_v4_compute_audit.md](order_shape_mechanism_v4_compute_audit.md) | V4 订单形态计算的字段、事件和资源审计 |
| [LEGACY_RESULT_MIGRATION.md](LEGACY_RESULT_MIGRATION.md) | 旧结果导入及 lineage 规则 |

## 维护规则

1. 新研究问题先登记到 `research/experiments.json`，只有可证伪问题获得 R 编号。
2. 新可复用因子登记到 `research/factors.json`；工程检查只使用 Q 编号。
3. 新 handoff 使用 `HANDOFF_<TOPIC>_NEXT_SESSION.md` 命名，并在本页登记。
4. 进度更新优先修改机器可读注册表，再同步当前总览；不得只改历史 `EXPERIMENT_PLAN.md`。
5. 不随意移动已有文件；确需迁移时，同步更新注册表 `theory_sources`、相对链接、代码和测试引用。
