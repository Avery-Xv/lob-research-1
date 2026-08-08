# 因子、研究实验与工程质量门

本仓库的用户主线只有两条：因子管线和研究实验管线。工程性工作不占研究实验编号，而是作为数据产物与质量门依附在任务 manifest 上。

## 1. 四类编号

| 前缀 | 含义 | 是否属于研究实验 |
|---|---|---|
| `F` | 因子定义或紧密相关的候选因子族 | 否；它是研究对象/输入 |
| `R` | 有研究问题、目标变量和可证伪判据的实证实验 | 是 |
| `P` | 可复用数据产物或公共扫描缓存 | 否 |
| `Q` | 数据、软件和运行质量门 | 否 |

判定一个条目能否进入 `R` 的最低标准：

1. 能写成需要数据回答的研究问题；
2. 有明确目标变量和主输出；
3. 有查看结果前确定的继续、停止或分类判据；
4. 结果能够改变对市场机制或因子有效性的判断。

以下事项本身不构成研究实验：ETF=0、事件顺序、午休重置、safe pre-book 实现、订单守恒、串并行一致性、manifest 指纹、断点续跑和输出 schema。它们分别登记为 `Q001-Q008`。

## 2. 注册表

- `research/factors.json`：`F001-F017` 因子总账；记录理论、实现、数据依赖和必过质量门。
- `research/experiments.json`：`R001-R025` 研究实验；每项强制包含 `research_question`、`research_outputs` 和 `decision_rule`。
- `research/data_products.json`：公共数据产品。共享 LOB 原语为 `P001`，非母单订单状态缓存为 `P002`。
- `research/quality_gates.json`：工程检查。原 PB01 被拆入 `Q003` 及受影响因子的定义版本，不再占实验编号。

原 RG01 是统一评估流程，也不再作为实验。raw 收益、稳健性、成本和中性化要求直接写进对应 `R` 的输出与判据。

## 3. 研究实验重新编号

| 范围 | 研究线 | 旧文档映射 |
|---|---|---|
| R001 | 已完成订单形态六机制基线 | M00、M1-M6 |
| R002 | 主动成交价格冲击跨月复现 | AG01 |
| R003 | 主动大单流与历史意外增量 | ID05 |
| R004-R009 | D06-D11 的点时机制与因子实证 | SF46I06-SF46I11 |
| R010-R013 | Event-LOB 四个最小实证 | ELOB01-ELOB04 |
| R014-R018 | 非母单第一批五项研究 | NP01-NP05 |
| R019 | 非母单事件机制深化 | NPE01-NPE05 |
| R020 | 近端大单遮挡与深档撤单机制 | DR01-DR05 |
| R021-R025 | 执行链/推断片段的五阶段研究 | MSI-A 至 MSI-E |

PB01、P001、RG01 没有映射到 `R`，因为它们分别属于口径修复、公共计算产物和统一评估流程。

## 4. 逻辑链路

```text
理论/研究假说
  -> F 因子定义
  -> P 公共数据依赖
  -> Q 工程质量门
  -> 因子 run manifest
  -> R 研究问题与预登记判据
  -> research run manifest
  -> 研究结果、结论与停止/继续决策
```

`Q` 通过只能说明结果可被信任，不能说明研究假说成立。反过来，任何统计显著但未通过必要 `Q` 的结果不能进入研究结论。

## 5. 使用方式

计算前必须先阅读 [`COMPUTE_PREFLIGHT.md`](COMPUTE_PREFLIGHT.md)，并使用其中的 PASS receipt；仅列出注册表不代表允许提交。

```bash
conda_lob/bin/python scripts/pipelines/factor_pipeline.py status
conda_lob/bin/python scripts/pipelines/factor_pipeline.py show F001
conda_lob/bin/python scripts/pipelines/experiment_pipeline.py status
conda_lob/bin/python scripts/pipelines/experiment_pipeline.py show R002
```

建立因子计算计划：

```bash
conda_lob/bin/python scripts/pipelines/factor_pipeline.py plan F001 \
  --run-id 202601_202604_all_safe_v2 \
  --months 202601 202602 202603 202604 \
  --exchange ALL --window 1000_1030 \
  --manifest data/manifests/v4_a_share_stock_paths_202601_202604.txt \
  --audit-receipt audits/Q003/q003_202601_12x3_v2/preflight_receipt.json
```

factor manifest 只有在凭证、清单月份和当前实现全部匹配后才写为 `ready_to_submit`。计算完成后先用 `complete_factor_run.py` 封存输出；研究实验只能绑定完成凭证：

```bash
conda_lob/bin/python scripts/pipelines/experiment_pipeline.py plan R002 \
  --run-id 202601_202604_all_v1 \
  --factor-run F001=runs/factors/F001/202601_202604_all_safe_v2/completion.json
```

研究任务输出写入 `results/research/<R-ID>/...`。工程审计报告应随对应 factor/data run 保存，不进入 `results/research/`，避免再次把工程完成状态误写成研究结论。

## 6. 研究报告原则

- 研究结论必须围绕注册表里的问题与判据书写。
- 直接目标有效不等于未来收益 alpha；只有明确接入收益的实验才能给出收益结论。
- raw 与中性化规格按对应研究计划分别报告，不允许工程默认值静默替换研究主规格。
- sparse event 研究优先报告事件对照、事件数和置信区间，不强求普通十分组单调性。
- 旧结果只用于查错和 sanity check；新路径可按当前规划重新提交，不要求完整复用旧产物。
