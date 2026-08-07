# 因子与研究计算前置审计门禁

更新日期：2026-08-07

本门禁解决两个不同问题：沪深主动单余量口径是否正确，以及某个计算任务是否绑定了通过审计的代码和股票清单。`Q` 是工程证据，不是研究实验。

## 事件口径

- 联合主键始终为 `(side, active_order_id)`；买卖方向不同但数字 ID 相同的订单不得合并。
- 上海：同一主动单先出现一条或多条 `TRADE`，随后才可能发布未成交余量 `ORDER_ADD`。原始提交量为前置立即成交量加后置余量；完全立即成交时可以没有 `ORDER_ADD`。
- 深圳：先发布完整提交量 `ORDER_ADD`，后续 `TRADE` 消耗该数量；不会为剩余量再发第二条新增委托。
- 任一交易所中，属于主动单键的 `ORDER_ADD` 都不再计为独立被动提交或报价到达。
- 不使用事后 `FULL/PARTIAL` 或前向 recid 链接构造点时特征。

## 凭证和状态机

```text
Q001-Q008 PASS receipt
  -> factor manifest: ready_to_submit
  -> 外部计算任务
  -> completion.json: completed_audited
  -> research manifest: planned
  -> 外部研究计算任务
```

因子 manifest 绑定输入清单、审计凭证、实现版本及其 SHA-256；其中任何一项变化，必须重新审计或使用新 run ID。研究实验只接受 `completed_audited` 的因子完成凭证，不接受仅计划或仍在运行的因子任务。

## 选择并建立因子批次

```bash
conda_lob/bin/python scripts/pipelines/factor_pipeline.py status
conda_lob/bin/python scripts/pipelines/factor_pipeline.py show F001
conda_lob/bin/python scripts/pipelines/factor_pipeline.py plan F001 \
  --run-id 202601_202602_sh_safe_v1 \
  --months 202601 202602 --exchange SH --window 1000_1030 \
  --manifest data/manifests/v4_a_share_stock_paths_202510_202602.txt \
  --audit-receipt audits/Q003/q003_202601_12x3_v1/preflight_receipt.json
```

成功建立的 manifest 状态为 `ready_to_submit`，可作为计算任务的唯一提交规格。未通过凭证、清单月份不覆盖、实现已改变或因子尚无实现时，命令会直接失败。

计算结束后封存输出：

```bash
conda_lob/bin/python scripts/pipelines/complete_factor_run.py \
  --factor-run runs/factors/F001/202601_202602_sh_safe_v1/manifest.json \
  --output data/processed/prebook_rerun/F001/202601_202602_sh_safe_v1
```

## 选择研究实验批次

```bash
conda_lob/bin/python scripts/pipelines/experiment_pipeline.py status
conda_lob/bin/python scripts/pipelines/experiment_pipeline.py show R002
conda_lob/bin/python scripts/pipelines/experiment_pipeline.py plan R002 \
  --run-id 202601_202602_sh_v1 \
  --factor-run F001=runs/factors/F001/202601_202602_sh_safe_v1/completion.json
```

研究 manifest 保留预登记研究问题、决策规则、因子完成凭证和结果路径。ETF、事件顺序、守恒、串并行一致性和 schema 检查仍只登记为 `Q`，不会混入 `R` 编号。

## 本次修复对计算选择的影响

| 分类 | 新因子编号 | 处理 |
|---|---|---|
| 必须按修复后代码重算 | F001、F005、F008、F010、F011、F013 | safe pre-book 或上海主动余量直接影响定义/公共扫描 |
| 本次发现同号异向键风险，旧结果不可直接继承 | F003 | 已冻结 `v2_side_qualified`，重新计算 |
| 先提交定义审计批次 | F002、F004 | 使用 `--purpose definition_audit`；达到预登记阈值并冻结后才能生成生产批次 |
| 本问题不触发重算 | F006 | 订单流历史意外不依赖余量作为被动新增的解释 |
| 暂不可提交 | F007、F009、F012、F014-F017 | 尚无冻结实现或仍依赖上游定义 |

上述“可重算”只代表计算口径和门禁已具备，不代表相应研究假说已获得支持。
