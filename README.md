# 全球数字贸易合作运营平台

本项目是一套可离线运行的 Python 服务端平台，用于管理跨境数字合作中的资源流转、项目证据评估和统计资料质量。平台把运营节点、合作通道、资源申请、项目版本、分析决定、样本观测、权限和审计事件持久化到 SQLite，供秘书处、项目办公室、数据团队和审计人员协作使用。

## 目录

- src/trade_flow/：运营节点、合作通道、资源批次、额度申请、分配和情景分析；
- src/cooperation_assurance/：合作项目、证据版本、评估协议、观测导入、分析任务与准入决定；
- src/metric_quality/：统计样本批次、指标观测、质量分析、账号权限和审批；
- fixtures/：离线验收使用的评估协议与结构化观测；
- tests/：领域规则、错误边界、事务、权限、HTTP API 和命令行验收测试。

## 环境

- Linux
- Python 3.11 或更高版本
- 运行时仅使用 Python 标准库和 SQLite

## 测试

    PYTHONPATH=src python3 -m unittest discover -s tests -v

## 构建检查

    python3 -m compileall -q src tests

## 离线验收

    PYTHONPATH=src python3 -m trade_flow.acceptance --workspace .
    PYTHONPATH=src python3 -m cooperation_assurance.acceptance --workspace .
    PYTHONPATH=src python3 -m metric_quality.acceptance

三条命令会在临时 SQLite 数据库中完成合作资源流转、项目证据评估和统计资料质量流程，不访问外部网络。

## HTTP 服务

    PYTHONPATH=src python3 -m trade_flow.api --database trade-flow.sqlite3 --host 127.0.0.1 --port 8080
    PYTHONPATH=src python3 -m cooperation_assurance.api --database cooperation-assurance.sqlite3 --host 127.0.0.1 --port 8081
    PYTHONPATH=src python3 -m metric_quality.api --database metric-quality.sqlite3 --host 127.0.0.1 --port 8082

服务提供 JSON 接口与健康检查。进程重启后可以继续读取 SQLite 中的业务状态和审计历史。

## 统计测量的写入边界数值契约

`metric_quality` 服务在测量记录进入业务表和审计链之前强制执行数值契约
（规则版本 `metric-measurement-contract-1.0.0`，见 `src/metric_quality/contracts.py`），
对观测期（`test_frequency_hz`）、指标值（`response`）、偏差（`noise`）、
来源身份（`instrument`）和观测身份（`observation_key`）统一校验：

- 请求体使用严格 JSON 解析：`Infinity`/`-Infinity`/`NaN` 与重复键在解析边界即被拒绝；
- 数值字段不接受字符串伪装（`"0.93"`、`"Infinity"`）、布尔值，且必须落在适用量程内；
- 同一观测身份的冲突重放被拒绝；内容一致的重放幂等返回；
- 单条与批量写入都在单个事务内完成，任一条非法则整批回滚，业务表和审计链均不被污染；
- 契约失败返回 `422`，并在 `error.rejections` 中给出每条记录的具体字段（`field`）、
  规则代码（`rule`）和可读说明，报送方在写入时即可定位问题。

契约建立前入库的存量记录不会在读取时被悄悄忽略：启动迁移时旧记录进入
`validity='quarantined'` 待甄别状态，`POST /quarantine/scan` 重新套用规则，
识别出的非有限数值、伪装类型和超量程记录写入 `measurement_quarantine` 隔离台账，
登记命中规则、规则版本、处置人和处置时间。命中数值/来源规则的记录只能 `purge`
剔除，不能人工放行；仅缺契约版本但数值合规的旧记录可由质量角色复核后 `release`。
后续分析（`/lots/{id}/analysis`）只消费 `validity='valid'` 且带契约版本的
可追溯观测，有效观测不足时直接报错而不是产出受污染的结论。
