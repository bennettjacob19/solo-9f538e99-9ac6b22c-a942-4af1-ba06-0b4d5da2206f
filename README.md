# Audit Event Online Index Service

在线为持续写入的审计事件表补建查询索引的 FastAPI + PostgreSQL 服务。

## 能力

- **事件写入 / 查询**：`audit_events(tenant_id, event_type, occurred_at)` 持续写入；
  支持按 `tenant_id` 或 `event_type` + 时间范围过滤查询。
- **在线索引申请**：可申请两种复合索引（均为 `CREATE INDEX CONCURRENTLY`，
  构建期间不阻塞写入与查询）：
  - `tenant_occurred` → `(tenant_id, occurred_at DESC)`
  - `event_type_occurred` → `(event_type, occurred_at DESC)`
- **任务状态查询**：`PENDING / RUNNING / SUCCEEDED / FAILED`，失败原因记录在 `error`。
- **并发与幂等**：
  - 相同 `(operation_key, mode)` 的并发重试共用同一任务行（唯一约束 +
    `ON CONFLICT DO NOTHING`）；失败后用相同键重试会把任务重新排队。
  - 同一 `operation_key` 改申请另一模式返回 **409 Conflict**。
  - 索引名按模式固定，**同模式不同键只建一次**，后续任务直接复用已存在的有效索引。
- **目录对账（重启 / 失败安全）**：启动时以 PostgreSQL 系统目录
  `pg_index.indisvalid/indisready` 为准修正任务状态：
  - 有效索引 → 任务置 `SUCCEEDED`，且任何路径都不会 DROP 有效索引；
  - 索引缺失或 INVALID 而任务声称 RUNNING/SUCCEEDED → 置 `FAILED`，
    残件绝不报成功；
  - 重试时先 `DROP INDEX CONCURRENTLY IF EXISTS` 清理 INVALID 残件再重建。
- DDL 使用独立 autocommit 连接（CONCURRENTLY 不能在事务块中执行），
  并以 `pg_advisory_lock` 按索引名串行化，多进程部署也安全。

## 启动（Docker Compose）

```bash
docker compose up --build
# API:   http://localhost:8000
# 文档:  http://localhost:8000/docs   （可直接在页面上申请索引、写事件、查事件、查任务）
```

## API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/index-tasks` | 申请索引；body `{"operation_key":"k","mode":"tenant_occurred"}` |
| GET  | `/index-tasks` | 全部任务 |
| GET  | `/index-tasks/{id}` | 单个任务状态 |
| POST | `/events` | 写审计事件 |
| GET  | `/events?tenant_id=...&event_type=...&occurred_after=...&occurred_before=...` | 条件查询（至少传 tenant_id 或 event_type 之一） |
| GET  | `/health` | 健康检查 |

## 本地开发（无 Docker 时）

仓库测试用嵌入式 PostgreSQL（`pgserver`），无需系统安装 Postgres：

```bash
python3 -m venv --without-pip .venv && .venv/bin/python get-pip.py   # 若 ensurepip 缺失
.venv/bin/pip install -r requirements.txt pytest pytest-asyncio asgi-lifespan pgserver httpx
.venv/bin/python -m pytest -q
```

## 测试覆盖（tests/test_e2e.py，11 个端到端用例）

1. 健康检查、事件写入与多条件查询、参数校验
2. 索引申请 → 构建 → SUCCEEDED 完整生命周期，目录状态 valid/ready
3. 8 个相同请求并发：只产生 1 个任务
4. 同键改模式 → 409；同键同模式幂等
5. 同模式不同键：物理索引只建一次，第二个任务复用
6. 预置 INVALID 残件：重试先 DROP 残件再建成功
7. 人为制造构建失败 → FAILED（含错误信息）→ 排障后同任务重试成功
8. 模拟崩溃（RUNNING + 索引缺失）：重启对账为 FAILED，重试成功
9. 有效索引在多次重启后 OID 不变、状态保持 SUCCEEDED（不误删）
10. 20 万行表构建期间持续写入/查询可用，且建成后查询走新索引
11. advisory lock 卡住构建中途时，写入与查询仍然可用
