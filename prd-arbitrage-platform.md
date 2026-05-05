# PRD: Binance 套利交易平台

## 1. 项目概述

Binance 套利交易平台是一个自动化交易系统，用于执行币安资金费率套利策略。用户可以开仓、平仓持仓，并自动跟踪资金费率收益。

## 2. 项目目标

- 实现资金费率套利的自动化开仓/平仓流程
- 支持期货和现货的订单执行
- 批量执行订单模式 (futures_first / spot_first)
- WebSocket + 轮询混合方式监控订单状态
- 持仓管理及收益跟踪

## 3. 用户故事

### US-001: 开仓
**描述:** 作为用户，我想开仓一个合约的套利仓位，以便获取资金费率收益。

**验收标准:**
- [ ] 发送开仓请求后创建 PositionExecute 记录
- [ ] 自动创建 BatchExecute 批次记录
- [ ] 调度器自动执行订单
- [ ] 记录执行状态和原因

### US-002: 平仓
**描述:** 作为用户，我想平掉已开仓位，结束套利。

**验收标准:**
- [ ] 发送平仓请求
- [ ] 执行平仓订单
- [ ] 更新持仓状态为 CLOSED

### US-003: 资金费率查询
**描述:** 我想查看当前所有合约的资金费率。

**验收标准:**
- [ ] 返回所有 USDT 合约的资金费率
- [ ] 显示下次资金结算时间

### US-004: 收益记录
**描述:** 我想查看历史收益。

**验收标准:**
- [ ] 存储资金费率收益
- [ ] 存储理财利息收益
- [ ] 计算总收益

### US-005: 健康检查
**描述:** 我想监控系统健康状态。

**验收标准:**
- [ ] 检查数据库连接状态
- [ ] 返回整体健康状态

## 4. 功能需求

### 后端 API

- FR-001: POST /api/open-position - 开仓接口
- FR-002: POST /api/close-position - 平仓接口
- FR-003: GET /api/funding-rates - 资金费率查询
- FR-004: GET /api/positions - 持仓列表
- FR-005: GET /api/positions/{id} - 持仓详情
- FR-006: GET /api/positions/{id}/progress - 开仓进度
- FR-007: GET /api/batches/{id} - 批次详情
- FR-008: GET /api/earnings - 收益历史
- FR-009: GET /api/health - 健康检查
- FR-010: GET /api/plugins - 可用插件列表

### 调度器

- FR-011: 定时唤醒待执行批次
- FR-012: 执行批次订单
- OrderWatcher 监控订单状态

## 5. 技术架构

- FastAPI + APScheduler
- PostgreSQL + SQLAlchemy AsyncORM
- aiohttp + websockets (订单监控)
- Docker Compose 部署

## 6. 配置说明

### 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| POSTGRES_HOST | localhost | 数据库地址 |
| POSTGRES_PORT | 5432 | 数据库端口 |
| POSTGRES_USER | postgres | 数据库用户 |
| POSTGRES_PASSWORD | postgres | 数据库密码 |
| POSTGRES_DB | arbitrage | 数据库名 |
| BINANCE_API_KEY | - | 币安 API Key |
| BINANCE_SECRET_KEY | - | 币安 Secret Key |
| CORS_ORIGINS | http://localhost:3000,http://localhost:8000 | CORS 允许的源 |
| DB_POOL_SIZE | 5 | 数据库连接池大小 |
| DB_MAX_OVERFLOW | 10 | 最大溢出连接数 |
| LOG_LEVEL | INFO | 日志级别 |

## 7. 订单执行流程

1. PENDING → 初始化参数
2. FIRST_ORDER_OPEN → 开第一边订单
3. FIRST_ORDER_WAIT → 等待第一边成交
4. FIRST_FILLED → 第一边已成交
5. SECOND_ORDER_OPEN → 开第二边订单
6. SECOND_ORDER_WAIT → 等待第二边成交
7. COMPLETED → 完成

## 8. 成功指标

- API 响应正常
- 订单执行流程完整
- 数据库操作稳定
- 健康检查通过
