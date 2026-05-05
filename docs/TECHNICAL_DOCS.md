# Binance 套利交易平台 - 技术文档

## 目录
1. [系统概述](#1-系统概述)
2. [架构图](#2-架构图)
3. [时序图](#3-时序图)
4. [数据库模型](#4-数据库模型)
5. [API 接口](#5-api-接口)
6. [配置说明](#6-配置说明)

---

## 1. 系统概述

### 1.1 项目目标
Binance 套利交易平台是一个自动化交易系统，用于执行币安资金费率套利策略：
- 合约做空 + 现货做多 → 持有赚取资金费率
- 资金费率收益 + 价差收益

### 1.2 核心技术栈
| 技术 | 用途 |
|------|------|
| FastAPI | Web 框架 |
| APScheduler | 任务调度 |
| SQLAlchemy 2.0 | 异步 ORM |
| PostgreSQL | 数据库 |
| aiohttp | HTTP 客户端 |
| WebSocket | 订单状态监控 |

### 1.3 核心模块
```
backend/
├── main.py              # FastAPI 应用入口
├── core.py             # 调度器核心
├── database.py        # 数据库层
├── api/routes.py       # API 路由
├── modules/
│   ├── collector.py   # 数据采集 (行情、资金费率)
│   ├── trader.py      # 交易执行
│   ├── portfolio.py  # 持仓管理
│   ├── order_watcher.py # 订单监控
│   └── strategy.py    # 策略选择
└── plugins/
    └── order_sequence/ # 订单顺序插件
```

---

## 2. 架构图

### 2.1 系统架构图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         用户端 (Frontend)                        │
└─────────────────────────────────┬───────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        FastAPI 应用服务器                        │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                      API Routes                             ││
│  │  /api/open-position    /api/close-position   /api/health  ││
│  │  /api/funding-rates  /api/positions         /api/earnings  ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────┬───────────────────────────┘
                                  │
              ┌───────────────────┼───────────────────┐
              │                   │                   │
              ▼                   ▼                   ▼
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│ PositionScheduler │  │ CloseScheduler   │  │ LockManager     │
│   (开仓调度器)    │  │   (平仓调度器)     │  │   (并发锁管理)    │
└────────┬─────────┘  └────────┬─────────┘  └──────────────────┘
         │                     │
         │         ┌───────────┴───────────┐
         │         │                     │
         ▼         ▼                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       数据库 (PostgreSQL)                       │
│  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐ │
│  │ position_  │ │  batch_    │ │  lock_     │ │  earning   │ │
│  │ execute    │ │ execute    │ │ info       │ │            │ │
│  └────────────┘ └────────────┘ └────────────┘ └────────────┘ │
└───────────────────────────────────────────────────────────────┘
         │
         │                   ┌──────────────────┐
         │                   │  OrderWatcher   │
         │                   │  (订单状态监控)   │
         │                   └────────┬─────────┘
         │                            │
         ▼                            ▼
┌──────────────────┐  ┌──────────────────┐
│ BinanceCollector │  │  BinanceTrader   │
│  (数据采集)       │  │   (交易执行)       │
└────────┬─────────┘  └────────┬─────────┘
         │                    │
         ▼                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Binance 交易所                        │
│  ┌──────────────────┐ ┌──────────────────┐ ┌──────────────┐  │
│  │ 现货 API         │ │ 合约 API         │ │ WebSocket    │  │
│  │ /api/v3/...     │ │ /fapi/v1/...    │ │ /stream      │  │
│  └──────────────────┘ └──────────────────┘ └──────────────┘  │
└───────────────────────────────────────────────────────────────┘
```

### 2.2 调度器架构

```
┌──────────────────────────────────────────────────────────────────┐
│                      APScheduler 调度框架                         │
├──────────────────────────────────────────────────────────────────┤
│                                                               │
│   ┌────────────────────────────────────────────────────────┐   │
│   │     PositionScheduler (开仓调度器)                     │   │
│   │                                                        │   │
│   │  Job1: wake_pending_batches                            │   │
│   │    - 每秒执行                                           │   │
│   │    - 唤醒状态为PENDING的批次                             │   │
│   │    - 同一合约只唤醒ID最小的                             │   │
│   │                                                        │   │
│   │  Job2: execute_running_batches                         │   │
│   │    - 每秒执行                                           │   │
│   │    - 执行状态为RUNNING的批次                            │   │
│   │    - 检查超时、检查订单、执行业务                        │   │
│   │                                                        │   │
│   │  内置 OrderWatcher                                      │   │
│   │    - WebSocket 监听订单成交                           │   │
│   │    - 轮询备用方案                                    │   │
│   └────────────────────────────────────────────────────────┘   │
│                                                               │
│   ┌────────────────────────────────────────────────────────┐   │
│   │     CloseScheduler (平仓调度器)                        │   │
│   │                                                        │   │
│   │  Job1: wake_pending_closes                             │   │
│   │    - 每秒执行                                           │   │
│   │    - 唤醒状态为PENDING的平仓批次                         │   │
│   │                                                        │   │
│   │  Job2: execute_closes                                  │   │
│   │    - 每秒执行                                           │   │
│   │    - 执行平仓操作                                      │   │
│   └────────────────────────────────────────────────────────┘   │
│                                                               │
└──────────────────────────────────────────────────────────────────┘
```

---

## 3. 时序图

### 3.1 开仓完整流程 (Open Position)

#### 3.1.1 时序图：用户发起开仓请求

```
┌────────┐     ┌─────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
│  用户  │     │  API   │     │ LockManager│   │  数据库 │    │Scheduler│
│        │     │Server │     │           │     │          │     │         │
└───┬────┘     └───┬────┘     └────┬─────┘     └────┬─────┘     └────┬────┘
    │              │              │              │              │
1   │──POST /api/open-position─────────>>│              │              │
2   │              │              │              │              │
3   │              │────────>>查询position记录─────────>>│              │
4   │              │              │              │              │
5   │              │              │              │              │
6   │              │              │              │              │
7   │              │              │              │              │
    │              │              │              │              │
    │              │              │              │              │
    │              │         acquire()              │              │
    │              │────────>>检查LockInfo────────────>>│              │
    │              │              │              │              │
    │              │              │    lock不存在           │
    │              │              │<<──return False──────│
    │              │              │              │              │
    │              │              │<<──────return False───│              │
    │              │              │              │              │
    │<<──return 400│              │              │              │
    │ (锁定中)     │              │              │              │
    │              │              │              │              │
    │              │              │              │              │
    │    重试      │              │              │              │
    │──POST /api/open-position─────────>>│              │              │
    │              │         acquire()              │              │
    │              │────────>>检查LockInfo────────────>>│              │
    │              │              │              │              │
    │              │              │   lock不存在        │
    │              │              │<<──return None────│
    │              │              │              │              │
    │              │         插入LockInfo              │              │
    │              │────────>>INSERT lock────────────>>│              │
    │              │              │              │  insert ok  │
    │              │              │<<──────return────│              │
    │              │              │              │              │
    │              │<<──────return True──────────────│              │
    │              │              │              │              │
    │    创建 PositionExecute                    │              │
    │              │────────>>INSERT position─────────>>│              │
    │              │              │              │  insert ok  │
    │              │              │<<──────return────│              │
    │              │              │              │              │
    │    创建 BatchExecute(s)                │              │
    │              │────────>>INSERT batch(es)─────>>│              │
    │              │              │              │  insert ok  │
    │              │              │<<──────return────│              │
    │              │              │              │              │
    │    释放锁    │         release()                │              │
    │              │────────>>UPDATE lock────────────>>│              │
    │              │              │              │  update ok │
    │              │              │<<──────return────│              │
    │              │              │              │              │
    │<<──return 200│              │              │              │
    │ (成功)       │              │              │              │
    │              │              │              │              │
    │              │              │              │   后台自动执行
    │              │              │              │────────>> wake
    │              │              │              │   execute
```

#### 3.1.2 时序图：调度器唤醒批次

```
┌──────────────┐     ┌──────────┐
│ APScheduler │     │  数据库  │
│             │     │          │
└──────┬───────┘     └────┬─────┘
       │
       │ Job: wake_pending_batches (每秒触发)
       │
       │ 查询RUNNING状态的合约
       │──>>SELECT position_execute WHERE status=RUNNING
       │              │              │
       │              │<<──return contracts[]
       │              │
       │ 查询PENDING状态的批次
       │──>>SELECT batch_execute WHERE status=PENDING ORDER BY id
       │              │
       │              │<<──return batches[]
       │              │
       ├─►遍历每个PENDING批次
       │   │
       │   │ 合约在RUNNING列表中?
       │   │
       │   ├─是──跳过
       │   │
       │   └─否──唤醒批次
       │       │──>>UPDATE batch SET status=RUNNING, phase=PENDING
       │       │              │
       │       │              │ update ok
       │       │              │<<──────return
       │       │
       │       输出日志: Batch {id} woken: contract={contract}
       │
```

#### 3.1.3 时序图：调度器执行批次

```
┌──────────────┐     ┌──────────┐     ┌─────────┐     ┌────────┐
│ APScheduler │     │  数据库  │ │Collector│     │Trader │
│             │     │          │       │        │
└──────┬──────┘     └────┬─────┘     └──┬─────┘     └──┬────┘
       │
       │ Job: execute_running_batches (每秒触发)
       │
       │ 查询RUNNING状态的批次
       │──>>SELECT batch_execute WHERE status=RUNNING ORDER BY id
       │              │
       │              │<<──return batches[]
       │              │
       ├─►遍历每个RUNNING批次
       │   │ 同一合约只处理ID最小的
       │   ├─处理过──跳过
       │   │
       │   └─未处理
       │       │
       │       检查超时
       │       │──elapsed = now() - updated_at
       │       │
       │       ├─超时──
       │       │   │──>>UPDATE status=COMPLETED, reason=TIMEOUT
       │       │   │   skip to next batch
       │       │   │
       │       │
       │       └─未超时──执行阶段
       │           │
       │           ├─phase=PENDING──>>_phase_init_params()
       │           │   │──>>查询配置
       │           │   │──>>get_contract_ticker()──>> Collector
       │           │   │   <<──return ticker
       │           │   │──>>get_spot_price()──>> Collector
       │           │   │   <<──return price
       │           │   │──>>UPDATE batch
       │           │   │   phase=FIRST_ORDER_OPEN
       │           │   │
       │           ├─phase=FIRST_ORDER_OPEN──>>_phase_first_order_open()
       │           │   │──>>读取order_sequence
       │           │   │──>>open_futures_short()──>> Trader
       │           │   │   <<──return result
       │           │   │──>>UPDATE order_id, phase=FIRST_ORDER_WAIT
       │           │   │
       │           ├─phase=FIRST_ORDER_WAIT──>>_phase_first_order_wait()
       │           │   │──>>OrderWatcher.watch_order()
       │           │   │
       │           ├─phase=FIRST_FILLED──>>_phase_first_filled()
       │           │   │──>>UPDATE phase=SECOND_ORDER_OPEN
       │           │   │
       │           ├─phase=SECOND_ORDER_OPEN──>>_phase_second_order_open()
       │           │   │──>>读取配置
       │           │   │──>>buy_spot()──>> Trader
       │           │   │   <<──return result
       │           │   │──>>UPDATE order_id, phase=SECOND_ORDER_WAIT
       │           │   │
       │           └─phase=SECOND_ORDER_WAIT──>>_phase_second_order_wait()
       │               │──>>OrderWatcher.watch_order()
       │
```

### 3.2 订单执行阶段流程

#### 3.2.1 阶段1：初始化参数 (PENDING)

```
┌──────────┐     ┌──────────┐     ┌──────────┐
│Scheduler │     │ 数据库   │     │Collector│
└────┬─────┘     └────┬─────┘     └────┬─────┘
     │
     │ 读取order_sequence配置
     │──>>SELECT order_sequence FROM plugin_config
     │
     │<<──return "futures_first"
     │
     │ 获取合约价格
     │──>>get_contract_ticker(symbol)──>>
     │                         │
     │                    调用 /fapi/v1/ticker/price
     │                         │
     │                    <<──return {price: 50000}
     │
     │  获取现货价格
     │──>>get_spot_price(symbol)──>>
     │                        │
     │                   调用 /api/v3/ticker/bookTicker
     │                        │
     │                   <<──return {bidPrice: 50000, askPrice: 50100}
     │
     │  更新批次
     │──>>UPDATE batch
     │    SET contract_price=50000,
     │        spot_price=50100,
     │        phase=FIRST_ORDER_OPEN
     │
     │              │ update ok
     │<<──────return
```

#### 3.2.2 阶段2：开第一边订单 (FIRST_ORDER_OPEN)

```
┌──────────┐     ┌──────────┐     ┌────────┐
│Scheduler │     │ 数据库  │     │Trader │
└────┬─────┘     └────┬─────┘     └──┬────┘
     │
     │  读取配置 (order_sequence)
     │──>>SELECT order_sequence, contract_price...
     │               │
     │               │<<──return {order_sequence: "futures_first"}
     │
     │  判断订单顺序
     │
     ├──futures_first
     │   │  开合约空仓
     │   │──>>open_futures_short(symbol, amount, price)──>>
     │   │                                           │
     │   │                                      调用 /fapi/v1/order
     │   │                                      (type=LIMIT, side=SELL)
     │   │                                           │
     │   │                                      <<──return {orderId: 123}
     │   │
     └──spot_first
         │  买入现货
         │──>>buy_spot(symbol, amount, price)──>>
         │                                    │
         │                               调用 /api/v3/order
         │                               (type=LIMIT, side=BUY)
         │                                    │
         │                               <<──return {orderId: 456}
         │
     │   │
     │   ├─订单成功
     │   │   │──>>UPDATE batch SET first_side_order_id=?, phase=FIRST_ORDER_WAIT
     │   │   │
     │   └─订单失败
     │       │──>>UPDATE status=COMPLETED, reason=ERROR
     │
```

#### 3.2.3 阶段3：等待第一边成交 (FIRST_ORDER_WAIT)

```
┌──────────┐     ┌────────────┐     ┌────────┐
│Scheduler │     │OrderWatcher│     │Trader │
└────┬─────┘     └─────┬──────┘     └──┬────┘
     │
     │  注册订单监控
     │──>>watch_order(batch_id, order_id, symbol, phase, timeout)
     │
     │  添加到监控列表
     │──>>_watching_orders[order_id] = {batch_id, phase, ...}
     │
     │  启动监控
     │──>>_start_polling() 或 WebSocket 监听
     │
     │  轮询检查订单状态
     │──>>get_order_status(symbol, orderId)──>>
     │                                    │
     │                               调用 /fapi/v1/order
     │                                    │
     │                               <<──return {status: "NEW"}
     │
     │
     ├─持续轮询 (间隔递增: 1,1,2,2,5,5,10,30,60)
     │
     ├─状态=FILLED
     │   │  触发回调
     │   │──>>trigger_phase("FIRST_FILLED", avgPrice)
     │   │   │──>>UPDATE phase=FIRST_FILLED
     │   │   │──>>UPDATE filled_price
     │   │   │
     ├─状态=CANCELLED/REJECTED
     │   │  触发回调
     │   │──>>trigger_phase("PENDING")
     │   │   │──>>UPDATE phase=PENDING (重试)
     │   │
     └─超时
         │  触发回调
         │──>>trigger_phase("COMPLETED")
         │   │──>>UPDATE status=COMPLETED, reason=TIMEOUT
         │
```

#### 3.2.4 阶段4：第一边已成交 (FIRST_FILLED)

```
┌──────────┐     ┌──────────┐
│Scheduler │     │ 数据库   │
└────┬─────┘     └────┬─────┘
     │
     │  更新成交价格
     │──>>UPDATE first_side_filled_price
     │
     │  判断下一阶段
     │
     ├─order_sequence=futures_first
     │   │──>>phase = SECOND_ORDER_OPEN
     │   │
     └─order_sequence=spot_first
         │──>>phase = SECOND_ORDER_OPEN
         │
```

#### 3.2.5 阶段5：开第二边订单 (SECOND_ORDER_OPEN)

```
┌──────────┐     ┌──────────┐     ┌────────┐
│Scheduler │     │ 数据库  │     │Trader │
└────┬─────┘     └────┬─────┘     └──┬────┘
     │
     │  读取配置
     │──>>SELECT order_sequence...
     │
     │               │<<──return
     │
     ├──futures_first (第一边已开空仓)
     │   │  买入现货对冲
     │   │──>>buy_spot(symbol, amount, price)──>>
     │   │                                    │
     │   │                               调用 /api/v3/order
     │   │                                    │
     │   │                               <<──return {orderId}
     │   │
     └──spot_first (第一边已买现货)
         │  开合约空仓对冲
         │──>>open_futures_short(...)──>>
         │                                    │
         │                               调用 /fapi/v1/order
         │                                    │
         │                               <<──return {orderId}
         │
     │   │
     │   ├─成功
     │   │   │──>>UPDATE phase=SECOND_ORDER_WAIT
     │   │   │
     │   └─失败
     │       │──>>UPDATE status=COMPLETED, reason=ERROR
     │
```

#### 3.2.6 阶段6：等待第二边成交 (SECOND_ORDER_WAIT)

```
┌──────────┐     ┌────────────┐     ┌────────┐
│Scheduler │     │OrderWatcher│     │Trader │
└────┬─────┘     └─────┬──────┘     └──┬────┘
     │
     │  注册订单监控
     │──>>watch_order(batch_id, order_id, symbol, phase, timeout)
     │
     │  轮询检查 (同3.2.3)
     │
     ├─状态=FILLED
     │   │  触发完成
     │   │──>>trigger_phase("COMPLETED")
     │   │   │
     │   │   └─>> 转入理财流程
     │   │
     ├─状态=其他
     │   │  处理异常
     │   │   (重试或标记失败)
     │   │
     └─超时
         │  触发超时
         │──>>trigger_phase("COMPLETED")
         │
```

#### 3.2.7 阶段7：完成 (COMPLETED)

```
┌──────────┐     ┌──────────┐     ┌────────┐
│Scheduler │     │ 数据库  │     │Trader │
└────┬─────┘     └────┬─────┘     └──┬────┘
     │
     │  更新第二边成交价
     │──>>UPDATE second_side_filled_price
     │
     │  标记完成
     │──>>UPDATE status=COMPLETED, reason=SUCCESS, phase=COMPLETED
     │
     │  转入理财
     │──>>transfer_to_savings(symbol, amount)──>>
     │                                           │
     │                                      调用 /api/v3/asset/transfer
     │                                      (type=1, main→savings)
     │                                           │
     │                                      <<──return success
     │    
     │  检查主记录完成状态
     │──>>SELECT batches WHERE position_execute_id=?
     │
     │  如果所有batch都是COMPLETED
     │   │──>>UPDATE position status=COMPLETED
     │   │──>>LockManager.release(contract)
     │   │
```

### 3.3 平仓流程

#### 3.3.1 用户发起平仓请求

```
┌────────┐     ┌─────────┐     ┌──────────┐     ┌──────────┐     ┌─────────┐
│  用户  │     │  API   │     │LockManager│   │  数据库  │    │Scheduler│
│        │     │Server │     │           │     │          │     │         │
└───┬────┘     └───┬────┘     └────┬─────┘     └────┬─────┘     └───┬────┘
    │              │              │              │              │
1   │──POST /api/close-position─────>>│              │              │
2   │              │              │              │              │
    │              │──>>查询position───────────>>│              │
3   │              │              │              │   return pos │
    │              │<<──────────────────────────────────│              │
4   │              │              │              │              │
5   │              │         acquire()              │              │
6   │              │────────>>LockInfo────────────>>│              │
7   │              │              │   insert ok            │
    │              │<<──────return True──────────────│              │
8   │              │              │              │              │
    │              │  创建平仓position            │              │
    │              │──>>INSERT position_execute───>>│              │
    │              │              │   insert ok        │
    │              │<<──────return────────────────│              │
    │              │              │              │              │
    │              │  创建平仓batch(es)           │              │
    │              │──>>INSERT batch_execute(s)──>>│              │
    │              │              │   insert ok        │
    │              │<<──────return────────────────│              │
    │              │              │              │
    │              │         release()              │              │
    │              │────────>>UPDATE lock─────────>>│              │
    │              │              │   update ok      │
    │              │<<──────return────────────────│              │
    │              │              │              │              │
    │<<──return 200│              │              │              │
    │              │              │              │              │
    │              │              │              │    后台自动执行
```

#### 3.3.2 平仓调度器执行

```
┌─────────────────┐     ┌──────────┐     ┌────────┐
│ CloseScheduler  │     │  数据库  │     │Trader │
└────────┬────────┘     └────┬─────┘     └──┬────┘
         │
         │ Job: execute_closes (每秒)
         │
         │ 查询平仓批次
         │──>>SELECT batch WHERE status=RUNNING AND offset=CLOSE
         │
         │              │<<──return batches
         │
         ├─►遍历平仓批次
         │   │
         │   ├─超时
         │   │   │──>>UPDATE status=COMPLETED, reason=TIMEOUT
         │   │   │
         │   └─执行平仓
         │       │──>>close_futures_position(symbol, amount)──>>
         │       │                                    │
         │       │                               调用 /fapi/v1/order
         │       │                               (type=MARKET, side=BUY)
         │       │                                    │
         │       │                          <<──return result
         │       │
         │       ├─成功
         │       │   │──>>phase=CLOSED, status=COMPLETED, reason=SUCCESS
         │       │
         │       └─失败
         │           │──>>status=COMPLETED, reason=ERROR
         │
```

### 3.4 资金费率查询流程

```
┌────────┐     ┌─────────┐     ┌──────────┐
│  用户  │     │  API   │     │Collector│
│        │     │Server │     │         │
└───┬────┘     └───┬────┘     └────┬────┘
    │              │              │
1   │──GET /api/funding-rates───────────>>│
2   │              │              │
    │              │──>>get_funding_rates()──>>
    │              │                   │
    │              │           调用 /fapi/v1/premiumIndex
    │              │                   │
    │              │              <<──return rates[]
    │              │                   │
    │              │<<──────return rates│
    │              │              │
3   │<<──return 200│              │
    │ (费率列表)    │              │
```

### 3.5 收益查询流程

```
┌────────┐     ┌─────────┐     ┌──────────┐
│  用户  │     │  API   │     │ 数据库  │
│        │     │Server │     │         │
└───┬────┘     └───┬────┘     └────┬────┘
    │              │              │
1   │──GET /api/earnings────────────>>│
2   │              │              │
    │              │──>>SELECT * FROM earnings────>>│
3   │              │              │   return list│
    │              │<<───────────────────│
4   │<<──return 200│              │
    │ (收益列表)    │              │
```

### 3.6 健康检查流程

```
┌────────┐     ┌─────────┐     ┌──────────┐
│  用户  │     │  API   │     │ 数据库  │
│        │     │Server │     │         │
└───┬────┘     └───┬────┘     └────┬────┘
    │              │              │
1   │──GET /api/health───────────────>>│
2   │              │              │
    │              │  检查数据库
    │              │──>>SELECT 1──>>│
3   │              │     return ok │
    │              │<<──────────│
    │              │              │
    │   ├─成功──>>status="ok", database="healthy"
    │   │
    │   └─失败──>>status="degraded", database="unhealthy"
    │              │
4   │<<──return 200│              │
    │              │
```

### 3.7 订单状态监控流程 (OrderWatcher)

#### 3.7.1 WebSocket 监控模式

```
┌───────────────┐     ┌─────────────┐     ┌──────────┐
│ OrderWatcher │     │WebSocket    │     │Scheduler│
│              │     │Server     │     │         │
└──────┬────────┘     └─────┬─────┘     └────┬────┘
       │
       │  连接WebSocket
       │──>>connect(wss://fstream.binance.com/stream)
       │
       │               │ connected
       │<<─────────────│
       │
       │  订阅订单事件
       │──>>SUBSCRIBE streams: btcusdt@executionReport
       │               │ subscribed
       │<<─────────────│
       │
       │──监听消息
       │
       │               │ 消息: {"e": "executionReport", ...}
       │<<─────────────│
       │
       │  解析消息
       │
       ├─event=executionReport
       │   ├─orderStatus=FILLED
       │   │   │──>>trigger_phase(FIRST_FILLED/COMPLETED)
       │   │   │
       │   ├─orderStatus=CANCELLED
       │   │   │──>>trigger_phase(PENDING)
       │   │   │
       │   └─其他──继续监听
       │
```

#### 3.7.2 轮询备用模式

```
┌───────────────┐     ┌─────────┐
│ OrderWatcher │     │ Trader │
└──────┬──────┘     └──┬──────┘
       │
       │ WebSocket未连接
       │
       │ 启动轮询任务
       │
       │──>>get_order_status(symbol, orderId)
       │               │
       │          调用 /fapi/v1/order
       │          <<──return状态
       │
       │  渐进式等待 (1,1,2,2,5,5,10,30,60秒)
       │
       ├─FILLED──>>触发回调, unwatch
       ├─CANCELLED/EXPIRED──>>触发回调, unwatch
       └─超时──>>触发回调
       │
```

### 3.8 并发控制流程 (LockManager)

#### 3.8.1 获取锁

```
┌──────────┐     ┌──────────┐
│LockManager│     │ 数据库  │
└────┬─────┘     └────┬─────┘
     │
     │  检查是否已锁定
     │──>>SELECT * FROM lock_info WHERE symbol=? AND locked=true
     │
     │              │  return lock / None
     │<<──────────
     │
     ├─有锁──return False
     │
     └─无锁──
         │  创建新锁
         │──>>INSERT INTO lock_info (symbol, operation, locked, locked_at)
         │
         │              │  insert ok
         │<<──────────
         │
         return True
```

#### 3.8.2 释放锁

```
┌──────────┐     ┌──────────┐
│LockManager│     │ 数据库  │
└────┬─────┘     └────┬─────┘
     │
     │  查询锁
     │──>>SELECT * FROM lock_info WHERE symbol=?
     │
     │              │  return lock
     │<<──────────
     │
     │  更新为未锁定
     │──>>UPDATE lock SET locked=false, released_at=NOW()
     │
     │              │  update ok
     │<<──────────
     │
```

---

## 4. 数据库模型

### 4.1 表结构总览

```
┌────────────────────┐
│  position_execute  │ ◄── 主记录表 (开仓/平仓)
└────────┬─────────┘
         │ 1:N
         ▼
┌────────────────────┐
│   batch_execute    │ ◄── 批次表 (执行阶段)
└────────┬─────────┘
         │
         ▼
┌────────────────────┐
│  position_orders  │ ◄── 订单记录表
└────────────────────┘

┌────────────────────┐
│    lock_info      │ ◄── 并发锁表
└────────────────────┘

┌────────────────────┐
│     earning      │ ◄── 收益表
└────────────────────┘

┌────────────────────┐
│ trading_history  │ ◄── 交易历史表
└────────────────────┘
```

### 4.2 表字段详情

#### position_execute (主记录表)
| 字段 | 类型 | 说明 |
|------|------|------|
| id | Integer | 主键 |
| contract | String(20) | 合约symbol (BTCUSDT) |
| batch_num | Integer | 批次数 |
| execute_status | String(20) | PENDING/RUNNING/COMPLETED |
| batch_position_value | Float | 批次开仓价值(USDT) |
| offset | String(10) | OPEN/CLOSE |
| created_at | DateTime | 创建时间 |
| updated_at | DateTime | 更新时间 |
| complete_reason | String(50) | 完成原因(TIMEOUT/SUCCESS/CANCELLED/ERROR) |

#### batch_execute (批次表)
| 字段 | 类型 | 说明 |
|------|------|------|
| id | Integer | 主键 |
| position_execute_id | Integer | 外键 → position_execute |
| timeout | Integer | 超时秒数(默认300) |
| execute_status | String(20) | PENDING/RUNNING/COMPLETED |
| offset | String(10) | OPEN/CLOSE |
| order_sequence | String(20) | FUTURES_FIRST/SPOT_FIRST |
| contract_price | Float | 合约价格 |
| spot_price | Float | 现货价格 |
| batch_value | Float | 本批次价值 |
| phase | String(50) | 当前阶段 |
| first_side_order_id | String(50) | 第一边订单ID |
| first_side_filled_price | Float | 第一边成交价 |
| second_side_order_id | String(50) | 第二边订单ID |
| second_side_filled_price | Float | 第二边成交价 |
| created_at | DateTime | 创建时间 |
| updated_at | DateTime | 更新时间 |
| complete_reason | String(50) | 完成原因 |

#### lock_info (并发锁表)
| 字段 | 类型 | 说明 |
|------|------|------|
| id | Integer | 主键 |
| symbol | String(20) | 合约symbol (唯一) |
| operation | String(10) | OPEN/CLOSE |
| locked | Boolean | 是否锁定 |
| locked_at | DateTime | 锁定时间 |
| released_at | DateTime | 释放时间 |

#### earning (收益表)
| 字段 | 类型 | 说明 |
|------|------|------|
| id | Integer | 主键 |
| contract | String(20) | 合约symbol |
| amount | Float | 仓位价值 |
| funding_earn | Float | 资金费率收益 |
| funding_rate | Float | 当时的资金费率 |
| interest_earn | Float | 理财利息收益 |
| pnl | Float | 价差收益 |
| total_earn | Float | 总收益 |
| status | String(20) | OPEN/CLOSED |
| created_at | DateTime | 创建时间 |
| closed_at | DateTime | 关闭时间 |

---

## 5. API 接口

### 5.1 接口列表

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | /api/open-position | 开仓 |
| POST | /api/close-position | 平仓 |
| GET | /api/funding-rates | 资金费率 |
| GET | /api/positions | 持仓列表 |
| GET | /api/positions/{id} | 持仓详情 |
| GET | /api/positions/{id}/progress | 开仓进度 |
| GET | /api/batch-detail/{id} | 批次详情 |
| GET | /api/earnings | 收益列表 |
| GET | /api/health | 健康检查 |
| GET | /api/plugins | 插件列表 |
| POST | /api/plugins/set | 设置插件 |

### 5.2 接口详情

#### POST /api/open-position
```
Request:
{
    "contract": "BTCUSDT",          // 必填，合约名
    "batch_num": 1,              // 可选，默认1，批次数
    "batch_position_value": 1000,   // 可选，默认1000，每批价值(USDT)
    "order_plugin": "futures_first"  // 可选，订单顺序插件
}

Response:
{
    "status": "success",
    "message": "Position opened: 1, batches: 1"
}
```

#### POST /api/close-position
```
Request:
{
    "position_id": 1,           // 必填，原开仓ID
    "batch_num": 1,          // 可选，默认1
    "batch_position_value": 1000  // 可选，默认1000
}

Response:
{
    "status": "success",
    "message": "Close position created: 2"
}
```

#### GET /api/funding-rates
```
Response:
[
    {
        "symbol": "BTCUSDT",
        "rate": 0.0001,
        "next_funding_time": 1704067200
    }
]
```

#### GET /api/health
```
Response:
{
    "status": "ok",
    "database": "healthy",
    "scheduler": "healthy"
}
```

---

## 6. 配置说明

### 6.1 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| POSTGRES_HOST | localhost | 数据库地址 |
| POSTGRES_PORT | 5432 | 数据库端口 |
| POSTGRES_USER | postgres | 数据库用户 |
| POSTGRES_PASSWORD | postgres | 数据库密码 |
| POSTGRES_DB | arbitrage | 数据库名 |
| BINANCE_API_KEY | - | 币安API Key |
| BINANCE_SECRET_KEY | - | 币安Secret Key |
| CORS_ORIGINS | http://localhost:3000,http://localhost:8000 | CORS允许的源 |
| DB_POOL_SIZE | 5 | 数据库连接池大小 |
| DB_MAX_OVERFLOW | 10 | 最大溢出连接数 |
| LOG_LEVEL | INFO | 日志级别 |

### 6.2 Docker 部署配置

```yaml
# docker-compose.yml
services:
  postgres:
    image: postgres:15-alpine
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: arbitrage
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

  backend:
    build:
      context: ./backend
      dockerfile: Dockerfile
    ports:
      - "8000:8000"
    environment:
      - POSTGRES_HOST=postgres
      - POSTGRES_PORT=5432
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=postgres
      - POSTGRES_DB=arbitrage
      - BINANCE_API_KEY=${BINANCE_API_KEY}
      - BINANCE_SECRET_KEY=${BINANCE_SECRET_KEY}
    depends_on:
      postgres:
        condition: service_healthy
```

---

## 7. 附录

### 7.1 订单阶段状态机

```
┌────────────────────────────────────────────────────────────────────────┐
│                                                                │
│  PENDING ──[INIT]──> FIRST_ORDER_OPEN ──[ORDER]──> FIRST_ORDER_WAIT  │
│                         │                           │            │
│                         │                    [FILLED]▼              │
│                         │                    FIRST_FILLED             │
│                         │                         │                  │
│                         │                   [NEXT]▼              │
│                         │              SECOND_ORDER_OPEN ──[ORDER]──> SECOND_ORDER_WAIT  │
│                         │                         │                │
│                         │                    [FILLED]▼            │
│                         │                    COMPLETED ◄─────┘    │
│                         │                                      │
│                         └────────────[TIMEOUT]──────────────┘  │
│                                                                │
└────────────────────────────────────────────────────────────────────────┘

说明:
  [INIT]   - 初始化参数
  [ORDER]  - 发送订单
  [FILLED] - 订单成交
  [NEXT]  - 进入下一阶段
  [TIMEOUT]- 超时
```

### 7.2 错误码说明

| 错误码 | 说明 |
|--------|------|
| TIMEOUT | 订单执行超时 |
| SUCCESS | 执行成功 |
| CANCELLED | 订单取消 |
| ERROR | 执行错误 |

### 7.3 订单顺序插件

| 插件名 | 说明 |
|--------|------|
| futures_first | 先开合约空仓，再买现货 |
| spot_first | 先买现货，再开合约空仓 |