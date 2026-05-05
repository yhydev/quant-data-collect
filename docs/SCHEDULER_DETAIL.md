# 调度器系统详细技术文档

> 本文档详细介绍 Binance 套利交易平台的调度器核心实现

---

## 目录

1. [系统概述](#1-系统概述)
2. [核心组件](#2-核心组件)
3. [调度器架构](#3-调度器架构)
4. [订单执行流程](#4-订单执行流程)
5. [OrderWatcher 订单监控](#5-orderwatcher-订单监控)
6. [数据模型](#6-数据模型)
7. [状态机](#7-状态机)
8. [时序图详解](#8-时序图详解)
9. [代码细节](#9-代码细节)

---

## 1. 系统概述

### 1.1 设计目标

调度器系统是整个交易平台的核心，负责：
- 自动化执行开仓/平仓批次
- 订单状态实时监控
- 超时处理和错误恢复
- 并发控制

### 1.2 技术栈

| 技术 | 用途 |
|------|------|
| APScheduler | 异步任务调度 |
| AsyncIO | 异步编程 |
| SQLAlchemy 2.0 | 异步数据库操作 |
| WebSocket | 实时订单监控 |

---

## 2. 核心组件

### 2.1 PositionScheduler (开仓调度器)

负责开仓批次的自动化执行，核心功能：

```python
class PositionScheduler:
    def __init__(self, collector_type='binance', 
                 trader_type='binance',
                 order_plugin='futures_first'):
        self.collector = create_collector(collector_type)  # 数据采集
        self.trader = create_trader(trader_type)          # 交易执行
        self.portfolio = PortfolioManager()                # 持仓管理
        self.lock_manager = LockManager()                 # 并发锁
        self.order_plugin = get_plugin(order_plugin)      # 订单顺序插件
        self.order_watcher = SchedulerOrderWatcher(self) # 订单监控
```

**初始化组件说明：**

| 组件 | 作用 |
|------|------|
| Collector | 获取行情数据、资金费率 |
| Trader | 执行交易订单 |
| PortfolioManager | 持仓和收益管理 |
| LockManager | 同一合约并发控制 |
| OrderPlugin | 订单执行顺序策略 |

### 2.2 CloseScheduler (平仓调度器)

负责平仓批次的执行：

```python
class CloseScheduler:
    def __init__(self, collector_type='binance', trader_type='binance'):
        self.collector = create_collector(collector_type)
        self.trader = create_trader(trader_type)
```

### 2.3 OrderWatcher (订单监控)

混合监控模式：WebSocket 优先 + 轮询备用

---

## 3. 调度器架构

### 3.1 任务列表

```
PositionScheduler (开仓调度器)
├── Job1: wake_pending_batches (每秒)
│   └── 唤醒 PENDING 状态的批次
└── Job2: execute_running_batches (每秒)
    └── 执行 RUNNING 状态的批次

CloseScheduler (平仓调度器)
├── Job1: wake_pending_closes (每秒)
│   └── 唤醒待平仓的 PENDING 批次
└── Job2: execute_closes (每秒)
    └── 执行平仓
```

### 3.2 任务触发机制

```python
# 使用 APScheduler 的 IntervalTrigger
trigger=IntervalTrigger(seconds=1)  # 每秒触发

# 任务配置
self.scheduler.add_job(
    self._wake_pending_batches,
    trigger=IntervalTrigger(seconds=1),
    id='wake_pending_batches',
    replace_existing=True  # 防止重复添加
)
```

---

## 4. 订单执行流程

### 4.1 七阶段执行模型

```
PENDING ──[初始化]──> FIRST_ORDER_OPEN ──[挂单]──> FIRST_ORDER_WAIT
     │                                                    │
     │                                             [WebSocket/轮询]
     │                                                    ▼
     │                                              FIRST_FILLED
     │                                                    │
     │                                             [下一阶段]
     ▼                                                    ▼
FIRST_ORDER_OPEN <──[取消重试]── SECOND_ORDER_WAIT ──[成交]── COMPLETED
     │
     ▼
[错误/超时]
     │
     ▼
COMPLETED
```

### 4.2 阶段详解

#### 阶段1: PENDING (初始化参数)

**代码位置**: `_phase_init_params()`

**执行内容**:
1. 获取订单执行顺序 (futures_first / spot_first)
2. 获取合约价格 (Mark Price)
3. 获取现货价格 (Ask/Bid Price)
4. 计算挂单价（含 0.1% 滑点）

**滑点计算逻辑**:

```python
SLIPPAGE = Decimal('0.001')  # 0.1%

if order_seq.value == 'futures_first':
    # 做空：价格稍高，保证成交
    contract_price = float(contract_ticker.mark_price * (1 + SLIPPAGE))
    spot_price_val = float(spot_price.ask_price)
else:
    # 做多：价格稍低，保证成交
    spot_price_val = float(spot_price.ask_price * (1 + SLIPPAGE))
    contract_price = float(contract_ticker.mark_price)
```

**数据库更新**:
```python
batch.order_sequence = order_seq.value
batch.contract_price = contract_price
batch.spot_price = spot_price_val
batch.phase = 'FIRST_ORDER_OPEN'
```

---

#### 阶段2: FIRST_ORDER_OPEN (第一边挂单)

**代码位置**: `_phase_first_order_open()`

**执行逻辑**:

```
futures_first (先开合约空仓):
    1. trader.open_futures_short(symbol, amount, price)
    2. 发送 LIMIT 卖单，开空仓
    3. 保存 order_id 到 first_side_order_id

spot_first (先买现货):
    1. trader.buy_spot(symbol, amount, price)
    2. 发送 LIMIT 买单，买入现货
    3. 保存 order_id 到 first_side_order_id
```

**API 调用**:

```python
# 期货开空仓
POST /fapi/v1/order
{
    "symbol": "BTCUSDT",
    "side": "SELL",
    "positionSide": "SHORT",
    "type": "LIMIT",
    "timeInForce": "GTC",
    "quantity": "0.001",
    "price": "50000.00",
    "timestamp": 1704067200000,
    "signature": "..."
}

# 现货买入
POST /api/v3/order
{
    "symbol": "BTCUSDT",
    "side": "BUY",
    "type": "LIMIT", 
    "timeInForce": "GTC",
    "quantity": "0.001",
    "price": "50000.00",
    "timestamp": 1704067200000,
    "signature": "..."
}
```

**成功响应处理**:
```python
if result.success:
    batch.first_side_order_id = str(result.order_id)
    batch.phase = 'FIRST_ORDER_WAIT'  # 进入等待成交阶段
else:
    batch.execute_status = 'COMPLETED'
    batch.complete_reason = f'ERROR: {result.message}'
```

---

#### 阶段3: FIRST_ORDER_WAIT (等待第一边成交)

**代码位置**: `_phase_first_order_wait()`

**核心逻辑**:
- 将订单交给 OrderWatcher 监控
- 不阻塞调度器主流程
- WebSocket 实时推送 / 轮询备选

**调用 OrderWatcher**:
```python
await self.order_watcher.watch_order(
    batch_id=batch.id,
    order_id=order_id,
    symbol=contract,
    phase='FIRST_ORDER_WAIT',
    timeout=timeout  # 默认300秒
)
```

---

#### 阶段4: FIRST_FILLED (第一边已成交)

**触发条件**: OrderWatcher 检测到订单 FILLED 状态

**执行内容**:
- 更新 first_side_filled_price
- 进入第二边挂单阶段

```python
batch.phase = 'SECOND_ORDER_OPEN'
```

---

#### 阶段5: SECOND_ORDER_OPEN (第二边挂单)

**代码位置**: `_phase_second_order_open()`

**执行逻辑** (与第一边相反):

```
futures_first (第一边已开空仓):
    1. trader.buy_spot(symbol, amount, price)
    2. 买入现货对冲
    
spot_first (第一边已买现货):
    1. trader.open_futures_short(symbol, amount, price)
    2. 开合约空仓对冲
```

---

#### 阶段6: SECOND_ORDER_WAIT (等待第二边成交)

**代码位置**: `_phase_second_order_wait()`

**与阶段3相同**，交给 OrderWatcher 监控

---

#### 阶段7: COMPLETED (完成)

**触发条件**: OrderWatcher 检测到第二边成交

**执行内容**:

```python
# 1. 更新成交价
batch.second_side_filled_price = filled_price

# 2. 转入理财 (赚取利息)
transfer_result = await self.trader.transfer_to_savings(
    symbol,
    amount
)

# 3. 标记完成
batch.execute_status = 'COMPLETED'
batch.complete_reason = 'SUCCESS'
batch.phase = 'COMPLETED'

# 4. 检查主记录完成状态
await self._check_position_complete(position_id)
```

**转账 API**:
```
POST /api/v3/asset/transfer
{
    "asset": "BTC",
    "amount": "0.001",
    "type": "1",  // 1 = 主账户转理财
    "timestamp": 1704067200000,
    "signature": "..."
}
```

---

## 5. OrderWatcher 订单监控

### 5.1 设计目标

- **实时性**: WebSocket 推送，毫秒级响应
- **可靠性**: WebSocket 断开时自动切换轮询
- **容错性**: 渐进式轮询间隔，避免过度请求

### 5.2 架构图

```
┌─────────────────────────────────────────────────────────────┐
│                     OrderWatcher                           │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────┐     ┌──────────────┐                   │
│  │   WebSocket  │     │   Polling    │                   │
│  │   (优先)     │     │   (备用)     │                   │
│  └──────┬───────┘     └──────┬───────┘                   │
│         │                    │                             │
│         │  连接断开时        │ 启动轮询                    │
│         └────────┬─────────┘                             │
│                  │                                        │
│         ┌────────▼────────┐                             │
│         │   统一回调处理   │                             │
│         │ _on_order_update │                            │
│         └────────┬────────┘                             │
│                  │                                        │
│         ┌────────▼────────┐                             │
│         │  Phase 转换     │                             │
│         │ trigger_phase() │                             │
│         └─────────────────┘                             │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 5.3 配置参数

```python
@dataclass
class WatcherConfig:
    # WebSocket
    ws_url: str = "wss://fstream.binance.com/stream"
    ws_reconnect_interval: int = 5   # 断开后5秒重连
    ws_ping_interval: int = 30      # 心跳间隔
    
    # 轮询备用
    use_polling_fallback: bool = True
    polling_intervals: list = [1, 1, 2, 2, 5, 5, 10, 30, 60]
    # 渐进式: 1s, 1s, 2s, 2s, 5s, 5s, 10s, 30s, 60s
    
    # 超时
    default_timeout: int = 300  # 5分钟
```

### 5.4 WebSocket 消息处理

**订阅格式**:
```python
{
    "method": "SUBSCRIBE",
    "params": ["btcusdt@executionReport"],
    "id": 1704067200000
}
```

**接收消息格式**:
```json
{
    "e": "executionReport",
    "s": "BTCUSDT",
    "i": 123456789,
    "X": "FILLED",  // NEW, PARTIALLY_FILLED, FILLED, CANCELLED
    "L": "50000.00",  // lastPrice
    "z": "0.001",     // cumulativeFilledQty
    "ap": "50000.00"  // avgPrice
}
```

### 5.5 状态映射

```python
# Binance API 状态码 -> 内部状态
'NEW'           -> PENDING
'PARTIALLY_FILLED' -> PARTIALLY_FILLED
'FILLED'        -> FILLED
'CANCELLED'     -> CANCELLED
'EXPIRED'       -> EXPIRED
'REJECTED'      -> REJECTED
```

### 5.6 轮询间隔策略

```python
polling_intervals = [1, 1, 2, 2, 5, 5, 10, 30, 60]

# 解释:
# 第1次: 等待1秒
# 第2次: 等待1秒 (累计2秒)
# 第3次: 等待2秒 (累计4秒)
# 第4次: 等待2秒 (累计6秒)
# 第5次: 等待5秒 (累计11秒)
# ... 以此类推
# 最后: 每60秒轮询一次，直到超时
```

---

## 6. 数据模型

### 6.1 BatchExecute (批次执行表)

```python
class BatchExecute(Base):
    # 主键
    id: int
    
    # 外键
    position_execute_id: int  # 关联主记录
    
    # 执行控制
    timeout: int = 300       # 超时时间(秒)
    execute_status: str       # PENDING | RUNNING | COMPLETED
    offset: str              # OPEN | CLOSE
    
    # 订单参数
    order_sequence: str       # futures_first | spot_first
    contract_price: float     # 合约挂单价
    spot_price: float        # 现货挂单价
    batch_value: float        # 本批次价值
    
    # 阶段控制
    phase: str               # 当前阶段
    
    # 订单ID记录
    first_side_order_id: str       # 第一边订单ID
    first_side_filled_price: float # 第一边成交价
    second_side_order_id: str      # 第二边订单ID
    second_side_filled_price: float # 第二边成交价
    
    # 完成信息
    complete_reason: str     # TIMEOUT | SUCCESS | CANCELLED | ERROR
```

### 6.2 状态流转图

```
execute_status (执行状态)
├── PENDING    (待执行)
├── RUNNING    (执行中)
└── COMPLETED  (已完成)

phase (执行阶段)
├── PENDING              (初始化)
├── FIRST_ORDER_OPEN     (第一边挂单中)
├── FIRST_ORDER_WAIT     (等待第一边成交)
├── FIRST_FILLED         (第一边已成交)
├── SECOND_ORDER_OPEN    (第二边挂单中)
├── SECOND_ORDER_WAIT    (等待第二边成交)
└── COMPLETED            (完成)

complete_reason (完成原因)
├── TIMEOUT    (超时)
├── SUCCESS    (成功)
├── CANCELLED  (取消)
└── ERROR:xxx  (错误:具体错误信息)
```

---

## 7. 状态机

### 7.1 完整状态图

```
PENDING ──────────────────────────────┐
   │                                    │
   │ INIT (初始化参数)                   │
   ▼                                    │
FIRST_ORDER_OPEN                       │
   │                                    │
   │ ORDER (发送订单)                    │
   ▼                                    │
FIRST_ORDER_WAIT                       │
   │                                    │
   ├─[FILLED]─────> FIRST_FILLED       │
   │                        │            │
   │                        │ NEXT       │
   │                        ▼            │
   ├─[CANCELLED]─────> PENDING (重试)   │
   │                        │            │
   │                        │ TIMEOUT    │
   └─[TIMEOUT]──────────> COMPLETED ◄───┘
                                    ▲
                                    │
                              [第二边成交]
                                    │
SECOND_ORDER_WAIT ───────────────────┘
   │
   ├─[FILLED]─────> COMPLETED
   │
   ├─[CANCELLED]─> FIRST_FILLED
   │
   └─[TIMEOUT]───> COMPLETED
```

### 7.2 状态转换条件

| 当前状态 | 事件 | 下一状态 | 说明 |
|---------|------|---------|------|
| PENDING | INIT | FIRST_ORDER_OPEN | 初始化参数 |
| PENDING | TIMEOUT | COMPLETED | 初始化超时 |
| FIRST_ORDER_OPEN | ORDER | FIRST_ORDER_WAIT | 订单已发送 |
| FIRST_ORDER_OPEN | ERROR | COMPLETED | 挂单失败 |
| FIRST_ORDER_WAIT | FILLED | FIRST_FILLED | 第一边成交 |
| FIRST_ORDER_WAIT | CANCELLED | PENDING | 取消重试 |
| FIRST_ORDER_WAIT | TIMEOUT | COMPLETED | 等待超时 |
| FIRST_FILLED | NEXT | SECOND_ORDER_OPEN | 进入第二阶段 |
| SECOND_ORDER_OPEN | ORDER | SECOND_ORDER_WAIT | 订单已发送 |
| SECOND_ORDER_OPEN | ERROR | COMPLETED | 挂单失败 |
| SECOND_ORDER_WAIT | FILLED | COMPLETED | 完成(成功) |
| SECOND_ORDER_WAIT | CANCELLED | FIRST_FILLED | 回到第一阶段 |
| SECOND_ORDER_WAIT | TIMEOUT | COMPLETED | 等待超时 |

---

## 8. 时序图详解

### 8.1 开仓完整时序

```
用户                    API服务器              数据库              调度器              交易所              OrderWatcher
  │                      │                    │                   │                    │                    │
  │ POST /open-position │                    │                   │                    │                    │
  │────────────────────>>│                    │                   │                    │                    │
  │                      │ 创建PositionExecute│                   │                    │                    │
  │                      │───────────────────>>│                   │                    │                    │
  │                      │                    │                   │                    │                    │
  │                      │ 创建BatchExecute   │                   │                    │                    │
  │                      │───────────────────>>│                   │                    │                    │
  │                      │                    │                   │                    │                    │
  │                      │<<──────────────────│                   │                    │                    │
  │                      │                    │                   │                    │                    │
  │<<────────────────────│                    │                   │                    │                    │
  │                      │                    │                   │                    │                    │
  │                      │                    │  每秒:查询PENDING │                    │                    │
  │                      │                    │<<─────────────────│                    │                    │
  │                      │                    │   返回批次列表     │                    │                    │
  │                      │                    │───────────────────>>│                    │                    │
  │                      │                    │                   │                    │                    │
  │                      │                    │  更新为RUNNING   │                    │                    │
  │                      │                    │<<─────────────────│                    │                    │
  │                      │                    │                   │                    │                    │
  │                      │                    │                   │ 获取合约价格      │                    │
  │                      │                    │                   │──────────────────>>│                    │
  │                      │                    │                   │                    │                    │
  │                      │                    │                   │<<─────────────────│                    │
  │                      │                    │                   │                    │                    │
  │                      │                    │                   │ 获取现货价格      │                    │
  │                      │                    │                   │──────────────────>>│                    │
  │                      │                    │                   │                    │                    │
  │                      │                    │                   │<<─────────────────│                    │
  │                      │                    │                   │                    │                    │
  │                      │                    │                   │ 开第一边订单      │                    │
  │                      │                    │                   │──────────────────>>│                    │
  │                      │                    │                   │                    │                    │
  │                      │                    │                   │<<─────────────────│                    │
  │                      │                    │                   │                    │                    │
  │                      │                    │                   │ 更新phase=WAYT   │                    │
  │                      │                    │                   │<<─────────────────│                    │
  │                      │                    │                   │                    │                    │
  │                      │                    │                   │  注册订单监控    │                    │
  │                      │                    │                   │───────────────────────────>>│
  │                      │                    │                   │                    │                    │
  │                      │                    │                   │                    │  WebSocket监听    │
  │                      │                    │                   │                    │◄───────────────────│
  │                      │                    │                   │                    │                    │
  │                      │                    │                   │                    │  订单成交推送    │
  │                      │                    │                   │                    │◄───────────────────│
  │                      │                    │                   │                    │                    │
  │                      │                    │                   │  触发阶段转换    │                    │
  │                      │                    │                   │<<─────────────────────────────│
  │                      │                    │                   │                    │                    │
  │                      │                    │                   │ 开第二边订单     │                    │
  │                      │                    │                   │──────────────────>>│                    │
  │                      │                    │                   │                    │                    │
  │                      │                    │                   │<<─────────────────│                    │
  │                      │                    │                   │                    │                    │
  │                      │                    │                   │  再次注册监控    │                    │
  │                      │                    │                   │───────────────────────────>>│
  │                      │                    │                   │                    │                    │
  │                      │                    │                   │                    │  第二边成交      │
  │                      │                    │                   │                    │◄───────────────────│
  │                      │                    │                   │                    │                    │
  │                      │                    │                   │  转入理财        │                    │
  │                      │                    │                   │──────────────────>>│                    │
  │                      │                    │                   │                    │                    │
  │                      │                    │                   │<<─────────────────│                    │
  │                      │                    │                   │                    │                    │
  │                      │                    │                   │  标记COMPLETED  │                    │
  │                      │                    │                   │<<─────────────────│                    │
```

---

## 9. 代码细节

### 9.1 核心配置

```python
# 滑点配置 (0.1%)
SLIPPAGE = Decimal('0.001')

# 默认订单超时 (5分钟)
DEFAULT_ORDER_TIMEOUT = 300
```

### 9.2 并发控制逻辑

**同一合约同时只能执行一个批次**:

```python
# 获取已有RUNNING的合约
contracts_running = set()
for batch in running:
    contracts_running.add(batch.position.contract)

# 按ID排序，唤醒最小的
for batch in pending:
    if batch.position.contract in contracts_running:
        continue  # 该合约已在执行，跳过
    
    # 唤醒
    batch.execute_status = 'RUNNING'
    contracts_running.add(batch.position.contract)
```

### 9.3 超时检测

```python
elapsed = (datetime.utcnow() - batch.updated_at).total_seconds()
if elapsed > batch.timeout:
    batch.execute_status = 'COMPLETED'
    batch.complete_reason = 'TIMEOUT'
```

### 9.4 错误处理

```python
try:
    await self._execute_phase(batch)
except Exception as e:
    print(f"Error executing batch {batch.id}: {e}")
    batch.execute_status = 'COMPLETED'
    batch.complete_reason = f'ERROR: {str(e)}'
```

### 9.5 主记录完成检测

```python
async def _check_position_complete(self, position_id: int):
    # 查询所有关联批次
    batches = await session.execute(
        select(BatchExecute).where(
            BatchExecute.position_execute_id == position_id
        )
    )
    
    # 判断完成原因优先级
    if all(b.execute_status == 'COMPLETED' for b in batches):
        reasons = [b.complete_reason for b in batches]
        
        if 'TIMEOUT' in reasons:
            overall = 'TIMEOUT'
        elif any('ERROR' in r for r in reasons):
            overall = 'ERROR'
        else:
            overall = 'SUCCESS'
        
        # 更新主记录
        pos.execute_status = 'COMPLETED'
        pos.complete_reason = overall
        
        # 释放锁
        await self.lock_manager.release(pos.contract)
```

---

## 10. 启动流程

### 10.1 FastAPI Lifespan 启动

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动
    await init_db_async()           # 初始化数据库
    
    position_scheduler = PositionScheduler()
    position_scheduler.start()      # 启动开仓调度器
    
    close_scheduler = CloseScheduler()
    close_scheduler.start()          # 启动平仓调度器
    
    yield
    
    # 关闭
    await position_scheduler.stop()
    await close_scheduler.stop()
```

### 10.2 调度器启动

```python
def start(self):
    # 1. 启动 OrderWatcher
    asyncio.create_task(self.order_watcher.start())
    
    # 2. 添加唤醒任务
    self.scheduler.add_job(
        self._wake_pending_batches,
        trigger=IntervalTrigger(seconds=1),
        id='wake_pending_batches'
    )
    
    # 3. 添加执行任务
    self.scheduler.add_job(
        self._execute_running_batches,
        trigger=IntervalTrigger(seconds=1),
        id='execute_running_batches'
    )
    
    # 4. 启动调度器
    self.scheduler.start()
```

---

## 附录

### A. 配置文件

| 参数 | 默认值 | 说明 |
|------|--------|------|
| SLIPPAGE | 0.001 | 滑点 (0.1%) |
| DEFAULT_ORDER_TIMEOUT | 300 | 默认超时 (秒) |
| ws_reconnect_interval | 5 | WebSocket 重连间隔 |
| ws_ping_interval | 30 | WebSocket 心跳间隔 |
| default_timeout | 300 | 订单监控超时 |

### B. 日志输出示例

```
Batch 1 woken: contract=BTCUSDT
Batch 1 params: order=futures_first, contract=50000.0, spot=50100.0
Batch 1 phase transition: FIRST_ORDER_OPEN -> FIRST_ORDER_WAIT
Batch 1 phase transition: FIRST_ORDER_WAIT -> FIRST_FILLED
Batch 1 phase transition: SECOND_ORDER_OPEN -> SECOND_ORDER_WAIT
Batch 1 completed: SUCCESS
```

### C. 常见问题排查

| 问题 | 可能原因 | 解决方案 |
|------|---------|---------|
| 批次一直PENDING | 调度器未启动 | 检查启动日志 |
| 订单超时 | 网络延迟/价格不利 | 检查超时配置 |
| WebSocket断开 | 网络不稳定 | 等待自动重连 |
| 并发执行 | 锁未正确释放 | 检查lock_manager |