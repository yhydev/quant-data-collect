# Binance 套利交易平台 - 详细技术文档

> 📚 本文档包含专业的架构图和时序图  
> 支持 Mermaid.js 实时渲染和 draw.io 编辑

---

## 目录
1. [系统架构图](#1-系统架构图)
2. [时序图 (Mermaid)](#2-时序图-mermaid)
3. [流程图](#3-流程图)
4. [数据库模型](#4-数据库模型)
5. [状态机](#5-状态机)
6. [draw.io 图表源文件](#6-drawio-图表源文件)

---

## 1. 系统架构图

### 1.1 整体系统架构

```mermaid
flowchart TB
    subgraph CLIENT["用户端"]
        USER["用户前端"]
    end
    
    subgraph API_SERVER["API 服务器"]
        FASTAPI["FastAPI 应用"]
        
        subgraph API_ROUTES["API 路由"]
            A1["/open-position"]
            A2["/close-position"]
            A3["/funding-rates"]
            A4["/positions"]
            A5["/health"]
            A6["/earnings"]
        end
    end
    
    subgraph SCHEDULER["调度层"]
        PS["PositionScheduler<br/>开仓调度器"]
        CS["CloseScheduler<br/>平仓调度器"]
        OW["OrderWatcher<br/>订单监控"]
    end
    
    subgraph SERVICE["服务层"]
        LM["LockManager<br/>并发锁管理"]
        PM["PortfolioManager<br/>持仓管理"]
    end
    
    subgraph DATA["数据层"]
        DB["PostgreSQL<br/>+ SQLAlchemy AsyncORM"]
    end
    
    subgraph EXTERNAL["外部服务"]
        BINANCE["Binance 交易所"]
        
        subgraph BINANCE_API["Binance API"]
            BC["Collector<br/>数据采集"]
            BT["Trader<br/>交易执行"]
            BWS["WebSocket<br/>实时监控"]
        end
    end
    
    USER -->|HTTP| FASTAPI
    FASTAPI --> A1
    FASTAPI --> A2
    FASTAPI --> A3
    FASTAPI --> A4
    FASTAPI --> A5
    FASTAPI --> A6
    
    A1 --> PS
    A2 --> CS
    PS <--> LM
    CS <--> LM
    PS <--> PM
    CS <--> PM
    
    PM <--> DB
    
    PS --> OW
    OW <--> BT
    
    BC <--> BT
    BT <--> BWS
    
    BC -->|REST API| BINANCE
    BT -->|REST API| BINANCE
    BWS -->|WebSocket| BINANCE

    style CLIENT fill:#e1f5fe
    style API_SERVER fill:#e8f5e8
    style SCHEDULER fill:#fff3e0
    style SERVICE fill:#f3e5f5
    style DATA fill:#e0f2f1
    style EXTERNAL fill:#ffebee
```

### 1.2 调度器内部架构

```mermaid
flowchart TB
    subgraph APSCHEDULER["APScheduler 调度框架"]
        subgraph POSITION["PositionScheduler"]
            J1["Job1: wake_pending_batches<br/>唤醒待执行批次"]
            J2["Job2: execute_running_batches<br/>执行批次任务"]
            OW["OrderWatcher<br/>订单状态监控"]
        end
        
        subgraph CLOSE["CloseScheduler"]
            J3["Job1: wake_pending_closes<br/>唤醒待平仓"]
            J4["Job2: execute_closes<br/>执行平仓"]
        end
    end
    
    subgraph PHASES["执行阶段"]
        P1["PENDING<br/>初始化"]
        P2["FIRST_ORDER_OPEN<br/>开第一边"]
        P3["FIRST_ORDER_WAIT<br/>等待第一边"]
        P4["FIRST_FILLED<br/>第一边成交"]
        P5["SECOND_ORDER_OPEN<br/>开第二边"]
        P6["SECOND_ORDER_WAIT<br/>等待第二边"]
        P7["COMPLETED<br/>完成"]
    end
    
    J1 -->|每秒触发| J2
    J2 -->|监控| OW
    J3 -->|每秒触发| J4
    
    P1 --> P2
    P2 --> P3
    P3 --> P4
    P4 --> P5
    P5 --> P6
    P6 --> P7

    style APSCHEDULER fill:#fff3e0,stroke:#ff9800
    style PHASES fill:#e8f5e8,stroke:#4caf50
```

---

## 2. 时序图 (Mermaid)

### 2.1 开仓完整流程

```mermaid
sequenceDiagram
    participant USER as 用户
    participant API as API服务器
    participant DB as 数据库
    participant LM as LockManager
    participant PS as PositionScheduler
    participant C as Collector
    participant T as Trader
    participant OW as OrderWatcher

    %% 步骤1: 用户发起开仓请求
    USER->>API: POST /api/open-position
    API->>LM: acquire(contract, "OPEN")
    alt 锁获取成功
        API->>DB: INSERT PositionExecute
        API->>DB: INSERT BatchExecute(s)
        DB-->>API: 返回记录
        API->>LM: release(contract)
        API-->>USER: 200 OK
    else 锁获取失败
        API-->>USER: 400 已被锁定
    end

    %% 步骤2: 后台调度器唤醒批次
    activate PS
    PS->>DB: SELECT PENDING batches
    DB-->>PS: 返回批次列表
    PS->>DB: UPDATE status=RUNNING, phase=PENDING
    deactivate PS

    %% 步骤3: 执行批次
    activate PS
    PS->>C: get_contract_ticker(symbol)
    C-->>PS: 返回行情
    PS->>C: get_spot_price(symbol)
    C-->>PS: 返回现货价
    PS->>DB: UPDATE 价格

    %% 步骤4: 开第一边订单
    PS->>T: open_futures_short/buy_spot
    T-->>PS: 返回order_id
    PS->>DB: UPDATE phase=FIRST_ORDER_WAIT

    %% 步骤5: 监控订单
    PS->>OW: watch_order()
    activate OW
    loop 轮询/WebSocket
        OW->>T: get_order_status()
        T-->>OW: 状态
    end
    OW-->>PS: trigger_phase(FIRST_FILLED)
    deactivate OW

    %% 步骤6: 开第二边订单
    PS->>T: buy_spot/open_futures_short
    T-->>PS: 返回order_id
    PS->>DB: UPDATE phase=SECOND_ORDER_WAIT

    %% 步骤7: 平仓监控
    activate OW
    loop 轮询/WebSocket
        OW->>T: get_order_status()
        T-->>OW: 状态
    end
    OW-->>PS: trigger_phase(COMPLETED)
    deactivate OW

    %% 步骤8: 完成
    PS->>T: transfer_to_savings()
    PS->>DB: UPDATE COMPLETED
    PS->>LM: release(contract)
```

### 2.2 订单执行阶段 - 时序图详解

#### 阶段1: 初始化参数

```mermaid
sequenceDiagram
    participant S as Scheduler
    participant DB as 数据库
    participant C as Collector

    S->>DB: 读取order_sequence配置
    DB-->>S: 返回配置
    
    S->>C: get_contract_ticker(symbol)
    C-->>S: mark_price, index_price
    
    S->>C: get_spot_price(symbol) 
    C-->>S: bid_price, ask_price
    
    S->>DB: UPDATE batch<br/>contract_price=50000<br/>spot_price=50100<br/>phase=FIRST_ORDER_OPEN
    DB-->>S: 更新成功
```

#### 阶段2: 开第一边订单

```mermaid
sequenceDiagram
    participant S as Scheduler
    participant DB as 数据库
    participant T as Trader
    participant API as Binance API

    S->>DB: SELECT order_sequence, contract_price
    DB-->>S: order_sequence="futures_first"
    
    alt futures_first
        S->>T: open_futures_short(symbol, amount, price)
        T->>API: POST /fapi/v1/order
        API-->>T: {orderId: 123}
        T-->>S: TradeResult(success=true, order_id=123)
    else spot_first
        S->>T: buy_spot(symbol, amount, price)
        T->>API: POST /api/v3/order
        API-->>T: {orderId: 456}
        T-->>S: TradeResult(success=true, order_id=456)
    end
    
    alt 订单成功
        S->>DB: UPDATE first_side_order_id=?, phase=FIRST_ORDER_WAIT
    else 订单失败
        S->>DB: UPDATE execute_status=COMPLETED, complete_reason=ERROR
    end
```

#### 阶段3-6: 等待成交循环

```mermaid
sequenceDiagram
    participant S as Scheduler
    participant OW as OrderWatcher
    participant T as Trader
    participant API as Binance API
    participant DB as 数据库

    S->>OW: watch_order(batch_id, order_id, phase, timeout)
    
    OW->>T: get_order_status(symbol, orderId)
    T->>API: GET /fapi/v1/order
    API-->>T: {status: "NEW"}
    T-->>OW: status="NEW"
    
    loop 渐进式轮询 [1,1,2,2,5,5,10,30,60]秒
        OW->>T: get_order_status()
        alt 状态 = FILLED
            OW->>S: trigger_phase(FIRST_FILLED, avgPrice)
            S->>DB: UPDATE phase=FIRST_FILLED
        else 状态 = CANCELLED/REJECTED
            OW->>S: trigger_phase(PENDING)
            S->>DB: UPDATE phase=PENDING
        end
    end
```

#### 阶段7: 完成并转入理财

```mermaid
sequenceDiagram
    participant S as Scheduler
    participant DB as 数据库
    participant T as Trader
    participant API as Binance API
    participant LM as LockManager

    S->>DB: UPDATE second_side_filled_price
    S->>DB: UPDATE execute_status=COMPLETED<br/>complete_reason=SUCCESS
    
    S->>T: transfer_to_savings(symbol, amount)
    T->>API: POST /api/v3/asset/transfer
    API-->>T: success
    T-->>S: TradeResult(success=true)
    
    S->>DB: SELECT batches WHERE position_id=?
    alt 所有batch已完珵
        S->>DB: UPDATE position status=COMPLETED
        S->>LM: release(contract)
    end
```

### 2.3 平仓流程

```mermaid
sequenceDiagram
    participant USER as 用户
    participant API as API服务器
    participant DB as 数据库
    participant LM as LockManager
    participant CS as CloseScheduler
    participant T as Trader
    participant API_BINANCE as Binance API

    USER->>API: POST /api/close-position
    API->>DB: SELECT position_execute WHERE id=?
    DB-->>API: 返回原持仓
    
    API->>LM: acquire(contract, "CLOSE")
    alt 锁获取成功
        API->>DB: INSERT close_position_execute
        API->>DB: INSERT close_batch_execute(s)
        API->>LM: release(contract)
        API-->>USER: 200 OK
    else 锁获取失败
        API-->>USER: 400 已被锁定
    end
    
    activate CS
    CS->>DB: SELECT RUNNING batches WHERE offset=CLOSE
    
    loop 遍历平仓批次
        alt 超时
            CS->>DB: UPDATE TIMEOUT
        else 执行平仓
            CS->>T: close_futures_position(symbol, amount)
            T->>API_BINANCE: POST /fapi/v1/order (MARKET)
            API_BINANCE-->>T: success
            T-->>CS: result
            
            alt 成功
                CS->>DB: UPDATE CLOSED, COMPLETED, SUCCESS
            else 失败
                CS->>DB: UPDATE COMPLETED, ERROR
            end
        end
    end
    deactivate CS
```

### 2.4 OrderWatcher 监控流程 (WebSocket + 轮询)

```mermaid
sequenceDiagram
    participant OW as OrderWatcher
    participant WS as Binance WebSocket
    participant T as Trader
    participant S as Scheduler

    %% WebSocket优先
    OW->>WS: connect(wss://fstream.binance.com/stream)
    WS-->>OW: connected
    
    OW->>WS: SUBSCRIBE symbol@executionReport
    WS-->>OW: subscribed
    
    loop 监听消息
        WS->>OW: 消息
        OW->>OW: 解析event
        
        alt event=executionReport
            alt orderStatus=FILLED
                OW->>S: trigger_phase(FIRST_FILLED/COMPLETED)
            else orderStatus=CANCELLED
                OW->>S: trigger_phase(PENDING)
            end
        end
    end
    
    %% 断开时轮询备用
    note over OW: WebSocket断开时
    
    loop 轮询 [1,1,2,2,5,5,10,30,60]秒
        OW->>T: get_order_status()
        alt status=FILLED
            OW->>S: trigger_phase()
            OW->>S: unwatch()
        else status=CANCELLED
            OW->>S: trigger_phase(PENDING)
            OW->>S: unwatch()
        end
    end
```

### 2.5 并发锁流程

```mermaid
sequenceDiagram
    participant LM as LockManager
    participant DB as 数据库
    participant OP as 操作请求

    %% 获取锁
    OP->>LM: acquire("BTCUSDT", "OPEN")
    LM->>DB: SELECT locked=true WHERE symbol="BTCUSDT"
    
    alt 已有锁
        DB-->>LM: 返回lock记录
        LM-->>OP: return False
    else 无锁
        DB-->>LM: return None
        LM->>DB: INSERT lock_info(symbol, operation, locked=true)
        LM-->>OP: return True
    end

    %% 释放锁
    OP->>LM: release("BTCUSDT")
    LM->>DB: SELECT WHERE symbol="BTCUSDT"
    DB-->>LM: 返回lock
    
    alt 找到记录
        LM->>DB: UPDATE locked=false, released_at=NOW()
    end
```

### 2.6 健康检查流程

```mermaid
sequenceDiagram
    participant USER as 用户
    participant API as API服务器
    participant DB as 数据库

    USER->>API: GET /api/health
    
    alt 数据库正常
        API->>DB: SELECT 1
        DB-->>API: success
        API-->>USER: {status: "ok", database: "healthy"}
    else 数据库异常
        API->>DB: SELECT 1
        DB-->>API: failed
        API-->>USER: {status: "degraded", database: "unhealthy"}
    end
```

---

## 3. 流程图

### 3.1 开仓业务流

```mermaid
flowchart TD
    A[用户 POST /api/open-position] --> B{锁可用?}
    B -->|否| E[返回 400 错误]
    B -->|是| C[获取锁]
    C --> D[创建 PositionExecute]
    D --> F[创建 BatchExecute]
    F --> G[释放锁]
    G --> H[返回成功]
    
    H -.-> I[调度器后台执行]
    
    I --> J[唤醒 PENDING 批次]
    J --> K[获取行情]
    K --> L[开第一边订单]
    L --> M[等待成交]
    M --> N{第一边成交?}
    N -->|否| M
    N -->|是| O[开第二边订单]
    O --> P[等待成交]
    P --> Q{第二边成交?}
    Q -->|否| P
    Q -->|是| R[转入理财]
    R --> S[标记完成]
    S --> T[释放锁]
```

### 3.2 订单执行状态机

```mermaid
stateDiagram-v2
    [*] --> PENDING: 用户开仓
    
    PENDING --> FIRST_ORDER_OPEN: 初始参数
    FIRST_ORDER_OPEN --> FIRST_ORDER_WAIT: 开单
    
    FIRST_ORDER_WAIT --> FIRST_ORDER_WAIT: 轮询中
    FIRST_ORDER_WAIT --> FIRST_FILLED: 成交
    FIRST_ORDER_WAIT --> PENDING: 被取消
    
    FIRST_FILLED --> SECOND_ORDER_OPEN: 进入第二阶段
    
    SECOND_ORDER_OPEN --> SECOND_ORDER_WAIT: 开单
    
    SECOND_ORDER_WAIT --> SECOND_ORDER_WAIT: 轮询中
    SECOND_ORDER_WAIT --> COMPLETED: 成交
    SECOND_ORDER_WAIT --> PENDING: 被取消
    
    COMPLETED --> [*]: 完成
    
    %% 超时分支
    FIRST_ORDER_WAIT --> COMPLETED: 超时
    SECOND_ORDER_WAIT --> COMPLETED: 超时
    
    %% 错误分支
    FIRST_ORDER_OPEN --> COMPLETED: 错误
    SECOND_ORDER_OPEN --> COMPLETED: 错误
```

---

## 4. 数据库模型

### 4.1 ER 图

```mermaid
erDiagram
    POSITION_EXECUTE ||--o{ BATCH_EXECUTE : contains
    BATCH_EXECUTE ||--o{ POSITION_ORDER : has
    POSITION_EXECUTE {
        int id PK
        string contract
        int batch_num
        string execute_status
        float batch_position_value
        string offset
        datetime created_at
        datetime updated_at
        string complete_reason
    }
    
    BATCH_EXECUTE {
        int id PK
        int position_execute_id FK
        int timeout
        string execute_status
        string offset
        string order_sequence
        float contract_price
        float spot_price
        float batch_value
        string phase
        string first_side_order_id
        float first_side_filled_price
        string second_side_order_id
        float second_side_filled_price
    }
    
    LOCK_INFO {
        int id PK
        string symbol UK
        string operation
        boolean locked
        datetime locked_at
        datetime released_at
    }
    
    EARNING {
        int id PK
        string contract
        float amount
        float funding_earn
        float funding_rate
        float interest_earn
        float pnl
        float total_earn
        string status
    }
    
    TRADING_HISTORY {
        int id PK
        string contract
        string side
        string order_id
        float price
        float amount
        float value
        float fee
    }
```

---

## 5. 状态机

### 5.1 订单执行阶段

```
┌─────────────────────────────────────────────────────────────────────┐
│                         订单执行状态机                                 │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌─────────┐    POST     ┌──────────────────┐    ORDER    ┌─────────────┐│
│  │ PENDING │ ────────>> │ FIRST_ORDER_OPEN │ ────────>> │ FIRST_ORDER ││
│  └─────────┘          └──────────────────┘           │   _WAIT   ││
│       ^                      │                      └─────┬──────┘│
│       │                     │                            │       │
│       │                SUCCESS                        FILLED ▼       │
│       │                                                  │       │
│       │  ┌──────────────────────────────────────────────┘       │
│       │  │                                                  │
│       │  ▼                                              ▼       │
│       │ ┌──────────────────┐   ORDER    ┌─────────────────┐       │
│       │ │ FIRST_FILLED    │ ────────>> │ SECOND_ORDER   │       │
│       │ └──────────────────┘           │     _OPEN    │       │
│       │                      │          └───────┬───────┘       │
│       │                      │              │              │
│       │                      │         SUCCESS▼              │
│       │                      │    ┌─────────────────┐        │
│       │                      └────│ SECOND_ORDER │        │
│       │                           │   _WAIT    │        │
│       │                           └─────┬───────┘        │
│       │                                 │              │
│       │                          ┌──────▼──────┐       │
│       │                          │COMPLETED   │        │
│       │                          └────────────┘        │
│       │                                 ^              │
│       │                                 │              │
│       │ TIMEOUT                      │              │
│       │<────────────────────────────┘              │
│                                                                     │
│  状态转换说明:                                              │
│  - POST: 创建订单                                          │
│  - ORDER: 发送订单到交易所                                     │
│  - FILLED: 订单成交                                        │
│  - TIMEOUT: 订单执行超时                                   │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 6. draw.io 图表源文件

### 6.1 系统架构图 (draw.io)

请将以下内容保存为 `architecture.drawio` 并在 draw.io 中打开：

```xml
<mxfile host="app.diagrams.net" modified="2024-01-01T00:00:00.000Z" agent="OpenHands" version="22.1.0">
  <diagram id="system-architecture" name="System Architecture">
    <mxGraphModel dx="1200" dy="800" grid="1" gridSize="10" guides="1" tooltips="1" connect="1" arrows="1" fold="1" page="1" pageScale="1" pageWidth="1200" pageHeight="800" math="0" shadow="0">
      <root>
        <mxCell id="0" />
        <mxCell id="1" parent="0" />
        
        <!-- 用户端 -->
        <mxCell id="CLIENT" value="用户端 (Frontend)" style="rounded=1;whiteSpace=wrap;html=1;fillColor=#e1f5fe;strokeColor=#0277dd;" vertex="1" parent="1">
          <mxGeometry x="40" y="40" width="120" height="40" as="geometry" />
        </mxCell>
        
        <!-- API 服务器 -->
        <mxCell id="API_SERVER" value="API 服务器" style="rounded=1;whiteSpace=wrap;html=1;fillColor=#e8f5e8;strokeColor=#2e7d32;" vertex="1" parent="1">
          <mxGeometry x="200" y="40" width="120" height="40" as="geometry" />
        </mxCell>
        
        <!-- 调度器 -->
        <mxCell id="SCHEDULER" value="调度层 (APScheduler)" style="rounded=1;whiteSpace=wrap;html=1;fillColor=#fff3e0;strokeColor=#ef6c00;" vertex="1" parent="1">
          <mxGeometry x="380" y="40" width="140" height="40" as="geometry" />
        </mxCell>
        
        <!-- 数据库 -->
        <mxCell id="DATABASE" value="PostgreSQL" style="shape=cylinder;whiteSpace=wrap;html=1;bounded=1;fillColor=#e0f2f1;strokeColor=#00695c;" vertex="1" parent="1">
          <mxGeometry x="560" y="40" width="100" height="50" as="geometry" />
        </mxCell>
        
        <!-- Binance -->
        <mxCell id="BINANCE" value="Binance 交易所" style="rounded=1;whiteSpace=wrap;html=1;fillColor=#ffebee;strokeColor=#c62828;" vertex="1" parent="1">
          <mxGeometry x="740" y="40" width="120" height="40" as="geometry" />
        </mxCell>
        
        <!-- 连接线 -->
        <mxCell id="E1" style="edgeStyle=orthogonalEdgeStyle;rounded=0;orthogonalLoop=1;" edge="1" parent="1" source="CLIENT" target="API_SERVER">
          <mxGeometry as="geometry" />
        </mxCell>
        <mxCell id="E2" style="edgeStyle=orthogonalEdgeStyle;rounded=0;orthogonalLoop=1;" edge="1" parent="1" source="API_SERVER" target="SCHEDULER">
          <mxGeometry as="geometry" />
        </mxCell>
        <mxCell id="E3" style="edgeStyle=orthogonalEdgeStyle;rounded=0;orthogonalLoop=1;" edge="1" parent="1" source="SCHEDULER" target="DATABASE">
          <mxGeometry as="geometry" />
        </mxCell>
        <mxCell id="E4" style="edgeStyle=orthogonalEdgeStyle;rounded=0;orthogonalLoop=1;" edge="1" parent="1" source="SCHEDULER" target="BINANCE">
          <mxGeometry as="geometry" />
        </mxCell>
      </root>
    </mxGraphModel>
  </diagram>
</mxfile>
```

### 6.2 时序图模板 (draw.io)

```xml
<mxfile host="app.diagrams.net" modified="2024-01-01T00:00:00.000Z" agent="OpenHands" version="22.1.0">
  <diagram id="sequence-diagram" name="Sequence Diagram">
    <mxGraphModel dx="1200" dy="800" grid="1" gridSize="10" guides="1" tooltips="1" connect="1" arrows="1" fold="1" page="1" pageScale="1" pageWidth="1200" pageHeight="800" math="0" shadow="0">
      <root>
        <mxCell id="0" />
        <mxCell id="1" parent="0" />
        
        <!-- 角色头部 -->
        <mxCell id="ACTOR1" value="用户" style="rounded=0;whiteSpace=wrap;html=1;fillColor=#e3f2fd;strokeColor=#1565c0;" vertex="1" parent="1">
          <mxGeometry x="40" y="40" width="80" height="30" as="geometry" />
        </mxCell>
        <mxCell id="ACTOR2" value="API" style="rounded=0;whiteSpace=wrap;html=1;fillColor=#e8f5e8;strokeColor=#2e7d32;" vertex="1" parent="1">
          <mxGeometry x="160" y="40" width="80" height="30" as="geometry" />
        </mxCell>
        <mxCell id="ACTOR3" value="数据库" style="rounded=0;whiteSpace=wrap;html=1;fillColor=#e0f2f1;strokeColor=#00695c;" vertex="1" parent="1">
          <mxGeometry x="280" y="40" width="80" height="30" as="geometry" />
        </mxCell>
        
        <!-- 时序线 -->
        <mxCell id="LINE1" value="" style="endArrow=classic;html=1;dashed=1;" edge="1" parent="1">
          <mxGeometry as="geometry">
            <mxPoint x="80" y="70" as="sourcePoint" />
            <mxPoint x="80" y="280" as="targetPoint" />
          </mxGeometry>
        </mxCell>
        <mxCell id="LINE2" value="" style="endArrow=classic;html=1;dashed=1;" edge="1" parent="1">
          <mxGeometry as="geometry">
            <mxPoint x="200" y="70" as="sourcePoint" />
            <mxPoint x="200" y="280" as="targetPoint" />
          </mxGeometry>
        </mxCell>
        <mxCell id="LINE3" value="" style="endArrow=classic;html=1;dashed=1;" edge="1" parent="1">
          <mxGeometry as="geometry">
            <mxPoint x="320" y="70" as="sourcePoint" />
            <mxPoint x="320" y="280" as="targetPoint" />
          </mxGeometry>
        </mxCell>
        
        <!-- 消息箭头示例 -->
        <mxCell id="MSG1" value="1. POST /api/open-position" style="endArrow=classic;html=1;" edge="1" parent="1">
          <mxGeometry as="geometry">
            <mxPoint x="80" y="90" as="sourcePoint" />
            <mxPoint x="200" y="90" as="targetPoint" />
          </mxGeometry>
        </mxCell>
        <mxCell id="MSG2" value="2. INSERT" style="endArrow=classic;html=1;" edge="1" parent="1">
          <mxGeometry as="geometry">
            <mxPoint x="200" y="120" as="sourcePoint" />
            <mxPoint x="320" y="120" as="targetPoint" />
          </mxGeometry>
        </mxCell>
      </root>
    </mxGraphModel>
  </diagram>
</mxfile>
```

### 6.3 状态机图 (draw.io)

```xml
<mxfile host="app.diagrams.net" modified="2024-01-01T00:00:00.000Z" agent="OpenHands" version="22.1.0">
  <diagram id="state-machine" name="State Machine">
    <mxGraphModel dx="800" dy="600" grid="1" gridSize="10" guides="1" tooltips="1" connect="1" arrows="1" fold="1" page="1" pageScale="1" pageWidth="800" pageHeight="600" math="0" shadow="0">
      <root>
        <mxCell id="0" />
        <mxCell id="1" parent="0" />
        
        <!-- PENDING -->
        <mxCell id="S1" value="PENDING" style="ellipse;whiteSpace=wrap;html=1;fillColor=#fff3e0;strokeColor=#ef6c00;" vertex="1" parent="1">
          <mxGeometry x="80" y="120" width="100" height="50" as="geometry" />
        </mxCell>
        
        <!-- FIRST_ORDER_OPEN -->
        <mxCell id="S2" value="FIRST_ORDER&#xa;OPEN" style="ellipse;whiteSpace=wrap;html=1;fillColor=#e3f2fd;strokeColor=#1565c0;" vertex="1" parent="1">
          <mxGeometry x="240" y="120" width="100" height="50" as="geometry" />
        </mxCell>
        
        <!-- FIRST_ORDER_WAIT -->
        <mxCell id="S3" value="FIRST_ORDER&#xa;WAIT" style="ellipse;whiteSpace=wrap;html=1;fillColor=#e8f5e8;strokeColor=#2e7d32;" vertex="1" parent="1">
          <mxGeometry x="400" y="120" width="100" height="50" as="geometry" />
        </mxCell>
        
        <!-- FIRST_FILLED -->
        <mxCell id="S4" value="FIRST_FILLED" style="ellipse;whiteSpace=wrap;html=1;fillColor=#fce4ec;strokeColor=#ad1457;" vertex="1" parent="1">
          <mxGeometry x="560" y="120" width="100" height="50" as="geometry" />
        </mxCell>
        
        <!-- COMPLETED -->
        <mxCell id="S5" value="COMPLETED" style="ellipse;whiteSpace=wrap;html=1;fillColor=#e0f2f1;strokeColor=#00695c;" vertex="1" parent="1">
          <mxGeometry x="320" y="280" width="100" height="50" as="geometry" />
        </mxCell>
        
        <!-- 状态转移 -->
        <mxCell id="T1" value="INIT" style="endArrow=classic;html=1;dashed=1;" edge="1" parent="1" source="S1" target="S2">
          <mxGeometry as="geometry" />
        </mxCell>
        <mxCell id="T2" value="ORDER" style="endArrow=classic;html=1;dashed=1;" edge="1" parent="1" source="S2" target="S3">
          <mxGeometry as="geometry" />
        </mxCell>
        <mxCell id="T3" value="FILLED" style="endArrow=classic;html=1;dashed=1;" edge="1" parent="1" source="S3" target="S4">
          <mxGeometry as="geometry" />
        </mxCell>
        <mxCell id="T4" value="NEXT" style="endArrow=classic;html=1;dashed=1;" edge="1" parent="1" source="S4" target="S5">
          <mxGeometry as="geometry" />
        </mxCell>
        <mxCell id="T5" value="TIMEOUT" style="endArrow=classic;html=1;dashed=1;" edge="1" parent="1" source="S3" target="S5">
          <mxGeometry as="geometry">
            <Array as="points">
              <mxPoint x="480" y="200" />
              <mxPoint x="480" y="280" />
            </Array>
          </mxGeometry>
        </mxCell>
      </root>
    </mxGraphModel>
  </diagram>
</mxfile>
```

---

## 7. API 接口汇总表

| 方法 | 路径 | 说明 | 请求体 | 响应 |
|------|------|------|--------|------|
| POST | /api/open-position | 开仓 | {contract, batch_num, ...} | {status, message} |
| POST | /api/close-position | 平仓 | {position_id, ...} | {status, message} |
| GET | /api/funding-rates | 资金费率 | - | [{symbol, rate, ...}] |
| GET | /api/positions | 持仓列表 | - | [{id, contract, ...}] |
| GET | /api/positions/{id} | 持仓详情 | - | {id, contract, ...} |
| GET | /api/positions/{id}/progress | 开仓进度 | - | {...} |
| GET | /api/batch-detail/{id} | 批次详情 | - | {...} |
| GET | /api/earnings | 收益列表 | - | [{contract, amount, ...}] |
| GET | /api/health | 健康检查 | - | {status, database} |
| GET | /api/plugins | 插件列表 | - | [{name, type, ...}] |
| POST | /api/plugins/set | 设置插件 | {plugin_name} | {status, message} |

---

## 8. 配置环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| POSTGRES_HOST | localhost | 数据库地址 |
| POSTGRES_PORT | 5432 | 数据库端口 |
| POSTGRES_USER | postgres | 数据库用户 |
| POSTGRES_PASSWORD | postgres | 数据库密码 |
| POSTGRES_DB | arbitrage | 数据库名 |
| BINANCE_API_KEY | - | 币安API Key |
| BINANCE_SECRET_KEY | - | 币安Secret Key |
| CORS_ORIGINS | http://localhost:3000,... | CORS允许的源 |
| DB_POOL_SIZE | 5 | 数据库连接池大小 |
| DB_MAX_OVERFLOW | 10 | 最大溢出连接数 |
| LOG_LEVEL | INFO | 日志级别 |

---

> 💡 **提示**: 
> - Mermaid.js 图表可在 GitHub、Notion、Typora 等支持 Mermaid 的编辑器中实时渲染
> - draw.io XML 文件可导入 draw.io 继续编辑
> - 推荐使用 VS Code + Markdown Preview Supported 插件查看本文档