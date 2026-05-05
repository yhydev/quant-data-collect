# 1. OBJECTIVE

设计并实现一个**币安套利平台**，基于**正资金费率+保本赚币**的套利逻辑：
- 手动选择合约做空（收取资金费率）
- 手动买入现货并存入币安活期理财（赚取存款利息）
- 通过做空合约+做多现货实现Delta中性对冲
- 系统内部各模块**低耦合设计**，通过接口/事件通信
- 支持手动操作，同时预留自动化扩展接口

## 分批建仓核心流程

```
Step 1: 计算价格
  ├── 计算合约建仓价 (Mark Price + 滑点)
  └── 计算现货建仓价 (Index Price + 滑点)

Step 2: 选择顺序 (可插拔插件)
  ├── 方案A: 先合约 → 后现货
  └── 方案B: 先现货 → 后合约

Step 3: 执行建仓
  ├── 订单方A挂单 (限价单)
  ├── 等待成交 → 记录成交价
  └── 订单方B挂单 → 等待成交

Step 4: 转入理财 (仅现货)
  └── 现货成交后 → 转入活期理财

每步记录数据库，支持页面查看进度
```

## 建仓操作完整流程

### Step 1: 提交建仓请求
- 前端: 选择标的、批次数、批次建仓大小
- 后台: 
  - 生成建仓记录 (position_execute)
  - 生成分批建仓记录 (batch_execute, N条)
  - 状态: PENDING

### Step 2: 参数初始化 (定时任务)
- 定时任务扫描 PENDING 状态的分批记录
- 调用插件返回建仓参数:
  - 先合约还是先现货 (order_sequence)
  - 合约挂单价 (contract_price)
  - 现货挂单价 (spot_price)
  - 开仓额 (amount)
- 更新到分批建仓记录
- 状态: RUNNING

### Step 3: 执行建仓 (定时任务)
- 扫描 RUNNING 状态的分批记录
- **执行顺序 = 先现货时**:
  1. 现货挂单 (限价单)
  2. WebSocket监听 + 定时拉取 (防止WS断开)
  3. 现货成交 → 转入活期理财
  4. 合约挂单
  5. WebSocket监听 + 定时拉取
  6. 合约成交 → 完成

- **执行顺序 = 先合约时**:
  1. 合约挂单 (限价单)
  2. WebSocket监听 + 定时拉取
  3. 合约成交
  4. 现货挂单
  5. WebSocket监听 + 定时拉取
  6. 现货成交 → 转入活期理财 → 完成

### Step 4: 完成
- 所有分批完成 → 建仓记录状态 END
- 记录完成原因 (TIMEOUT / SUCCESS)

## 并发控制规则

```
1. 不同合约可并行建仓
   - BTC开仓的同时，ETH可以开仓 ✓
   - BTCUSDT 和 ETHUSDT 是不同的合约
   
2. 同一合约不能同时建仓
   - BTCUSDT 正在开仓中，不能再次开仓 ✗
   - 需要等待当前建仓完成
   
3. 同一合约不能同时建仓和平仓
   - BTCUSDT 正在开仓中，不能平仓 ✗
   - BTCUSDT 正在平仓中，不能开仓 ✗
   
4. 实现方式
   - 检查数据库中同一合约的 execute_status
   - RUNNING 状态时拒绝新请求
```

# 2. CONTEXT SUMMARY

## 系统架构 (单体应用，模块低耦合)

```
┌─────────────────────────────────────────────────────────────────┐
│                        Frontend (React)                         │
│   管理界面(手动开平仓) + 监控面板 + 资金费率列表               │
└─────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────────┐
│                      API Gateway (FastAPI)                      │
└─────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────────┐
│                    Core Modules (低耦合)                         │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────┐│
│  │   交易执行  │  │   持仓管理  │  │   策略筛选  │  │   数据采集  ││
│  │  Module   │  │  Module   │  │  Module   │  │  Module   ││
│  │          │  │          │  │  (预留)   │  │          ││
│  └─────┬──────┘  └─────┬───────┘  └─────┬──────┘  └─────┬─────┘│
│        │              │              │             │        │
│        └──────────────┴──────────────┴─────────────┘        │
│                         │                                   │
│              ┌──────────┴──────────┐                    │
│              │   Business Logic      │  统一调度各模块         │
│              └──────────────────────┘                    │
└─────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────────┐
│                    Database Layer (PostgreSQL)                  │
└─────────────────────────────────────────────────────────────────┘
```

## 低耦合设计原则

1. **模块间通过接口通信** - 每个模块暴露抽象接口
2. **依赖注入** - 模块通过构造函数注入依赖
3. **可替换** - 可以替换某个模块而不影响其他
4. **可独立测试** - 每个模块可独立单元测试
5. **预留自动化** - 留出策略接口、调度接口

## 技术栈

| 模块 | 技术栈 |
|------|--------|
| 核心框架 | Python FastAPI |
| 模块通信 | 接口(ABC) + 依赖注入 |
| 前端 | React + TypeScript + Vite |
| 数据库 | PostgreSQL (SQLAlchemy) |
| 部署 | Docker + Docker Compose |
| 外部API | 币安U本位合约API、现货API、理财API |

## 核心功能 (初版)

1. **资金费率列表** - 显示近期高资金费率的合约
2. **手动开仓** - 手动选择合约 + 设置总仓位 + 分批执行
3. **手动平仓** - 手动选择持仓 + 设置平仓数量
4. **监控面板** - 查看当前持仓、资金费率、收益
5. **进度查看** - 每批建仓/平仓进度

# 3. APPROACH OVERVIEW

## 架构设计原则

- **低耦合**：模块间通过接口通信
- **手动优先**：初版全部手动操作
- **预留自动**：留出自动化扩展接口

## 模块职责 (低耦合)

| 模块 | 职责 | 接口 |
|------|------|------|
| **数据采集模块** | 获取资金费率、现货价格 | `ICollector` |
| **交易执行模块** | 执行买卖操作 | `ITrader` |
| **持仓管理模块** | 仓位和收益管理 | `IPortfolio` |
| **策略模块** | 筛选交易对 | `IStrategy` (预留) |
| **调度模块** | 定时任务 | `IScheduler` (预留) |
| **订单插件模块** | 选择执行顺序 | `IOrderPlugin` (可插拔) |
| **并发控制模块** | 控制同一合约不能同时操作 | `ILockManager` |

## 核心模块接口定义

```python
# interfaces/__init__.py

class ICollector(ABC):
    @abstractmethod
    def get_funding_rates(self) -> List[FundingRate]: pass
    
    @abstractmethod
    def get_spot_price(self, symbol: str) -> float: pass

class ITrader(ABC):
    @abstractmethod
    def open_position(self, symbol: str, amount: float) -> TradeResult: pass
    
    @abstractmethod
    def close_position(self, position_id: int, amount: float) -> TradeResult: pass

class IOrderPlugin(ABC):  # 可插拔插件
    @abstractmethod
    def get_order_sequence(self) -> OrderSequence:
        """返回先做合约还是先做现货"""
        pass

class ILockManager(ABC):  # 并发控制
    @abstractmethod
    async def acquire(self, symbol: str, operation: str) -> bool:
        """尝试获取锁，成功返回True"""
        pass
    
    @abstractmethod
    async def release(self, symbol: str) -> None:
        """释放锁"""
        pass
    
    @abstractmethod
    def is_locked(self, symbol: str) -> bool:
        """检查锁状态"""
        pass

class IPortfolio(ABC):
    @abstractmethod
    def get_positions(self) -> List[Position]: pass
    
    @abstractmethod
    def get_earnings(self) -> List[Earning]: pass

class IStrategy(ABC):  # 预留自动化
    @abstractmethod
    def select_pairs(self, rates: List[FundingRate]) -> List[TradingPair]: pass
```

## 订单顺序插件

```python
# plugins/order_sequence/futures_first.py
class FuturesFirstPlugin(IOrderPlugin):
    def get_order_sequence(self) -> OrderSequence:
        return OrderSequence.FUTURES_FIRST  # 先合约后现货

# plugins/order_sequence/spot_first.py  
class SpotFirstPlugin(IOrderPlugin):
    def get_order_sequence(self) -> OrderSequence:
        return OrderSequence.SPOT_FIRST  # 先现货后合约
```

## 部署架构

```
┌────────────────────────────────────────────────────────────────┐
│                     Docker Compose                             │
│  ┌─────────────┐ ┌─────────────┐                              │
│  │   Backend   │ │  Frontend  │   ← 单体应用，内部模块低耦合  │
│  │  (FastAPI  │ │   (Vite)   │                              │
│  │  + Modules) │ │           │                              │
│  └─────────────┘ └─────────────┘                              │
│  ┌─────────────┐                                           │
│  │ PostgreSQL │                                           │
│  └─────────────┘                                           │
└────────────────────────────────────────────────────────────────┘
```

# 4. IMPLEMENTATION STEPS

## Phase 1: 基础设施搭建 (第1天)

### Step 1.1: 创建项目结构
```
binance-arbitrage/
├── backend/
│   ├── modules/           # 核心模块 (低耦合)
│   │   ├── collector.py   # 数据采集模块
│   │   ├── trader.py      # 交易执行模块
│   │   ├── portfolio.py  # 持仓管理模块
│   │   └── strategy.py   # 策略模块 (预留)
│   ├── plugins/           # 可插拔插件
│   │   └── order_sequence/   # 订单顺序插件
│   ├── interfaces/        # 接口定义
│   ├── api/              # API路由
│   ├── core.py           # 核心调度层
│   └── database.py      # 数据库层
├── frontend/
│   └── src/pages/        # 前端页面
└── docker-compose.yml
```
- **目标**: 建立低耦合模块结构和插件目录
- **方法**: 按功能划分模块，定义接口

### Step 1.2: 定义模块接口
- **目标**: 定义各模块接口规范
- **方法**: 创建 `interfaces/` 目录，定义抽象基类

### Step 1.3: 设置 PostgreSQL 数据库
- **目标**: 创建数据库表结构
- **方法**: SQLAlchemy ORM，创建表：

| 表名 | 说明 |
|------|------|
| `position_execute` | 仓位执行表 (建仓/平仓主记录) |
| `batch_execute` | 批次仓位执行表 (分批记录) |
| `position_orders` | 每个订单记录 |
| `position_steps` | 每步执行记录 |
| `trading_history` | 交易历史 |
| `funding_rates` | 资金费率历史 |
| `earnings` | 收益记录 |

### 表结构详细设计

```python
# position_execute (仓位执行表)
class PositionExecute(Base):
    __tablename__ = 'position_execute'
    
    id = Column(Integer, primary_key=True)
    contract = Column(String)           # 合约 symbol，如 BTCUSDT
    batch_num = Column(Integer)        # 批次数
    execute_status = Column(String)   # PENDING | RUNNING | END
    batch_position_value = Column(Float)  # 批次开仓价值
    offset = Column(String)          # OPEN | CLOSE
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    complete_reason = Column(String) # TIMEOUT | SUCCESS | None

# batch_execute (批次仓位执行表)
class BatchExecute(Base):
    __tablename__ = 'batch_execute'
    
    id = Column(Integer, primary_key=True)
    position_execute_id = Column(Integer, ForeignKey('position_execute.id'))
    timeout = Column(Integer)        # 成交超时时间(秒)
    execute_status = Column(String)  # PENDING | RUNNING | COMPLETED
    offset = Column(String)         # OPEN | CLOSE
    contract_price = Column(Float)  # 合约挂单价
    spot_price = Column(Float)      # 现货挂单价
    phase = Column(String)         # 当前阶段
    complete_reason = Column(String) # TIMEOUT | SUCCESS | None
    
# 并发控制规则
# 1. 同一合约 position_execute 中 execute_status=RUNNING 只能有一个
# 2. 同一合约所有 batch_execute 的 execute_status=COMPLETED 后，position_execute 才能为 END
# 3. 建仓时检查是否存在未结束的平仓，反之亦然
```

### Step 1.4: 配置 Docker Compose
- **目标**: 编排应用
- **方法**: 创建docker-compose.yml

---

## Phase 2: 核心模块开发 (第2-3天)

### Step 2.1: 数据采集模块
- **目标**: 获取并存储市场数据
- **方法**: 调用币安API，实现ICollector接口
- **文件**: `modules/collector.py`

### Step 2.2: 交易执行模块
- **目标**: 执行买卖操作
- **方法**: 
  - 实现开空/买入/存入
  - 实现ITrader接口
  - 支持分批执行
  - 计算建仓价 (Mark Price + 滑点)
- **文件**: `modules/trader.py`

### Step 2.3: 订单顺序插件系统
- **目标**: 可插拔选择先做合约还是现货
- **方法**:
  - 定义IOrderPlugin接口
  - 实现FuturesFirstPlugin
  - 实现SpotFirstPlugin
  - 可动态切换
- **文件**: `plugins/order_sequence/`

### Step 2.4: 持仓管理模块
- **目标**: 管理仓位
- **方法**: 记录持仓/分批/订单/步骤，实现IPortfolio接口
- **文件**: `modules/portfolio.py`

### Step 2.5: 策略模块(预留)
- **目标**: 预留自动化筛选接口
- **方法**: 实现IStrategy接口，默认返回空
- **文件**: `modules/strategy.py`

---

## Phase 3: 建仓/平仓流程开发 (第3-4天)

### Step 3.1: 提交建仓/平仓请求接口
- **目标**: 接收前端建仓/平仓请求
- **方法**:
  - 接收参数: symbol, batch_num, batch_position_value
  - 检查并发: 同一合约是否有未结束的建仓/平仓
  - 生成 position_execute 记录
  - 生成 N条 batch_execute 记录
  - 返回 execute_id

### Step 3.2: 参数初始化定时任务
- **目标**: 初始化每批建仓参数
- **方法**: 
  - 每秒扫描 PENDING 状态的 batch_execute
  - 获取 Mark Price / Index Price
  - 调用插件计算建仓参数:
    - order_sequence (先合约/先现货)
    - contract_price (合约挂单价)
    - spot_price (现货挂单价)
  - 更新 batch_execute
  - 状态: RUNNING

### Step 3.3: 订单执行定时任务
- **目标**: 执行挂单和监控
- **方法**:
  - 每秒扫描 RUNNING 状态的 batch_execute
  - 根据 phase 执行:
    - SPOT_ORDER_OPEN: 现货挂单
    - SPOT_WAIT_FILLED: 等待现货成交 + WebSocket/定时拉取
    - SPOT_TRANSFER: 现货转入理财
    - CONTRACT_ORDER_OPEN: 合约挂单
    - CONTRACT_WAIT_FILLED: 等待合约成交 + WebSocket/定时拉取
    - COMPLETED: 完成
  - 处理超时: timeout 字段

### Step 3.4: 订单状态跟踪
- **目标**: 跟踪每个订单状态
- **方法**:
  - PENDING (待挂单)
  - RUNNING (执行中)
  - COMPLETED (完成)
  - 阶段 phase: 
    - PENDING → init params
    - PHASE_1_ORDER_OPEN → PHASE_1_WAIT_FILLED → PHASE_2_ORDER_OPEN → PHASE_2_WAIT_FILLED → TRANSFER → COMPLETED

### Step 3.5: 建仓/平仓完成处理
- **目标**: 标记完成状态
- **方法**: 
  - 检查所有 batch_execute 状态
  - 全部 COMPLETED → position_execute 设为 END
  - 记录 complete_reason (TIMEOUT / SUCCESS)

### Step 3.6: 并发控制
- **目标**: 控制同一合约操作互斥
- **方法**:
  - 建仓时检查是否有未结束的平仓
  - 平仓时检查是否有未结束的建仓
  - 使用数据库锁或分布式锁

---

## Phase 4: API和前端开发 (第4-5天)

### Step 4.1: API服务开发
- **目标**: REST API接口
- **方法**: FastAPI实现:

  **资金费率**:
  - `GET /api/funding-rates` - 资金费率列表
  
  **开仓**:
  - `POST /api/open-position` - 手动开仓
  - `GET /api/open-progress` - 开仓进度
  - `GET /api/batch-detail/{id}` - 分批详情
  
  **平仓**:
  - `POST /api/close-position` - 手动平仓
  - `GET /api/close-progress` - 平仓进度
  
  **持仓**:
  - `GET /api/positions` - 持仓列表
  - `GET /api/positions/{id}` - 持仓详情
  
  **其他**:
  - `GET /api/earnings` - 收益历史
  
  **插件**:
  - `GET /api/plugins` - 可用插件
  - `POST /api/plugins/set` - 设置插件

  **预留**:
  - `POST /api/strategy/select` - 调用策略筛选
  - `POST /api/scheduler/trigger` - 触发调度

- **文件**: `api/routes.py`

### Step 4.2: 前端-资金费率列表
- **目标**: 显示高资金费率合约
- **文件**: `pages/FundingRates.tsx`

### Step 4.3: 前端-手动开仓界面
- **目标**: 手动选择合约开仓+分批设置
- **文件**: `pages/OpenPosition.tsx`

### Step 4.4: 前端-建仓进度详情
- **目标**: 查看每批建仓情况
- **方法**:
  - 显示批次列表
  - 显示每步状态 (计算价→挂单→成交→转入)
  - 显示合约/现货成交价
- **文件**: `pages/OpenProgress.tsx`

### Step 4.5: 前端-手动平仓界面
- **目标**: 手动选择持仓平仓
- **文件**: `pages/ClosePosition.tsx`

### Step 4.6: 前端-平仓进度详情
- **目标**: 查看每批平仓情况
- **文件**: `pages/CloseProgress.tsx`

### Step 4.7: 前端-监控面板
- **目标**: 实时监控
- **文件**: `pages/Dashboard.tsx`

---

## Phase 5: 集成测试 (第6天)

### Step 5.1: 联调测试
### Step 5.2: Docker部署

---

# 5. TESTING AND VALIDATION

## 验证要点

| 验证项 | 预期结果 |
|--------|----------|
| 数据采集 | 能获取资金费率、价格 |
| 价格计算 | 能计算建仓价+滑点 |
| 订单插件 | 可切换先合约/先现货 |
| 并发控制 | 不同合约可并行，同一合约不能并行 |
| 分批建仓 | 流程完整(A挂→等成交→B挂→等成交→转入) |
| 分批平仓 | 流程完整 |
| 数据库记录 | 每步都有记录 |
| 建仓进度 | 页面查看每批情况 |
| 平仓进度 | 页面查看每批情况 |

## 成功标准

1. ✅ 提交建仓/平仓请求生成主记录和分批记录
2. ✅ 参数初始化定时任务正确计算建仓参数
3. ✅ 订单执行定时任务能执行挂单和监控
4. ✅ WebSocket + 定时拉取双重监控
5. ✅ 现货成交后转入活期理财
6. ✅ 并发控制正确 (建仓时检查平仓)
7. ✅ 超时处理正确
8. ✅ 页面查看进度
9. ✅ 模块低耦合设计

## 页面结构

```
frontend/src/pages/
├── Dashboard.tsx         # 监控面板
├── FundingRates.tsx    # 资金费率列表
├── OpenPosition.tsx    # 手动开仓
├── OpenProgress.tsx    # 开仓进度详情
├── ClosePosition.tsx   # 手动平仓
├── CloseProgress.tsx    # 平仓进度详情
└── PluginSettings.tsx   # 插件设置
```

## 订单状态流程

```
建仓流程:
CALCULATED_PRICE → FIRST_SIDE_ORDER_OPEN → FIRST_SIDE_ORDER_FILLED 
→ SECOND_SIDE_ORDER_OPEN → SECOND_SIDE_ORDER_FILLED → TRANSFERRED

平仓流程:
CALCULATED_PRICE → FIRST_SIDE_ORDER_OPEN → FIRST_SIDE_ORDER_FILLED 
→ SECOND_SIDE_ORDER_OPEN → SECOND_SIDE_ORDER_FILLED → CLOSED
```

## 配置参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `DEFAULT_BATCH_SIZE` | 默认每批金额 | 1000 USDT |
| `SLIPPAGE` | 滑点比例 | 0.001 (0.1%) |
| `ORDER_TIMEOUT` | 订单超时时间 | 300秒 |
| `POSTGRES_HOST` | 数据库地址 | postgres |
