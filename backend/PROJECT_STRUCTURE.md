# 项目结构重构说明

## 新的目录结构 (按事件类型组织)

```
backend/
├── api/                    # HTTP请求处理
│   ├── __init__.py
│   └── routes.py          # FastAPI路由定义
│
├── scheduler/              # 定时任务处理
│   ├── __init__.py
│   └── core.py            # WakeScheduler, ExecuteScheduler
│
├── events/                 # 事件处理与状态机
│   ├── __init__.py
│   ├── phase_machine.py    # BatchPhaseMachine状态机
│   ├── phase_service.py   # PhaseService事件处理服务
│   └── order_watcher.py   # OrderWatcher订单监控
│
├── services/               # 业务服务
│   ├── __init__.py
│   ├── collector.py       # 数据采集服务 (BinanceCollector)
│   ├── trader.py          # 交易执行服务 (BinanceTrader)
│   ├── portfolio.py       # 投资组合管理 (PortfolioManager)
│   └── strategy.py        # 策略服务 (预留)
│
├── models/                 # 数据模型
│   ├── __init__.py
│   ├── database.py        # 数据库模型 (SQLAlchemy)
│   └── interfaces/        # 接口定义
│       └── __init__.py    # ICollector, ITrader等接口
│
├── plugins/                # 插件系统
│   └── order_sequence/   # 订单序列插件
│
├── main.py                # 应用入口
├── requirements.txt       # Python依赖
└── Dockerfile            # Docker配置
```

## 各包职责说明

### 1. api包 - HTTP请求处理
- **职责**: 处理所有HTTP API请求
- **主要文件**: `routes.py`
- **功能**: 
  - REST API端点定义
  - 请求参数验证
  - 响应格式化
  - 调用services和events包完成业务逻辑

### 2. scheduler包 - 定时任务处理
- **职责**: 处理定时任务和调度
- **主要文件**: `core.py`
- **功能**:
  - `WakeScheduler`: 唤醒PENDING批次
  - `ExecuteScheduler`: 执行RUNNING批次
  - 基于APScheduler的定时任务管理

### 3. events包 - 事件处理与状态机
- **职责**: 处理系统事件和状态流转
- **主要文件**:
  - `phase_machine.py`: 批次执行状态机
  - `phase_service.py`: 事件处理服务
  - `order_watcher.py`: 订单状态监控
- **功能**:
  - 事件发布/订阅机制
  - 状态机管理批次生命周期
  - WebSocket/Polling订单监控

### 4. services包 - 业务服务
- **职责**: 核心业务逻辑服务
- **主要文件**:
  - `collector.py`: 数据采集 (Binance API)
  - `trader.py`: 交易执行 (下单、撤单等)
  - `portfolio.py`: 投资组合管理
  - `strategy.py`: 交易策略 (预留)
- **功能**:
  - 实现接口定义的具体服务
  - 封装第三方API调用
  - 提供可替换的服务实现 (如Mock服务用于测试)

### 5. models包 - 数据模型
- **职责**: 数据模型和接口定义
- **主要文件**:
  - `database.py`: SQLAlchemy ORM模型
  - `interfaces/__init__.py`: 抽象接口定义
- **功能**:
  - 数据库表模型定义
  - 服务接口抽象
  - 数据访问层

### 6. plugins包 - 插件系统
- **职责**: 可扩展的插件机制
- **主要插件**: `order_sequence`
- **功能**:
  - 订单执行顺序插件
  - 支持futures_first, spot_first等策略

## 导入路径规则

所有导入都使用绝对导入，格式为:
```python
# 从models包导入
from models.database import ...
from models.interfaces import ...

# 从其他顶层包导入
from api.routes import ...
from scheduler.core import ...
from events.phase_service import ...
from services import ...
from plugins.order_sequence import ...
```

## 设计原则

1. **单一职责**: 每个包只负责一类功能
2. **依赖倒置**: 通过interfaces包定义抽象接口
3. **事件驱动**: events包解耦事件源和处理逻辑
4. **可测试性**: services包支持Mock实现
5. **可扩展性**: plugins包支持功能扩展

## 重构完成状态

✅ 目录结构已按事件类型重新组织
✅ 导入路径已更新为绝对导入
✅ 各包的`__init__.py`已创建
✅ 主要功能模块已归类到对应包中

## 待办事项

- [ ] 测试所有导入是否正常
- [ ] 运行单元测试验证功能
- [ ] 更新文档和注释
- [ ] 考虑是否需要进一步的模块拆分
