"""
Core scheduler for position execution using APScheduler.
Each phase is extracted into independent methods for testability.
Now with hybrid approach: WebSocket + Polling fallback for order status.
Using async SQLAlchemy 2.0 for database operations.
"""
import asyncio
from datetime import datetime
from decimal import Decimal
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from sqlalchemy import select

from .modules import create_collector, create_trader, PortfolioManager, LockManager
from .modules.order_watcher import OrderWatcher, SchedulerOrderWatcher, OrderStatus as OWStatus
from .plugins.order_sequence import get_plugin
from .database import get_async_session, init_db_async, BatchExecute, PositionExecute


# Configuration
SLIPPAGE = Decimal('0.001')  # 0.1% slippage
DEFAULT_ORDER_TIMEOUT = 300  # 5 minutes


class PositionScheduler:
    """Scheduler for position execution using APScheduler.
    
    Hybrid approach: WebSocket + Polling fallback for order status monitoring.
    """
    
    def __init__(self, collector_type: str = 'binance', 
                 trader_type: str = 'binance',
                 order_plugin: str = 'futures_first'):
        self.collector = create_collector(collector_type)
        self.trader = create_trader(trader_type)
        self.portfolio = PortfolioManager()
        self.lock_manager = LockManager()
        self.order_plugin = get_plugin(order_plugin)
        self.scheduler = AsyncIOScheduler()
        
        # Order watcher with WebSocket + Polling fallback
        self.order_watcher = SchedulerOrderWatcher(self)
    
    def start(self):
        """Start scheduler with APScheduler."""
        # Start order watcher first
        asyncio.create_task(self.order_watcher.start())
        
        # Job 1: 唤醒pending批次
        self.scheduler.add_job(
            self._wake_pending_batches,
            trigger=IntervalTrigger(seconds=1),
            id='wake_pending_batches',
            replace_existing=True
        )
        
        # Job 2: 执行批次
        self.scheduler.add_job(
            self._execute_running_batches,
            trigger=IntervalTrigger(seconds=1),
            id='execute_running_batches',
            replace_existing=True
        )
        
        self.scheduler.start()
        print("APScheduler started with 2 jobs + OrderWatcher")
    
    async def stop(self):
        """Stop scheduler gracefully."""
        # Stop order watcher first
        await self.order_watcher.stop()
        
        # Shutdown scheduler
        self.scheduler.shutdown()
        print("APScheduler stopped")
    
    async def trigger_phase(self, batch_id: int, phase: str, filled_price: float = None):
        """
        Trigger phase transition from order watcher callback.
        
        Args:
            batch_id: Batch ID
            phase: Target phase
            filled_price: Filled price (if any)
        """
        async with get_async_session() as session:
            result = await session.execute(
                select(BatchExecute).where(BatchExecute.id == batch_id)
            )
            batch = result.scalar_one_or_none()
            
            if not batch:
                return
            
            if phase == 'FIRST_FILLED':
                batch.first_side_filled_price = filled_price or batch.contract_price
                batch.phase = 'FIRST_FILLED'
                batch.updated_at = datetime.utcnow()
                await session.commit()
            elif phase == 'COMPLETED':
                batch.execute_status = 'COMPLETED'
                batch.complete_reason = 'SUCCESS'
                batch.phase = 'COMPLETED'
                batch.updated_at = datetime.utcnow()
                await session.commit()
    
    # ===== Job 1: 唤醒pending批次 =====
    async def _wake_pending_batches(self):
        """
        定时任务1: 唤醒pending批次
        - 将没有RUNNING的合约的PENDING批次转为RUNNING
        - 同一合约只唤醒ID最小的批次
        """
        async with get_async_session() as session:
            # 获取已有RUNNING的合约
            contracts_running = set()
            result = await session.execute(
                select(BatchExecute).where(BatchExecute.execute_status == 'RUNNING')
            )
            running = list(result.scalars().all())
            for batch in running:
                contracts_running.add(batch.position.contract)
            
            # 按ID排序，唤醒最小的
            result = await session.execute(
                select(BatchExecute).where(BatchExecute.execute_status == 'PENDING').order_by(BatchExecute.id)
            )
            pending = list(result.scalars().all())
            
            woken = 0
            for batch in pending:
                contract = batch.position.contract
                
                if contract in contracts_running:
                    continue
                
                # 唤醒
                batch.execute_status = 'RUNNING'
                batch.phase = 'PENDING'
                batch.updated_at = datetime.utcnow()
                await session.commit()
                
                contracts_running.add(contract)
                woken += 1
                print(f"Batch {batch.id} woken: contract={contract}")
            
            if woken > 0:
                print(f"Woke {woken} pending batches")
    
    # ===== Job 2: 执行批次 =====
    async def _execute_running_batches(self):
        """
        定时任务2: 执行批次
        - 按ID升序处理每个RUNNING批次
        - 同一合约只处理ID最小的
        - 检查超时后调用 _execute_phase()
        """
        async with get_async_session() as session:
            # 按ID排序获取RUNNING批次
            result = await session.execute(
                select(BatchExecute).where(BatchExecute.execute_status == 'RUNNING').order_by(BatchExecute.id)
            )
            running = list(result.scalars().all())
            
            contracts_processed = set()
            
            for batch in running:
                contract = batch.position.contract
                
                if contract in contracts_processed:
                    continue
                contracts_processed.add(contract)
                
                try:
                    # 检查超时
                    elapsed = (datetime.utcnow() - batch.updated_at).total_seconds()
                    if elapsed > batch.timeout:
                        batch.execute_status = 'COMPLETED'
                        batch.complete_reason = 'TIMEOUT'
                        await session.commit()
                        continue
                    
                    # 执行批次
                    await self._execute_phase(batch)
                    
                except Exception as e:
                    print(f"Error executing batch {batch.id}: {e}")
                    batch.execute_status = 'COMPLETED'
                    batch.complete_reason = f'ERROR: {str(e)}'
                    await session.commit()
    
    # ===== Phase执行：按当前phase调用对应方法 =====
    async def _execute_phase(self, batch: BatchExecute):
        """根据phase执行对应阶段"""
        phase = batch.phase
        
        if phase == 'PENDING':
            await self._phase_init_params(batch)
        elif phase == 'FIRST_ORDER_OPEN':
            await self._phase_first_order_open(batch)
        elif phase == 'FIRST_ORDER_WAIT':
            await self._phase_first_order_wait(batch)
        elif phase == 'FIRST_FILLED':
            await self._phase_first_filled(batch)
        elif phase == 'SECOND_ORDER_OPEN':
            await self._phase_second_order_open(batch)
        elif phase == 'SECOND_ORDER_WAIT':
            await self._phase_second_order_wait(batch)
    
    # ===== 阶段1: 初始化参数 =====
    async def _phase_init_params(self, batch: BatchExecute):
        """
        阶段1: 初始化参数
        - 获取订单顺序（futures_first / spot_first）
        - 获取合约价格（Mark Price）和现货价格（Ask Price）
        - 计算挂单价（含0.1%滑点）
        - 保存到批次记录
        """
        async with get_async_session() as session:
            # 获取订单顺序
            order_seq = self.order_plugin.get_order_sequence()
            contract = batch.position.contract
            
            # 获取价格
            contract_ticker = await self.collector.get_contract_ticker(contract)
            spot_price = await self.collector.get_spot_price(contract)
            
            # 计算挂单价（含滑点）
            if order_seq.value == 'futures_first':
                contract_price = float(contract_ticker.mark_price * (1 + SLIPPAGE))
                spot_price_val = float(spot_price.ask_price)
            else:
                spot_price_val = float(spot_price.ask_price * (1 + SLIPPAGE))
                contract_price = float(contract_ticker.mark_price)
            
            # 保存参数
            batch.order_sequence = order_seq.value
            batch.contract_price = contract_price
            batch.spot_price = spot_price_val
            batch.phase = 'FIRST_ORDER_OPEN'
            batch.updated_at = datetime.utcnow()
            await session.commit()
            
            print(f"Batch {batch.id} params: order={order_seq.value}, contract={contract_price}, spot={spot_price_val}")
    
    # ===== 阶段2: 第一边挂单 =====
    async def _phase_first_order_open(self, batch: BatchExecute):
        """
        阶段2: 第一边挂单
        - futures_first: 做空合约（open_futures_short）
        - spot_first: 买入现货（buy_spot）
        - 保存订单ID，进入等待阶段
        """
        async with get_async_session() as session:
            amount = batch.batch_value or 1000
            
            if batch.order_sequence == 'futures_first':
                result = await self.trader.open_futures_short(
                    batch.position.contract,
                    amount,
                    batch.contract_price
                )
            else:
                result = await self.trader.buy_spot(
                    batch.position.contract,
                    amount,
                    batch.spot_price
                )
            
            if result.success:
                batch.first_side_order_id = str(result.order_id)
                batch.phase = 'FIRST_ORDER_WAIT'
                batch.updated_at = datetime.utcnow()
                await session.commit()
            else:
                batch.execute_status = 'COMPLETED'
                batch.complete_reason = f'ERROR: {result.message}'
                await session.commit()
    
    # ===== 阶段3: 第一边等待成交 (使用WebSocket+轮询) =====
    async def _phase_first_order_wait(self, batch: BatchExecute):
        """
        阶段3: 第一边等待成交
        - 使用 OrderWatcher 监控订单状态
        - WebSocket 优先，失败时用轮询
        - 成交后进入下一阶段
        """
        if not batch.first_side_order_id:
            return
        
        order_id = batch.first_side_order_id
        contract = batch.position.contract
        timeout = batch.timeout
        
        # 注册到 OrderWatcher 进行监控
        await self.order_watcher.watch_order(
            batch_id=batch.id,
            order_id=order_id,
            symbol=contract,
            phase='FIRST_ORDER_WAIT',
            timeout=timeout
        )
    
    # ===== 阶段4: 第一边已成交 =====
    async def _phase_first_filled(self, batch: BatchExecute):
        """
        阶段4: 第一边已成交
        - 无论哪个顺序，都进入第二边挂单
        - 下一阶段会处理现货转理财
        """
        async with get_async_session() as session:
            batch.phase = 'SECOND_ORDER_OPEN'
            batch.updated_at = datetime.utcnow()
            await session.commit()
    
    # ===== 阶段5: 第二边挂单 =====
    async def _phase_second_order_open(self, batch: BatchExecute):
        """
        阶段5: 第二边挂单 (与第一边相反)
        - futures_first: 买入现货 (buy_spot)
        - spot_first: 做空合约 (open_futures_short)
        - 保存订单ID，进入等待阶段
        """
        async with get_async_session() as session:
            amount = batch.batch_value or 1000
            
            if batch.order_sequence == 'futures_first':
                # 第二边: 买入现货
                result = await self.trader.buy_spot(
                    batch.position.contract,
                    amount,
                    batch.spot_price
                )
            else:
                # 第二边: 做空合约
                result = await self.trader.open_futures_short(
                    batch.position.contract,
                    amount,
                    batch.contract_price
                )
            
            if result.success:
                batch.second_side_order_id = str(result.order_id)
                batch.phase = 'SECOND_ORDER_WAIT'
                batch.updated_at = datetime.utcnow()
                await session.commit()
            else:
                batch.execute_status = 'COMPLETED'
                batch.complete_reason = f'ERROR: {result.message}'
                await session.commit()
    
    # ===== 阶段6: 第二边等待成交 (使用WebSocket+轮询) =====
    async def _phase_second_order_wait(self, batch: BatchExecute):
        """
        阶段6: 第二边等待成交
        - 使用 OrderWatcher 监控订单状态
        - WebSocket 优先，失败时用轮询
        - 成交后转入理财并标记完成
        """
        if not batch.second_side_order_id:
            return
        
        order_id = batch.second_side_order_id
        contract = batch.position.contract
        timeout = batch.timeout
        
        # 注册到 OrderWatcher 进行监控
        await self.order_watcher.watch_order(
            batch_id=batch.id,
            order_id=order_id,
            symbol=contract,
            phase='SECOND_ORDER_WAIT',
            timeout=timeout
        )
    
    # ===== 阶段6处理: 订单成交后的处理 =====
    async def _handle_second_order_filled(self, batch: BatchExecute, filled_price: float = None):
        """
        第二边成交后的处理
        - 由 OrderWatcher 回调触发
        - 转入理财并标记完成
        """
        async with get_async_session() as session:
            # 更新成交价
            if filled_price:
                batch.second_side_filled_price = filled_price
            
            # 现货成交后，转入理财
            transfer_result = await self.trader.transfer_to_savings(
                batch.position.contract,
                batch.batch_value or 1000
            )
            
            batch.execute_status = 'COMPLETED'
            batch.complete_reason = 'SUCCESS'
            batch.phase = 'COMPLETED'
            batch.updated_at = datetime.utcnow()
            await session.commit()
            
            # 检查主记录完成状态
            await self._check_position_complete(batch.position_execute_id)
    
    # ===== 检查主记录完成状态 =====
    async def _check_position_complete(self, position_id: int):
        """检查主记录的所有批次是否都完成"""
        async with get_async_session() as session:
            result = await session.execute(
                select(BatchExecute).where(BatchExecute.position_execute_id == position_id)
            )
            batches = list(result.scalars().all())
            
            if all(b.execute_status == 'COMPLETED' for b in batches):
                result = await session.execute(
                    select(PositionExecute).where(PositionExecute.id == position_id)
                )
                pos = result.scalar_one_or_none()
                
                if pos:
                    reasons = [b.complete_reason for b in batches]
                    if 'TIMEOUT' in reasons:
                        overall = 'TIMEOUT'
                    elif any('ERROR' in r for r in reasons):
                        overall = 'ERROR'
                    else:
                        overall = 'SUCCESS'
                    
                    pos.execute_status = 'COMPLETED'
                    pos.complete_reason = overall
                    pos.updated_at = datetime.utcnow()
                    await session.commit()
                    
                    await self.lock_manager.release(pos.contract)


# ===== 平仓调度器 =====
class CloseScheduler:
    """平仓调度器 - 关闭持仓"""
    
    def __init__(self, collector_type: str = 'binance', trader_type: str = 'binance'):
        self.collector = create_collector(collector_type)
        self.trader = create_trader(trader_type)
        self.scheduler = AsyncIOScheduler()
    
    def start(self):
        """Start close scheduler."""
        # Job 1: 唤醒待平仓
        self.scheduler.add_job(
            self._wake_pending_closes,
            trigger=IntervalTrigger(seconds=1),
            id='wake_pending_closes',
            replace_existing=True
        )
        # Job 2: 执行平仓
        self.scheduler.add_job(
            self._execute_closes,
            trigger=IntervalTrigger(seconds=1),
            id='execute_closes',
            replace_existing=True
        )
        self.scheduler.start()
        print("CloseScheduler started")
    
    async def stop(self):
        """Stop close scheduler."""
        self.scheduler.shutdown()
        print("CloseScheduler stopped")
    
    # ===== Job 1: 唤醒待平仓 =====
    async def _wake_pending_closes(self):
        """唤醒待平仓批次"""
        async with get_async_session() as session:
            from sqlalchemy import select
            result = await session.execute(
                select(BatchExecute).where(
                    BatchExecute.execute_status == 'PENDING',
                    BatchExecute.offset == 'CLOSE'
                ).order_by(BatchExecute.id)
            )
            pending = list(result.scalars().all())
            
            woken = 0
            for batch in pending:
                batch.execute_status = 'RUNNING'
                batch.updated_at = datetime.utcnow()
                await session.commit()
                woken += 1
            
            if woken > 0:
                print(f"Woke {woken} pending close batches")
    
    # ===== Job 2: 执行平仓 =====
    async def _execute_closes(self):
        """执行平仓"""
        async with get_async_session() as session:
            from sqlalchemy import select
            result = await session.execute(
                select(BatchExecute).where(
                    BatchExecute.execute_status == 'RUNNING',
                    BatchExecute.offset == 'CLOSE'
                ).order_by(BatchExecute.id)
            )
            running = list(result.scalars().all())
            
            for batch in running:
                try:
                    # 检查超时
                    elapsed = (datetime.utcnow() - batch.updated_at).total_seconds()
                    if elapsed > batch.timeout:
                        batch.execute_status = 'COMPLETED'
                        batch.complete_reason = 'TIMEOUT'
                        await session.commit()
                        continue
                    
                    # 执行平仓逻辑
                    await self._execute_close(batch)
                    
                except Exception as e:
                    print(f"Error closing batch {batch.id}: {e}")
                    batch.execute_status = 'COMPLETED'
                    batch.complete_reason = f'ERROR: {str(e)}'
                    await session.commit()
    
    async def _execute_close(self, batch: BatchExecute):
        """执行单笔平仓"""
        contract = batch.position.contract
        batch_value = batch.batch_value or 1000
        
        async with get_async_session() as session:
            result = await self.trader.close_futures_position(
                contract,
                batch_value
            )
            
            if result.success:
                batch.phase = 'CLOSED'
                batch.execute_status = 'COMPLETED'
                batch.complete_reason = 'SUCCESS'
                batch.updated_at = datetime.utcnow()
                await session.commit()
            else:
                batch.execute_status = 'COMPLETED'
                batch.complete_reason = f'ERROR: {result.message}'
                await session.commit()