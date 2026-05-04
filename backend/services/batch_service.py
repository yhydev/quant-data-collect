"""
Batch execution service - 业务逻辑层
处理所有批次相关的业务逻辑，供events层调用
"""
import asyncio
import logging
from datetime import datetime
from decimal import Decimal
from typing import Dict, Optional

from sqlalchemy import select

from models.database import get_async_session, BatchExecute
from events.order_watcher import SchedulerOrderWatcher
from services import create_collector, create_trader
from plugins.order_sequence import get_plugin

# 延迟导入，避免循环导入
BatchPhaseMachine = None
PhaseState = None

def _get_phase_machine_module():
    from events.phase_machine import BatchPhaseMachine as BPM, PhaseState as PS
    return BPM, PS


class BatchExecutionService:
    """
    批次执行服务 - 业务逻辑层
    
    职责：
    1. 处理批次唤醒的业务逻辑
    2. 处理批次执行的业务逻辑
    3. 处理订单事件的业务逻辑
    4. 管理状态机
    """
    
    def __init__(self, collector_type='binance', trader_type='binance', 
                 order_plugin_name='futures_first'):
        # 创建依赖（也可以由main.py注入）
        self.collector = create_collector(collector_type)
        self.trader = create_trader(trader_type)
        self.order_plugin = get_plugin(order_plugin_name)
        
        # 不再缓存状态机，每次从数据库重新加载
    
    # ==================== 批次唤醒逻辑 ====================
    
    async def wake_pending_batches(self) -> int:
        """
        唤醒PENDING批次
        
        Returns:
            唤醒的批次数量
        """
        async with get_async_session() as session:
            from sqlalchemy import select
            
            # Get contracts already running
            contracts_running = set()
            result = await session.execute(
                select(BatchExecute).where(BatchExecute.execute_status == 'RUNNING')
            )
            running = list(result.scalars().all())
            for batch in running:
                contracts_running.add(batch.position.contract)
            
            # Find pending batches to wake
            result = await session.execute(
                select(BatchExecute).where(BatchExecute.execute_status == 'PENDING').order_by(BatchExecute.id)
            )
            pending = list(result.scalars().all())
            
            woken = 0
            for batch in pending:
                contract = batch.position.contract
                
                if contract in contracts_running:
                    continue
                
                # Wake batch
                batch.execute_status = 'RUNNING'
                batch.phase = PhaseState.PENDING
                batch.updated_at = datetime.utcnow()
                await session.commit()
                
                contracts_running.add(contract)
                woken += 1
                
                logger.info(f"Batch {batch.id} woken: contract={contract}")
            
            if woken > 0:
                logger.info(f"Woke {woken} pending batches")
            
            return woken
    
    # ==================== 批次执行逻辑 ====================
    
    async def execute_batch(self, batch_id: int) -> bool:
        """
        执行批次
        
        Args:
            batch_id: 批次ID
            
        Returns:
            True if executed successfully, False otherwise
        """
        async with get_async_session() as session:
            result = await session.execute(
                select(BatchExecute).where(BatchExecute.id == batch_id)
            )
            batch = result.scalar_one_or_none()
            
            if not batch:
                return False
            
            # Check if already completed
            if batch.execute_status == 'COMPLETED':
                return False
            
            # Check timeout
            elapsed = (datetime.utcnow() - batch.updated_at).total_seconds()
            if elapsed > batch.timeout:
                batch.execute_status = 'COMPLETED'
                batch.complete_reason = 'TIMEOUT'
                await session.commit()
                logger.warning(f"Batch {batch.id} timeout")
                return False
            
            # Create machine for batch (no caching)
            machine = self._create_machine_for_batch(batch)
            
            # Execute current phase
            try:
                await self._execute_current_phase(machine)
                return True
            except Exception as e:
                logger.error(f"Batch execution error: {e}")
                batch.execute_status = 'COMPLETED'
                batch.complete_reason = f'ERROR: {str(e)}'
                await session.commit()
                return False
    
    async def trigger_all_running(self) -> None:
        """
        触发所有RUNNING批次的执行
        由scheduler定时调用，直接执行并等待所有批次完成当前阶段
        """
        from models.database import get_async_session
        from sqlalchemy import select
        
        # 先收集所有需要执行的批次，避免长事务
        async with get_async_session() as session:
            result = await session.execute(
                select(BatchExecute)
                .where(BatchExecute.execute_status == 'RUNNING')
                .order_by(BatchExecute.id)
            )
            running = result.scalars().all()
        
        contracts_processed = set()
        tasks = []
        
        for batch in running:
            contract = batch.position.contract
            
            # 同一合约只处理一次
            if contract in contracts_processed:
                continue
            contracts_processed.add(contract)
            
            # 直接执行批次，创建任务
            task = asyncio.create_task(self.execute_batch(batch.id))
            tasks.append(task)
        
        # 等待所有批次执行完成（当前阶段）
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    # ==================== 订单事件逻辑 ====================
    
    async def handle_order_filled(self, batch_id: int, filled_price: float = 0) -> None:
        """
        处理订单成交事件
        
        Args:
            batch_id: 批次ID
            filled_price: 成交价格
        """
        global PhaseState
        
        # 延迟导入
        if PhaseState is None:
            BatchPhaseMachine, PhaseState = _get_phase_machine_module()
        
        # 从数据库重新加载批次并创建状态机
        async with get_async_session() as session:
            result = await session.execute(
                select(BatchExecute).where(BatchExecute.id == batch_id)
            )
            batch = result.scalar_one_or_none()
        
        if not batch:
            logger.warning(f"No batch found for batch {batch_id}")
            return
        
        machine = self._create_machine_for_batch(batch)
        
        # Determine which order was filled
        state = machine.state
        if state == PhaseState.FIRST_ORDER_WAIT:
            # Trigger state change (synchronous)
            machine.first_order_filled()
            # Call async handler manually
            await machine._handle_first_order_filled(filled_price)
            # Proceed to second order
            machine.proceed_to_second()
        elif state == PhaseState.SECOND_ORDER_WAIT:
            # Trigger state change (synchronous)
            machine.second_order_filled()
            # Call async handler manually
            await machine._handle_second_order_filled(filled_price)
            logger.info(f"Batch {batch_id} completed")
    
    async def handle_order_cancelled(self, batch_id: int) -> None:
        """
        处理订单取消事件
        
        Args:
            batch_id: 批次ID
        """
        global PhaseState
        
        # 延迟导入
        if PhaseState is None:
            BatchPhaseMachine, PhaseState = _get_phase_machine_module()
        
        # 从数据库重新加载批次并创建状态机
        async with get_async_session() as session:
            result = await session.execute(
                select(BatchExecute).where(BatchExecute.id == batch_id)
            )
            batch = result.scalar_one_or_none()
        
        if not batch:
            return
        
        machine = self._create_machine_for_batch(batch)
        
        state = machine.state
        if state == PhaseState.FIRST_ORDER_WAIT:
            machine.retry_first_order()
        elif state == PhaseState.SECOND_ORDER_WAIT:
            machine.proceed_to_second()
    
    async def handle_order_rejected(self, batch_id: int) -> None:
        """处理订单拒绝事件"""
        await self.handle_order_cancelled(batch_id)
    
    # ==================== 批次管理逻辑 ====================
    
    async def retry_batch(self, batch_id: int) -> bool:
        """
        重试批次
        
        Args:
            batch_id: 批次ID
            
        Returns:
            True if retried successfully
        """
        async with get_async_session() as session:
            result = await session.execute(
                select(BatchExecute).where(BatchExecute.id == batch_id)
            )
            batch = result.scalar_one_or_none()
            
            if not batch:
                return False
            
            batch.execute_status = 'RUNNING'
            batch.phase = PhaseState.PENDING
            batch.updated_at = datetime.utcnow()
            await session.commit()
            
            return True
    
    async def cancel_batch(self, batch_id: int) -> bool:
        """
        取消批次
        
        Args:
            batch_id: 批次ID
            
        Returns:
            True if cancelled successfully
        """
        async with get_async_session() as session:
            result = await session.execute(
                select(BatchExecute).where(BatchExecute.id == batch_id)
            )
            batch = result.scalar_one_or_none()
            
            if not batch:
                return False
            
            batch.execute_status = 'COMPLETED'
            batch.complete_reason = 'CANCELLED'
            await session.commit()
            
            return True
    
    async def timeout_batch(self, batch_id: int) -> bool:
        """
        批次超时处理
        
        Args:
            batch_id: 批次ID
            
        Returns:
            True if timed out successfully
        """
        async with get_async_session() as session:
            result = await session.execute(
                select(BatchExecute).where(BatchExecute.id == batch_id)
            )
            batch = result.scalar_one_or_none()
            
            if batch and batch.execute_status == 'RUNNING':
                batch.execute_status = 'COMPLETED'
                batch.complete_reason = 'TIMEOUT'
                await session.commit()
                
                logger.info(f"Batch {batch.id} marked as TIMEOUT")
                return True
            
            return False
    
    # ==================== 状态机管理 ====================
    
    def _create_machine_for_batch(self, batch: BatchExecute):
        """Create state machine for batch (no caching)."""
        global BatchPhaseMachine, PhaseState
        
        # 延迟导入
        if BatchPhaseMachine is None:
            BatchPhaseMachine, PhaseState = _get_phase_machine_module()
        
        # Create new machine
        machine = BatchPhaseMachine.load_from_batch(
            batch,
            collector=self.collector,
            trader=self.trader,
            order_plugin=self.order_plugin,
            order_watcher=self.order_watcher
        )
        
        return machine
    
    async def _execute_current_phase(self, machine):
        """Execute current phase of the state machine."""
        global PhaseState
        
        # 延迟导入
        if PhaseState is None:
            BatchPhaseMachine, PhaseState = _get_phase_machine_module()
        
        state = machine.state
        
        # Trigger appropriate transition based on current state
        # Note: trigger() is synchronous, async operations are called manually after
        if state == PhaseState.PENDING:
            machine.initialize_params()  # Trigger state change
            await machine._initialize_params()  # Call async operation
        elif state == PhaseState.FIRST_ORDER_OPEN:
            machine.open_first_order()  # Trigger state change
            await machine._open_first_order()  # Call async operation
        elif state == PhaseState.FIRST_ORDER_WAIT:
            # Already watching, just wait
            pass
        elif state == PhaseState.FIRST_FILLED:
            machine.proceed_to_second()  # Trigger state change
        elif state == PhaseState.SECOND_ORDER_OPEN:
            machine.open_second_order()  # Trigger state change
            await machine._open_second_order()  # Call async operation
        elif state == PhaseState.SECOND_ORDER_WAIT:
            # Already watching, just wait
            pass
        elif state == PhaseState.COMPLETED:
            pass
    
    # 不再缓存状态机，以下方法已移除：
    # get_machine, get_all_machines, active_count
    # 如需获取状态机，请使用 _create_machine_for_batch 从数据库重新创建
    
    async def _check_position_complete(self, position_execute_id: int):
        """Check if all batches for a position are complete, update position status."""
        from models.database import PositionExecute, get_async_session
        from sqlalchemy import select
        
        async with get_async_session() as session:
            # Get all batches for this position
            result = await session.execute(
                select(BatchExecute).where(
                    BatchExecute.position_execute_id == position_execute_id
                )
            )
            batches = list(result.scalars().all())
            
            # Check if all completed
            if all(b.phase == 'COMPLETED' for b in batches):
                result = await session.execute(
                    select(PositionExecute).where(
                        PositionExecute.id == position_execute_id
                    )
                )
                pos = result.scalar_one_or_none()
                
                if pos:
                    reasons = [b.complete_reason for b in batches]
                    if 'TIMEOUT' in reasons:
                        overall = 'TIMEOUT'
                    elif any('ERROR' in r for r in reasons if r):
                        overall = 'ERROR'
                    else:
                        overall = 'SUCCESS'
                    
                    pos.execute_status = 'COMPLETED'
                    pos.complete_reason = overall
                    pos.updated_at = datetime.utcnow()
                    await session.commit()
