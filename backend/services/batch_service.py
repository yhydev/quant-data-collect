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
        self.phase_service = None  # 由main.py注入
        
        # State machines: batch_id -> machine
        self._machines: Dict[int, BatchPhaseMachine] = {}
    
    def set_phase_service(self, phase_service):
        """注入PhaseService依赖"""
        self.phase_service = phase_service
    
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
                
                # Load or create state machine
                self._get_or_create_machine(batch)
                
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
            
            # Get or create machine
            machine = self._get_or_create_machine(batch)
            
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
    
    def trigger_all_running(self) -> None:
        """
        触发所有RUNNING批次的执行
        由scheduler定时调用，只做触发，不查询数据库
        """
        # 发布事件让event worker处理
        if self._phase_service:
            import asyncio
            task = asyncio.create_task(self._async_trigger_all_running())
            # 添加回调来处理异常，避免task被垃圾回收
            task.add_done_callback(lambda t: t.exception() if t.exception() else None)
    
    async def _async_trigger_all_running(self) -> None:
        """异步触发所有running批次"""
        from models.database import get_async_session
        from sqlalchemy import select
        
        async with get_async_session() as session:
            result = await session.execute(
                select(BatchExecute)
                .where(BatchExecute.execute_status == 'RUNNING')
                .order_by(BatchExecute.id)
            )
            running = result.scalars().all()
            
            contracts_processed = set()
            
            for batch in running:
                contract = batch.position.contract
                
                # 同一合约只处理一次
                if contract in contracts_processed:
                    continue
                contracts_processed.add(contract)
                
                try:
                    # 发布执行事件，让event handler和service层处理
                    await self._phase_service.publish_batch_execute(batch.id)
                    
                except Exception as e:
                    logger.error(f"Error triggering batch {batch.id}: {e}")
    
    # ==================== 订单事件逻辑 ====================
    
    async def handle_order_filled(self, batch_id: int, filled_price: float = 0) -> None:
        """
        处理订单成交事件
        
        Args:
            batch_id: 批次ID
            filled_price: 成交价格
        """
        global PhaseState
        
        machine = self._machines.get(batch_id)
        if not machine:
            logger.warning(f"No machine for batch {batch_id}")
            return
        
        # 延迟导入
        if PhaseState is None:
            BatchPhaseMachine, PhaseState = _get_phase_machine_module()
        
        # Determine which order was filled
        state = machine.state
        if state == PhaseState.FIRST_ORDER_WAIT:
            await machine.first_order_filled(filled_price=filled_price)
            machine.proceed_to_second()
        elif state == PhaseState.SECOND_ORDER_WAIT:
            await machine.second_order_filled(filled_price=filled_price)
            logger.info(f"Batch {batch_id} completed")
    
    def handle_order_cancelled(self, batch_id: int) -> None:
        """
        处理订单取消事件
        
        Args:
            batch_id: 批次ID
        """
        global PhaseState
        
        machine = self._machines.get(batch_id)
        if not machine:
            return
        
        # 延迟导入
        if PhaseState is None:
            BatchPhaseMachine, PhaseState = _get_phase_machine_module()
        
        state = machine.state
        if state == PhaseState.FIRST_ORDER_WAIT:
            machine.retry_first_order()
        elif state == PhaseState.SECOND_ORDER_WAIT:
            machine.proceed_to_second()
    
    def handle_order_rejected(self, batch_id: int) -> None:
        """处理订单拒绝事件"""
        self.handle_order_cancelled(batch_id)
    
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
            
            # Create new machine
            self._get_or_create_machine(batch)
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
            
            # Remove machine
            if batch_id in self._machines:
                del self._machines[batch_id]
            
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
                
                # Remove machine
                if batch_id in self._machines:
                    del self._machines[batch_id]
                
                logger.info(f"Batch {batch.id} marked as TIMEOUT")
                return True
            
            return False
    
    # ==================== 状态机管理 ====================
    
    def _get_or_create_machine(self, batch: BatchExecute):
        """Get or create state machine for batch."""
        global BatchPhaseMachine, PhaseState
        
        batch_id = batch.id
        
        if batch_id in self._machines:
            return self._machines[batch_id]
        
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
        
        self._machines[batch_id] = machine
        return machine
    
    async def _execute_current_phase(self, machine):
        """Execute current phase of the state machine."""
        global PhaseState
        
        # 延迟导入
        if PhaseState is None:
            BatchPhaseMachine, PhaseState = _get_phase_machine_module()
        
        state = machine.state
        
        # Trigger appropriate transition based on current state
        if state == PhaseState.PENDING:
            await machine.initialize_params()
        elif state == PhaseState.FIRST_ORDER_OPEN:
            await machine.open_first_order()
        elif state == PhaseState.FIRST_ORDER_WAIT:
            # Already watching, just wait
            pass
        elif state == PhaseState.FIRST_FILLED:
            machine.proceed_to_second()
        elif state == PhaseState.SECOND_ORDER_OPEN:
            await machine.open_second_order()
        elif state == PhaseState.SECOND_ORDER_WAIT:
            # Already watching, just wait
            pass
        elif state == PhaseState.COMPLETED:
            pass
    
    def get_machine(self, batch_id: int) -> Optional[BatchPhaseMachine]:
        """Get machine by batch ID."""
        return self._machines.get(batch_id)
    
    def get_all_machines(self) -> Dict[int, BatchPhaseMachine]:
        """Get all active machines."""
        return self._machines.copy()
    
    @property
    def active_count(self) -> int:
        """Number of active state machines."""
        return len(self._machines)
