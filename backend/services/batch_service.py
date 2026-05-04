"""
Batch execution service - 业务逻辑层
处理所有批次相关的业务逻辑，供scheduler调用
"""
import asyncio
import logging
from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import select

from models.database import get_async_session, BatchExecute, PositionExecute
from events.order_watcher import SchedulerOrderWatcher
from services import create_collector, create_trader
from plugins.order_sequence import get_plugin

# Phase constants (replacing PhaseState from state machine)
class Phase:
    PENDING = 'PENDING'
    FIRST_ORDER_OPEN = 'FIRST_ORDER_OPEN'
    FIRST_ORDER_WAIT = 'FIRST_ORDER_WAIT'
    FIRST_FILLED = 'FIRST_FILLED'
    SECOND_ORDER_OPEN = 'SECOND_ORDER_OPEN'
    SECOND_ORDER_WAIT = 'SECOND_ORDER_WAIT'
    COMPLETED = 'COMPLETED'


# Slippage config
SLIPPAGE = Decimal('0.001')  # 0.1%


class BatchExecutionService:
    """
    批次执行服务 - 业务逻辑层

    职责：
    1. 处理批次唤醒的业务逻辑
    2. 处理批次执行的业务逻辑（直接if-else，无状态机）
    3. 处理订单事件的业务逻辑
    """

    def __init__(self, collector_type='binance', trader_type='binance',
                 order_plugin_name='futures_first'):
        # 创建依赖（也可以由main.py注入）
        self.collector = create_collector(collector_type)
        self.trader = create_trader(trader_type)
        self.order_plugin = get_plugin(order_plugin_name)
        self.order_watcher = SchedulerOrderWatcher(self)

    # ==================== 批次唤醒逻辑 ====================

    async def wake_pending_batches(self) -> int:
        """
        唤醒PENDING批次

        Returns:
            唤醒的批次数量
        """
        async with get_async_session() as session:
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
                batch.phase = Phase.PENDING
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
        执行批次的当前阶段

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

            # Execute current phase
            try:
                await self._execute_current_phase(batch, session)
                return True
            except Exception as e:
                logger.error(f"Batch execution error: {e}", exc_info=True)
                batch.execute_status = 'COMPLETED'
                batch.complete_reason = f'ERROR: {str(e)}'
                await session.commit()
                return False

    async def trigger_all_running(self) -> None:
        """
        触发所有RUNNING批次的执行
        由scheduler定时调用，直接执行并等待所有批次完成当前阶段
        """
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
        async with get_async_session() as session:
            result = await session.execute(
                select(BatchExecute).where(BatchExecute.id == batch_id)
            )
            batch = result.scalar_one_or_none()

        if not batch:
            logger.warning(f"No batch found for batch {batch_id}")
            return

        phase = batch.phase

        if phase == Phase.FIRST_ORDER_WAIT:
            # 第一单成交
            batch.first_side_filled_price = filled_price or batch.contract_price
            batch.phase = Phase.FIRST_FILLED
            batch.updated_at = datetime.utcnow()
            
            async with get_async_session() as session:
                await session.merge(batch)
                await session.commit()
            
            logger.info(f"Batch {batch_id} first order filled - price={filled_price}")
            
            # 如果第一单是现货，立即转币到savings
            if batch.order_sequence != 'futures_first':  # spot_first，第一单是现货
                await self._transfer_spot_to_savings(batch)
            
        elif phase == Phase.SECOND_ORDER_WAIT:
            # 第二单成交 → 完成
            batch.second_side_filled_price = filled_price or batch.spot_price
            batch.execute_status = 'COMPLETED'
            batch.complete_reason = 'SUCCESS'
            batch.phase = Phase.COMPLETED
            batch.updated_at = datetime.utcnow()
            
            async with get_async_session() as session:
                await session.merge(batch)
                await session.commit()
            
            # 如果第二单是现货（futures_first），转币到savings
            if batch.order_sequence == 'futures_first':  # futures_first，第二单是现货
                await self._transfer_spot_to_savings(batch)
            else:
                # spot_first，第二单是合约，检查仓位完成
                await self._check_position_complete(batch.position_execute_id)
            
            logger.info(f"Batch {batch_id} completed")

    async def handle_order_cancelled(self, batch_id: int) -> None:
        """
        处理订单取消事件

        Args:
            batch_id: 批次ID
        """
        async with get_async_session() as session:
            result = await session.execute(
                select(BatchExecute).where(BatchExecute.id == batch_id)
            )
            batch = result.scalar_one_or_none()

        if not batch:
            return

        phase = batch.phase

        if phase == Phase.FIRST_ORDER_WAIT:
            # 回到PENDING，下次调度重试
            batch.phase = Phase.PENDING
            batch.updated_at = datetime.utcnow()

            async with get_async_session() as session:
                await session.merge(batch)
                await session.commit()

        elif phase == Phase.SECOND_ORDER_WAIT:
            # 回到FIRST_FILLED
            batch.phase = Phase.FIRST_FILLED
            batch.updated_at = datetime.utcnow()

            async with get_async_session() as session:
                await session.merge(batch)
                await session.commit()

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
            batch.phase = Phase.PENDING
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

    # ==================== 核心：阶段执行（直接if-else，无状态机） ====================

    async def _execute_current_phase(self, batch: BatchExecute, session):
        """Execute current phase based on batch.phase (no state machine)."""
        phase = batch.phase

        if phase == Phase.PENDING:
            await self._initialize_params(batch, session)

        elif phase == Phase.FIRST_ORDER_OPEN:
            await self._open_first_order(batch, session)

        elif phase == Phase.FIRST_ORDER_WAIT:
            # Already watching, just wait
            pass

        elif phase == Phase.FIRST_FILLED:
            # Proceed to second order
            batch.phase = Phase.SECOND_ORDER_OPEN
            batch.updated_at = datetime.utcnow()
            await session.commit()

        elif phase == Phase.SECOND_ORDER_OPEN:
            await self._open_second_order(batch, session)

        elif phase == Phase.SECOND_ORDER_WAIT:
            # Already watching, just wait
            pass

        elif phase == Phase.COMPLETED:
            pass

    # ==================== 阶段具体实现 ====================

    async def _initialize_params(self, batch: BatchExecute, session):
        """Initialize trading parameters (PENDING -> FIRST_ORDER_OPEN)."""
        contract = batch.position.contract
        
        # Get initial params from plugin (one call for order sequence + prices)
        params = await self.order_plugin.get_initial_params(
            self.collector, contract, batch.batch_value
        )
        
        # Update batch
        batch.order_sequence = params.order_sequence.value
        batch.contract_price = params.contract_price
        batch.spot_price = params.spot_price
        batch.phase = Phase.FIRST_ORDER_OPEN
        batch.updated_at = datetime.utcnow()
        await session.commit()
        
        logger.info(f"Batch {batch.id}: params init - "
                   f"order={params.order_sequence.value}, "
                   f"contract={params.contract_price}, spot={params.spot_price}")

    async def _open_first_order(self, batch: BatchExecute, session):
        """Open first order (FIRST_ORDER_OPEN -> FIRST_ORDER_WAIT)."""
        amount = batch.batch_value

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
            batch.phase = Phase.FIRST_ORDER_WAIT
            batch.updated_at = datetime.utcnow()
            await session.commit()

            # Register to OrderWatcher
            await self.order_watcher.watch_order(
                batch_id=batch.id,
                order_id=batch.first_side_order_id,
                symbol=batch.position.contract,
                phase=Phase.FIRST_ORDER_WAIT,
                timeout=batch.timeout
            )

            logger.info(f"Batch {batch.id}: First order placed - {result.order_id}")
        else:
            logger.error(f"Batch {batch.id}: First order failed - {result.message}")
            raise Exception(f"Order failed: {result.message}")

    async def _open_second_order(self, batch: BatchExecute, session):
        """Open second order (SECOND_ORDER_OPEN -> SECOND_ORDER_WAIT)."""
        amount = batch.batch_value

        # Second order is opposite of first
        if batch.order_sequence == 'futures_first':
            result = await self.trader.buy_spot(
                batch.position.contract,
                amount,
                batch.spot_price
            )
        else:
            result = await self.trader.open_futures_short(
                batch.position.contract,
                amount,
                batch.contract_price
            )

        if result.success:
            batch.second_side_order_id = str(result.order_id)
            batch.phase = Phase.SECOND_ORDER_WAIT
            batch.updated_at = datetime.utcnow()
            await session.commit()

            # Register to OrderWatcher
            await self.order_watcher.watch_order(
                batch_id=batch.id,
                order_id=batch.second_side_order_id,
                symbol=batch.position.contract,
                phase=Phase.SECOND_ORDER_WAIT,
                timeout=batch.timeout
            )

            logger.info(f"Batch {batch.id}: Second order placed - {result.order_id}")
        else:
            logger.error(f"Batch {batch.id}: Second order failed - {result.message}")
            raise Exception(f"Order failed: {result.message}")

    async def _transfer_spot_to_savings(self, batch: BatchExecute):
        """Transfer spot asset to savings (called when spot order filled)."""
        # Calculate actual quantity to transfer
        spot_quantity = (batch.batch_value or 1000) / batch.spot_price if batch.spot_price else 0
        asset = batch.position.contract.replace('USDT', '')
        
        # Transfer to savings
        transfer_result = await self.trader.transfer_to_savings(
            batch.position.contract,
            round(spot_quantity, 6)
        )
        
        logger.info(f"Batch {batch.id}: transferred {spot_quantity} {asset} to savings")
        
        # Check if position complete
        await self._check_position_complete(batch.position_execute_id)

    # ==================== 辅助方法 ====================

    async def _check_position_complete(self, position_execute_id: int):
        """Check if all batches for a position are complete, update position status."""
        async with get_async_session() as session:
            # Get all batches for this position
            result = await session.execute(
                select(BatchExecute).where(
                    BatchExecute.position_execute_id == position_execute_id
                )
            )
            batches = list(result.scalars().all())

            # Check if all completed
            if all(b.phase == Phase.COMPLETED for b in batches):
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
