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
from events.order_watcher import UnifiedOrderWatcher
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
        self.order_watcher = UnifiedOrderWatcher(self)

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

    # ==================== 订单事件逻辑（统一入口） ====================

    async def handle_order_update(self, update: 'OrderUpdate'):
        """
        统一处理订单状态更新（由OrderWatcher回调）
        所有业务逻辑（phase转换、转币等）都在这里处理
        """
        order_id = update.order_id
        batch_id = update.batch_id
        phase = update.phase
        is_spot = update.is_spot
        
        if not batch_id or not phase:
            return
        
        async with get_async_session() as session:
            result = await session.execute(
                select(BatchExecute).where(BatchExecute.id == batch_id)
            )
            batch = result.scalar_one_or_none()
            
            if not batch:
                return
            
            # 处理不同状态
            if update.status == OrderStatus.FILLED:
                await self._handle_filled(batch, phase, update.avg_price, is_spot, session)
            elif update.status in (OrderStatus.CANCELLED, OrderStatus.REJECTED):
                await self._handle_cancelled(batch, phase, session)
    
    async def _handle_filled(self, batch, phase, filled_price, is_spot, session):
        """处理订单成交"""
        if phase == Phase.FIRST_ORDER_WAIT:
            # 第一单成交
            batch.first_side_filled_price = filled_price or batch.contract_price
            batch.phase = Phase.FIRST_FILLED
            batch.updated_at = datetime.utcnow()
            await session.commit()
            
            logger.info(f"Batch {batch.id} first order filled - price={filled_price}")
            
            # 如果第一单是现货，立即转币到savings
            if is_spot:
                await self._transfer_spot_to_savings(batch)
                
        elif phase == Phase.SECOND_ORDER_WAIT:
            # 第二单成交 → 完成
            batch.second_side_filled_price = filled_price or batch.spot_price
            batch.execute_status = 'COMPLETED'
            batch.complete_reason = 'SUCCESS'
            batch.phase = Phase.COMPLETED
            batch.updated_at = datetime.utcnow()
            await session.commit()
            
            # 如果第二单是现货，转币到savings
            if is_spot:
                await self._transfer_spot_to_savings(batch)
            else:
                # 第二单是合约，检查仓位完成
                await self._check_position_complete(batch.position_execute_id)
            
            logger.info(f"Batch {batch.id} completed")
    
    async def _handle_cancelled(self, batch, phase, session):
        """处理订单取消/拒绝"""
        if phase == Phase.FIRST_ORDER_WAIT:
            # 回到PENDING，下次调度重试
            batch.phase = Phase.PENDING
            batch.updated_at = datetime.utcnow()
            await session.commit()
        elif phase == Phase.SECOND_ORDER_WAIT:
            # 回到FIRST_FILLED
            batch.phase = Phase.FIRST_FILLED
            batch.updated_at = datetime.utcnow()
            await session.commit()
    
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

            # Register to OrderWatcher (is_spot depends on order sequence)
            is_spot = (batch.order_sequence != 'futures_first')  # spot_first means first order is spot
            await self.order_watcher.watch_order(
                batch.id,
                batch.first_side_order_id,
                batch.position.contract,
                Phase.FIRST_ORDER_WAIT,
                is_spot
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

            # Register to OrderWatcher (is_spot depends on order sequence)
            is_spot = (batch.order_sequence == 'futures_first')  # futures_first means second order is spot
            await self.order_watcher.watch_order(
                batch.id,
                batch.second_side_order_id,
                batch.position.contract,
                Phase.SECOND_ORDER_WAIT,
                is_spot
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

    # ==================== 订单轮询方法（Scheduler Job 调用） ====================

    async def poll_spot_orders(self):
        """
        轮询现货订单状态（由 Scheduler Job 调用）
        查询 DB 中现货的 *_WAIT 订单，获取订单详情，触发业务逻辑
        """
        # 1. 查询 DB：所有 RUNNING 且 phase 为 *_WAIT 的批次
        async with get_async_session() as session:
            from sqlalchemy import select, or_

            result = await session.execute(
                select(BatchExecute).where(
                    BatchExecute.execute_status == 'RUNNING',
                    or_(
                        BatchExecute.phase == 'FIRST_ORDER_WAIT',
                        BatchExecute.phase == 'SECOND_ORDER_WAIT'
                    )
                )
            )
            waiting_batches = result.scalars().all()

        if not waiting_batches:
            return

        # 2. 过滤出现货订单（根据 order_sequence 判断）
        for batch in waiting_batches:
            is_spot = self._is_spot_order(batch)

            if not is_spot:
                continue

            await self._poll_single_order(batch, is_spot=True)

    async def poll_futures_orders(self):
        """
        轮询合约订单状态（由 Scheduler Job 调用）
        查询 DB 中合约的 *_WAIT 订单，获取订单详情，触发业务逻辑
        """
        # 1. 查询 DB：所有 RUNNING 且 phase 为 *_WAIT 的批次
        async with get_async_session() as session:
            from sqlalchemy import select, or_

            result = await session.execute(
                select(BatchExecute).where(
                    BatchExecute.execute_status == 'RUNNING',
                    or_(
                        BatchExecute.phase == 'FIRST_ORDER_WAIT',
                        BatchExecute.phase == 'SECOND_ORDER_WAIT'
                    )
                )
            )
            waiting_batches = result.scalars().all()

        if not waiting_batches:
            return

        # 2. 过滤出合约订单
        for batch in waiting_batches:
            is_spot = self._is_spot_order(batch)

            if is_spot:
                continue

            await self._poll_single_order(batch, is_spot=False)

    def _is_spot_order(self, batch: BatchExecute) -> bool:
        """判断当前等待的订单是否为现货订单"""
        if batch.phase == 'FIRST_ORDER_WAIT':
            # 第一单是现货，当且仅当 order_sequence 不是 futures_first
            return batch.order_sequence != 'futures_first'
        elif batch.phase == 'SECOND_ORDER_WAIT':
            # 第二单是现货，当且仅当 order_sequence 是 futures_first
            return batch.order_sequence == 'futures_first'
        return False

    def _get_order_id(self, batch: BatchExecute) -> str:
        """获取当前等待阶段的订单ID"""
        if batch.phase == 'FIRST_ORDER_WAIT':
            return batch.first_side_order_id
        elif batch.phase == 'SECOND_ORDER_WAIT':
            return batch.second_side_order_id
        return None

    async def _poll_single_order(self, batch: BatchExecute, is_spot: bool):
        """轮询单个订单，触发业务逻辑"""
        order_id = self._get_order_id(batch)

        if not order_id:
            return

        try:
            status_data = await self.trader.get_order_status(
                batch.position.contract,
                int(order_id),
                is_spot=is_spot
            )

            status = status_data.get('status', 'UNKNOWN')
            avg_price = float(status_data.get('avgPrice', 0))

            # 检查是否为终态
            if status == 'FILLED':
                update = OrderUpdate(
                    order_id=order_id,
                    symbol=batch.position.contract,
                    status=OrderStatus.FILLED,
                    avg_price=avg_price,
                    batch_id=batch.id,
                    phase=batch.phase,
                    is_spot=is_spot
                )
                logger.info(f"Poll: Batch {batch.id} order {order_id} FILLED")
                await self.handle_order_update(update)

            elif status in ('CANCELLED', 'EXPIRED', 'REJECTED'):
                update = OrderUpdate(
                    order_id=order_id,
                    symbol=batch.position.contract,
                    status=OrderStatus(status),
                    batch_id=batch.id,
                    phase=batch.phase,
                    is_spot=is_spot
                )
                logger.info(f"Poll: Batch {batch.id} order {order_id} {status}")
                await self.handle_order_update(update)

        except Exception as e:
            logger.warning(f"Poll error for batch {batch.id} order {order_id}: {e}")
