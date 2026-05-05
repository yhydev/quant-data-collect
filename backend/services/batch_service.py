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

from models.database import get_async_session, BatchExecute, PositionExecute, BatchPhaseHistory
from events.order_watcher import OrderUpdate, OrderStatus
from services import create_collector, create_trader
from plugins.order_sequence import get_plugin
from services.rule_executer_service import RuleExecuterService
from services.arbitrage_service import ArbitrageService


logger = logging.getLogger(__name__)

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
        self.arbitrage_service = ArbitrageService()
        self.order_plugin = get_plugin(order_plugin_name)
        self.phase_service = None
        self.rule_executer_service = RuleExecuterService()

    def set_phase_service(self, phase_service) -> None:
        """Set phase service reference for compatibility."""
        self.phase_service = phase_service

    def set_batch_phase(self, batch: BatchExecute, to_phase: str, session, trigger: str = 'SYSTEM', note: str | None = None) -> None:
        """Set batch phase and persist transition history when changed."""
        from_phase = batch.phase
        if from_phase == to_phase:
            return

        batch.phase = to_phase
        batch.updated_at = datetime.utcnow()
        session.add(
            BatchPhaseHistory(
                batch_execute_id=batch.id,
                from_phase=from_phase,
                to_phase=to_phase,
                trigger=trigger,
                note=note,
            )
        )

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
                self.set_batch_phase(batch, Phase.PENDING, session, trigger='WAKE_BATCH')
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

            # Check timeout (only while waiting for order fills)
            if self._is_wait_phase(batch.phase) and batch.updated_at:
                elapsed = (datetime.utcnow() - batch.updated_at).total_seconds()
            else:
                elapsed = 0

            if batch.phase == Phase.FIRST_ORDER_WAIT and elapsed > (batch.first_order_wait_timeout or 300):
                self.set_batch_phase(batch, Phase.PENDING, session, trigger='FIRST_ORDER_WAIT_TIMEOUT')
                await session.commit()
                logger.warning(
                    "Batch %s first order wait timeout (%ss), reset to PENDING",
                    batch.id,
                    batch.first_order_wait_timeout or 300,
                )
                return False

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
                await self._handle_filled(
                    batch,
                    phase,
                    update.avg_price,
                    is_spot,
                    session,
                    update.executed_qty,
                )
            elif update.status in (OrderStatus.CANCELLED, OrderStatus.REJECTED):
                await self._handle_cancelled(batch, phase, session)
    
    async def _handle_filled(self, batch, phase, filled_price, is_spot, session, executed_qty: float | None = None):
        """处理订单成交"""
        if phase == Phase.FIRST_ORDER_WAIT:
            # 第一单成交
            batch.first_side_filled_price = filled_price or batch.contract_price
            self.set_batch_phase(batch, Phase.FIRST_FILLED, session, trigger='ORDER_FILLED')
            await session.commit()
            
            logger.info(f"Batch {batch.id} first order filled - price={filled_price}")
            
            # 如果第一单是现货，立即转币到savings
            if is_spot:
                await self._transfer_spot_to_savings(batch, executed_qty)
                
        elif phase == Phase.SECOND_ORDER_WAIT:
            # 第二单成交 → 完成
            batch.second_side_filled_price = filled_price or batch.spot_price
            batch.execute_status = 'COMPLETED'
            batch.complete_reason = 'SUCCESS'
            self.set_batch_phase(batch, Phase.COMPLETED, session, trigger='ORDER_FILLED')
            await session.commit()
            
            # 如果第二单是现货，转币到savings
            if is_spot:
                await self._transfer_spot_to_savings(batch, executed_qty)
            else:
                # 第二单是合约，检查仓位完成
                await self._check_position_complete(batch.position_execute_id)
            
            logger.info(f"Batch {batch.id} completed")
    
    async def _handle_cancelled(self, batch, phase, session):
        """处理订单取消/拒绝"""
        if phase == Phase.FIRST_ORDER_WAIT:
            # 回到PENDING，下次调度重试
            self.set_batch_phase(batch, Phase.PENDING, session, trigger='ORDER_CANCELLED')
            await session.commit()
        elif phase == Phase.SECOND_ORDER_WAIT:
            # 回到FIRST_FILLED
            self.set_batch_phase(batch, Phase.FIRST_FILLED, session, trigger='ORDER_CANCELLED')
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

            await self._cancel_outstanding_orders(batch)

            # Reset phase-related runtime data before re-initialization
            batch.contract_price = None
            batch.spot_price = None
            batch.first_side_order_id = None
            batch.first_side_filled_price = None
            batch.second_side_order_id = None
            batch.second_side_filled_price = None
            batch.complete_reason = None

            batch.execute_status = 'RUNNING'
            self.set_batch_phase(batch, Phase.PENDING, session, trigger='RESET_BATCH')
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

    # ==================== 核心：阶段执行（规则执行器） ====================

    async def _execute_current_phase(self, batch: BatchExecute, session):
        """Execute current phase through rule executer."""
        await self.rule_executer_service.execute(batch, session, self)

    async def _transfer_spot_to_savings(self, batch: BatchExecute, executed_qty: float | None = None):
        """Transfer spot asset to savings (called when spot order filled)."""
        if executed_qty is not None and executed_qty > 0:
            spot_quantity = float(executed_qty)
        else:
            spot_quantity = (batch.batch_value or 0) / batch.spot_price if batch.spot_price else 0

        if spot_quantity <= 0:
            raise Exception("Transfer to savings skipped: invalid spot quantity")

        transfer_result = await self.arbitrage_service.transfer_to_savings(
            self.trader,
            batch.position.contract,
            round(spot_quantity, 6),
        )
        if not transfer_result.success:
            raise Exception(f"Transfer to savings failed: {transfer_result.message}")

        asset = batch.position.contract.replace('USDT', '')
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
            if batches and all(b.execute_status == 'COMPLETED' for b in batches):
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

    async def reconcile_running_positions(self) -> int:
        """Finalize RUNNING positions when all related batches are COMPLETED."""
        async with get_async_session() as session:
            result = await session.execute(
                select(PositionExecute).where(PositionExecute.execute_status == 'RUNNING')
            )
            running_positions = list(result.scalars().all())

            finalized = 0
            for pos in running_positions:
                batches_result = await session.execute(
                    select(BatchExecute).where(BatchExecute.position_execute_id == pos.id)
                )
                batches = list(batches_result.scalars().all())
                if not batches or any(b.execute_status != 'COMPLETED' for b in batches):
                    continue

                reasons = [b.complete_reason for b in batches]
                if 'TIMEOUT' in reasons:
                    overall = 'TIMEOUT'
                elif any('ERROR' in r for r in reasons if r):
                    overall = 'ERROR'
                elif 'CANCELLED' in reasons:
                    overall = 'CANCELLED'
                else:
                    overall = 'SUCCESS'

                pos.execute_status = 'COMPLETED'
                pos.complete_reason = overall
                pos.updated_at = datetime.utcnow()
                finalized += 1

            if finalized > 0:
                await session.commit()
                logger.info("Reconciled %s running positions to COMPLETED", finalized)

            return finalized

    # ==================== 订单轮询方法（一个 Scheduler Job 调用） ====================

    async def poll_order_status(self):
        """
        轮询所有等待中的订单状态（现货+合约，一个调度任务）
        查询 DB 中 *_WAIT 订单，根据类型路由到不同的轮询逻辑
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

        # 2. 轮询每个订单（根据类型自动路由）
        for batch in waiting_batches:
            is_spot = self._is_spot_order(batch)
            order_id = self._get_order_id(batch)

            logger.info(
                "Poll: Batch %s phase=%s order_sequence=%s is_spot=%s order_id=%s",
                batch.id,
                batch.phase,
                batch.order_sequence,
                is_spot,
                order_id,
            )

            if not order_id:
                continue

            await self._poll_single_order(batch, order_id, is_spot)

    def _is_spot_order(self, batch: BatchExecute) -> bool:
        """判断当前等待的订单是否为现货订单"""
        order_sequence = (batch.order_sequence or '').strip().lower()
        if batch.phase == 'FIRST_ORDER_WAIT':
            # 第一单是现货，当且仅当 order_sequence 不是 futures_first
            return order_sequence != 'futures_first'
        elif batch.phase == 'SECOND_ORDER_WAIT':
            # 第二单是现货，当且仅当 order_sequence 是 futures_first
            return order_sequence == 'futures_first'
        return False

    def _is_wait_phase(self, phase: str) -> bool:
        """Only wait phases should be timeout-checked."""
        return phase in (Phase.FIRST_ORDER_WAIT, Phase.SECOND_ORDER_WAIT)

    def _get_order_id(self, batch: BatchExecute) -> str:
        """获取当前等待阶段的订单ID"""
        if batch.phase == 'FIRST_ORDER_WAIT':
            return batch.first_side_order_id
        elif batch.phase == 'SECOND_ORDER_WAIT':
            return batch.second_side_order_id
        return None

    async def _cancel_outstanding_orders(self, batch: BatchExecute) -> None:
        """Cancel possible stale open orders before batch reset."""
        symbol = batch.position.contract
        order_sequence = (batch.order_sequence or '').strip().lower()
        candidates: list[tuple[str, bool]] = []

        if batch.first_side_order_id:
            is_spot_first = order_sequence != 'futures_first'
            candidates.append((batch.first_side_order_id, is_spot_first))

        if batch.second_side_order_id:
            is_spot_second = order_sequence == 'futures_first'
            candidates.append((batch.second_side_order_id, is_spot_second))

        for order_id, is_spot in candidates:
            try:
                status_data = await self.trader.get_order_status(symbol, int(order_id), is_spot=is_spot)
                status = str(status_data.get('status', '')).upper()
                if status in ('FILLED', 'CANCELED', 'CANCELLED', 'REJECTED', 'EXPIRED'):
                    continue

                result = await self.trader.cancel_order(symbol, int(order_id), is_spot=is_spot)
                if result.success:
                    logger.info("Batch %s cancelled stale order %s (is_spot=%s)", batch.id, order_id, is_spot)
                else:
                    logger.warning("Batch %s failed cancelling stale order %s: %s", batch.id, order_id, result.message)
            except Exception as exc:
                logger.warning("Batch %s cancel stale order %s error: %s", batch.id, order_id, exc)

    async def _poll_single_order(self, batch: BatchExecute, order_id: str, is_spot: bool):
        """轮询单个订单，触发业务逻辑"""
        try:
            status_data = await self.trader.get_order_status(
                batch.position.contract,
                int(order_id),
                is_spot=is_spot
            )

            status = status_data.get('status', 'UNKNOWN')
            avg_price = float(status_data.get('avgPrice', 0))
            executed_qty_raw = status_data.get('executedQty')
            if executed_qty_raw is None:
                executed_qty_raw = status_data.get('cumQty')
            if executed_qty_raw is None:
                executed_qty_raw = status_data.get('origQty')
            try:
                executed_qty = float(executed_qty_raw) if executed_qty_raw is not None else None
            except (TypeError, ValueError):
                executed_qty = None

            # 检查是否为终态
            if status == 'FILLED':
                update = OrderUpdate(
                    order_id=order_id,
                    symbol=batch.position.contract,
                    status=OrderStatus.FILLED,
                    avg_price=avg_price,
                    executed_qty=executed_qty,
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
