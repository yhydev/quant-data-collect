"""
Core schedulers refactored:
- WakeScheduler: 统一唤醒PENDING批次
- ExecuteScheduler: 基于状态机路由执行RUNNING批次
"""
import asyncio
from datetime import datetime
from decimal import Decimal
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from sqlalchemy import select

from .modules import create_collector, create_trader, PortfolioManager, LockManager
from .plugins.order_sequence import get_plugin
from .database import get_async_session, BatchExecute
from .modules.phase_service import PhaseService, PhaseServiceConfig


SLIPPAGE = Decimal('0.001')
DEFAULT_ORDER_TIMEOUT = 300


class WakeScheduler:
    """统一唤醒调度器 - 负责将PENDING批次转为RUNNING"""
    
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
    
    def start(self):
        """启动唤醒调度器"""
        self.scheduler.add_job(
            self._wake_pending_batches,
            trigger=IntervalTrigger(seconds=1),
            id='wake_pending_batches',
            replace_existing=True
        )
        self.scheduler.start()
        print("WakeScheduler started")
    
    def stop(self):
        """停止唤醒调度器"""
        self.scheduler.shutdown()
        print("WakeScheduler stopped")
    
    @staticmethod
    async def _wake_pending_batches():
        """唤醒PENDING批次，每个合约只唤醒ID最小的"""
        async with get_async_session() as session:
            # 1. 获取已有RUNNING的合约
            result = await session.execute(
                select(BatchExecute).where(BatchExecute.execute_status == 'RUNNING')
            )
            contracts_running = {batch.position.contract for batch in result.scalars().all()}
            
            # 2. 获取所有PENDING批次，按合约分组取最小ID
            result = await session.execute(
                select(BatchExecute)
                .where(BatchExecute.execute_status == 'PENDING')
                .order_by(BatchExecute.id)
            )
            
            # 每个合约只取第一个（ID最小）
            contract_min_batch = {}
            for batch in result.scalars().all():
                contract = batch.position.contract
                if contract not in contract_min_batch:
                    contract_min_batch[contract] = batch
            
            # 3. 唤醒不在RUNNING中的合约批次
            woken = 0
            for contract, batch in contract_min_batch.items():
                if contract not in contracts_running:
                    batch.execute_status = 'RUNNING'
                    batch.updated_at = datetime.utcnow()
                    await session.commit()
                    woken += 1
            
            if woken > 0:
                print(f"Woke {woken} pending batches")


class ExecuteScheduler:
    """执行调度器 - 基于状态机路由执行RUNNING批次"""
    
    def __init__(self, collector_type: str = 'binance',
                 trader_type: str = 'binance',
                 order_plugin: str = 'futures_first'):
        self.collector = create_collector(collector_type)
        self.trader = create_trader(trader_type)
        self.portfolio = PortfolioManager()
        self.lock_manager = LockManager()
        self.order_plugin = get_plugin(order_plugin)
        self.scheduler = AsyncIOScheduler()
        
        # Phase service for state machine routing
        self.phase_service = PhaseService(PhaseServiceConfig(
            collector_type=collector_type,
            trader_type=trader_type,
            order_plugin=order_plugin
        ))
    
    def start(self):
        """启动执行调度器"""
        # Start phase service (includes order watcher)
        asyncio.create_task(self.phase_service.start())
        
        # Job: 执行RUNNING批次
        self.scheduler.add_job(
            self._execute_running_batches,
            trigger=IntervalTrigger(seconds=1),
            id='execute_running_batches',
            replace_existing=True
        )
        self.scheduler.start()
        print("ExecuteScheduler started with PhaseService")
    
    async def stop(self):
        """停止执行调度器"""
        await self.phase_service.stop()
        self.scheduler.shutdown()
        print("ExecuteScheduler stopped")
    
    async def _execute_running_batches(self):
        """执行RUNNING批次 - 通过PhaseService基于状态机路由"""
        async with get_async_session() as session:
            # 获取所有RUNNING批次
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
                    # 检查超时
                    elapsed = (datetime.utcnow() - batch.updated_at).total_seconds()
                    if elapsed > batch.timeout:
                        batch.execute_status = 'COMPLETED'
                        batch.complete_reason = 'TIMEOUT'
                        await session.commit()
                        continue
                    
                    # 通过PhaseService发布执行事件（状态机会自动路由）
                    await self.phase_service.publish_batch_execute(batch.id)
                    
                except Exception as e:
                    print(f"Error executing batch {batch.id}: {e}")
                    batch.execute_status = 'COMPLETED'
                    batch.complete_reason = f'ERROR: {str(e)}'
                    await session.commit()
