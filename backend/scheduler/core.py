"""
Scheduler layer - 只做定时触发，业务逻辑在services层
"""
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from services.batch_service import BatchExecutionService
from events.phase_service import PhaseService, PhaseServiceConfig


class WakeScheduler:
    """唤醒调度器 - 定时触发唤醒任务"""
    
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self._batch_service = None
    
    def set_batch_service(self, batch_service):
        """设置BatchExecutionService依赖"""
        self._batch_service = batch_service
    
    def start(self):
        """启动唤醒调度器"""
        if self._batch_service is None:
            raise RuntimeError("BatchService not set. Call set_batch_service() first.")
        
        self.scheduler.add_job(
            self._trigger_wake,
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
    
    async def _trigger_wake(self):
        """定时触发唤醒 - 只调用service层"""
        if self._batch_service:
            woken = self._batch_service.wake_pending_batches()
            if woken > 0:
                print(f"Woke {woken} pending batches")


class ExecuteScheduler:
    """执行调度器 - 定时触发批次执行"""
    
    def __init__(self, phase_service=None):
        self.phase_service = phase_service
        self._batch_service = None
        self.scheduler = AsyncIOScheduler()
    
    def set_phase_service(self, phase_service):
        """设置PhaseService依赖"""
        self.phase_service = phase_service
    
    def set_batch_service(self, batch_service):
        """设置BatchExecutionService依赖"""
        self._batch_service = batch_service
    
    def start(self):
        """启动执行调度器"""
        if self.phase_service is None:
            raise RuntimeError("PhaseService not set. Call set_phase_service() first.")
        
        # Start phase service (includes order watcher)
        asyncio.create_task(self.phase_service.start())
        
        # Job: 定时触发执行事件
        self.scheduler.add_job(
            self._trigger_execute,
            trigger=IntervalTrigger(seconds=1),
            id='execute_running_batches',
            replace_existing=True
        )
        self.scheduler.start()
        print("ExecuteScheduler started")
    
    async def stop(self):
        """停止执行调度器"""
        if self.phase_service:
            await self.phase_service.stop()
        self.scheduler.shutdown()
        print("ExecuteScheduler stopped")
    
    async def _trigger_execute(self):
        """定时触发执行 - 只调用service层"""
        if not self.phase_service:
            return
        
        # 让BatchExecutionService处理所有逻辑
        if self._batch_service:
            await self._batch_service.trigger_all_running()
        else:
            # 如果没有batch_service，直接通过phase_service发布事件
            await self._trigger_all_running()
    
    async def _trigger_all_running(self):
        """备用方法：直接通过phase_service触发"""
        from models.database import get_async_session, BatchExecute
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
                
                if contract in contracts_processed:
                    continue
                contracts_processed.add(contract)
                
                try:
                    await self.phase_service.publish_batch_execute(batch.id)
                except Exception as e:
                    print(f"Error triggering batch {batch.id}: {e}")
