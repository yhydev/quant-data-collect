"""
Scheduler layer - 纯定时触发，通过回调函数解耦
只有一个调度器，两个调度任务：唤醒和执行
"""
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
import logging


logger = logging.getLogger(__name__)


class TradingScheduler:
    """交易调度器 - 纯定时触发，管理多个调度任务"""
    
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self._wake_callback = None
        self._execute_callback = None
        self._poll_callback = None
        self._funding_sync_callback = None
        self._position_reconcile_callback = None
    
    def set_wake_callback(self, callback):
        """设置唤醒回调函数（由main.py注入）
        callback签名：async def callback()
        """
        self._wake_callback = callback
    
    def set_execute_callback(self, callback):
        """设置执行回调函数（由main.py注入）
        callback签名：async def callback()
        """
        self._execute_callback = callback
    
    def set_poll_callback(self, callback):
        """设置订单轮询回调函数（现货+合约，一个任务）
        callback签名：async def callback()
        """
        self._poll_callback = callback

    def set_funding_sync_callback(self, callback):
        """设置资金费率历史同步回调函数（每小时）"""
        self._funding_sync_callback = callback

    def set_position_reconcile_callback(self, callback):
        """设置仓位状态对账回调函数"""
        self._position_reconcile_callback = callback
    
    def start(self):
        """启动调度器 - 注册所有定时任务"""
        if self._wake_callback is None:
            raise RuntimeError("Wake callback not set. Call set_wake_callback() first.")
        if self._execute_callback is None:
            raise RuntimeError("Execute callback not set. Call set_execute_callback() first.")
        
        # 唤醒任务
        self.scheduler.add_job(
            self._wake_trigger,
            trigger=IntervalTrigger(seconds=1),
            id='wake_pending_batches',
            replace_existing=True
        )
        
        # 执行任务
        self.scheduler.add_job(
            self._execute_trigger,
            trigger=IntervalTrigger(seconds=1),
            id='execute_running_batches',
            replace_existing=True
        )
        
        # 订单轮询任务（现货+合约，一个任务）
        if self._poll_callback:
            self.scheduler.add_job(
                self._poll_trigger,
                trigger=IntervalTrigger(seconds=5),
                id='poll_order_status',
                replace_existing=True
            )

        # 资金费率历史同步任务（每小时）
        if self._funding_sync_callback:
            self.scheduler.add_job(
                self._funding_sync_trigger,
                trigger=IntervalTrigger(hours=1),
                id='sync_funding_rate_history',
                replace_existing=True
            )

        # 仓位状态对账任务
        if self._position_reconcile_callback:
            self.scheduler.add_job(
                self._position_reconcile_trigger,
                trigger=IntervalTrigger(seconds=5),
                id='reconcile_running_positions',
                replace_existing=True
            )
        
        self.scheduler.start()
        print("TradingScheduler started with 3 tasks")
    
    def stop(self):
        """停止调度器"""
        self.scheduler.shutdown()
        print("TradingScheduler stopped")
    
    async def _wake_trigger(self):
        """定时触发唤醒任务 - 只调用回调函数"""
        if self._wake_callback:
            try:
                woken = await self._wake_callback()
                logger.info("Scheduler wake trigger executed, woken=%s", woken)
            except Exception:
                logger.exception("Scheduler wake trigger failed")
    
    async def _execute_trigger(self):
        """定时触发执行任务 - 只调用回调函数"""
        if self._execute_callback:
            try:
                await self._execute_callback()
                logger.info("Scheduler execute trigger executed")
            except Exception:
                logger.exception("Scheduler execute trigger failed")
    
    async def _poll_trigger(self):
        """定时触发订单轮询 - 只调用回调函数"""
        if self._poll_callback:
            try:
                await self._poll_callback()
            except Exception:
                logger.exception("Scheduler poll trigger failed")

    async def _funding_sync_trigger(self):
        """定时触发资金费率历史同步 - 只调用回调函数"""
        if self._funding_sync_callback:
            try:
                await self._funding_sync_callback()
            except Exception:
                logger.exception("Scheduler funding sync trigger failed")

    async def _position_reconcile_trigger(self):
        """定时触发仓位状态对账 - 只调用回调函数"""
        if self._position_reconcile_callback:
            try:
                reconciled = await self._position_reconcile_callback()
                logger.info("Scheduler position reconcile executed, reconciled=%s", reconciled)
            except Exception:
                logger.exception("Scheduler position reconcile trigger failed")
