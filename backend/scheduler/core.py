"""
Scheduler layer - 纯定时触发，通过回调函数解耦
只有一个调度器，两个调度任务：唤醒和执行
"""
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger


class TradingScheduler:
    """交易调度器 - 纯定时触发，管理多个调度任务"""
    
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self._wake_callback = None
        self._execute_callback = None
    
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
        
        self.scheduler.start()
        print("TradingScheduler started with 2 tasks")
    
    def stop(self):
        """停止调度器"""
        self.scheduler.shutdown()
        print("TradingScheduler stopped")
    
    async def _wake_trigger(self):
        """定时触发唤醒任务 - 只调用回调函数"""
        if self._wake_callback:
            await self._wake_callback()
    
    async def _execute_trigger(self):
        """定时触发执行任务 - 只调用回调函数"""
        if self._execute_callback:
            await self._execute_callback()
