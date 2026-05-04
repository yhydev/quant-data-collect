"""
Scheduler layer - 纯定时触发，通过回调函数解耦
scheduler不知道任何业务逻辑，只负责定时调用回调函数
"""
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger


class WakeScheduler:
    """唤醒调度器 - 纯定时触发"""
    
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self._callback = None
    
    def set_callback(self, callback):
        """设置回调函数（由main.py注入）
        callback签名：async def callback()
        """
        self._callback = callback
    
    def start(self):
        """启动调度器 - 只注册定时任务"""
        if self._callback is None:
            raise RuntimeError("Callback not set. Call set_callback() first.")
        
        self.scheduler.add_job(
            self._trigger,
            trigger=IntervalTrigger(seconds=1),
            id='wake_pending_batches',
            replace_existing=True
        )
        self.scheduler.start()
        print("WakeScheduler started")
    
    def stop(self):
        """停止调度器"""
        self.scheduler.shutdown()
        print("WakeScheduler stopped")
    
    async def _trigger(self):
        """定时触发 - 只调用回调函数"""
        if self._callback:
            await self._callback()


class ExecuteScheduler:
    """执行调度器 - 纯定时触发"""
    
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self._callback = None
    
    def set_callback(self, callback):
        """设置回调函数（由main.py注入）
        callback签名：async def callback()
        """
        self._callback = callback
    
    def start(self):
        """启动调度器 - 只注册定时任务"""
        if self._callback is None:
            raise RuntimeError("Callback not set. Call set_callback() first.")
        
        self.scheduler.add_job(
            self._trigger,
            trigger=IntervalTrigger(seconds=1),
            id='execute_running_batches',
            replace_existing=True
        )
        self.scheduler.start()
        print("ExecuteScheduler started")
    
    def stop(self):
        """停止调度器"""
        self.scheduler.shutdown()
        print("ExecuteScheduler stopped")
    
    async def _trigger(self):
        """定时触发 - 只调用回调函数"""
        if self._callback:
            await self._callback()
