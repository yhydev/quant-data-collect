"""
Phase Service - 简化版，只管理OrderWatcher生命周期
事件驱动机制已移除，批次执行改为调度器直接调用
"""
import logging
from typing import Optional

logger = logging.getLogger(__name__)

from services.batch_service import BatchExecutionService
from events.order_watcher import SchedulerOrderWatcher


class PhaseServiceConfig:
    """Phase service configuration - 简化后无需配置"""
    pass


class PhaseService:
    """
    Phase Service - 简化后只管理OrderWatcher
    """

    def __init__(self, batch_service: BatchExecutionService, config: PhaseServiceConfig = None):
        """Initialize phase service with batch_service dependency."""
        self.batch_service = batch_service
        self.config = config or PhaseServiceConfig()

        # Create OrderWatcher (needs access to trader from batch_service)
        self.order_watcher = SchedulerOrderWatcher(batch_service)

        # Running state
        self._running = False

    # ==================== Properties for SchedulerOrderWatcher ====================

    @property
    def trader(self):
        """Expose trader for SchedulerOrderWatcher."""
        return self.batch_service.trader

    # ==================== Lifecycle ====================

    async def start(self):
        """Start the service."""
        self._running = True

        # Start order watcher
        await self.order_watcher.start()

        logger.info("PhaseService started")

    async def stop(self):
        """Stop the service gracefully."""
        self._running = False

        # Stop order watcher
        await self.order_watcher.stop()

        logger.info("PhaseService stopped")
