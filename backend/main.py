"""FastAPI application for Binance Arbitrage Platform."""
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from typing import AsyncGenerator

from api.routes import router as api_router
from models.database import init_db_async

# 导入各层组件
from scheduler.core import TradingScheduler
from services.batch_service import BatchExecutionService
from services.funding_rate_sync_service import FundingRateSyncService
from events.phase_service import PhaseService, PhaseServiceConfig
from settings import settings


# Global scheduler instances
trading_scheduler: TradingScheduler | None = None
batch_service: BatchExecutionService | None = None
phase_service: PhaseService | None = None
funding_rate_sync_service: FundingRateSyncService | None = None


def setup_logging() -> None:
    """Setup basic logging configuration."""
    log_level_name = str(settings.get("log_level", "INFO")).upper()
    log_level = getattr(logging, log_level_name, logging.INFO)
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    )
    # Silence APScheduler logs during troubleshooting
    logging.getLogger("apscheduler").setLevel(logging.CRITICAL)
    logging.getLogger("apscheduler").propagate = False


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan handler."""
    global trading_scheduler, batch_service, phase_service, funding_rate_sync_service
    
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    # Startup
    logger.info("Starting application...")
    
    # Initialize async database
    try:
        await init_db_async()
        logger.info("Database initialized")
    except Exception as e:
        logger.warning(f"Database init warning: {e}")
    
    # ========== 依赖注入：main.py负责组装所有组件 ==========
    
    # 1. 创建services层（BatchExecutionService自己创建依赖）
    # 使用 mock 模式进行测试，不依赖真实币安API
    batch_service = BatchExecutionService(
        collector_type=settings.get('collector_type', 'mock'),
        trader_type=settings.get('trader_type', 'mock'),
        order_plugin_name=settings.get('order_plugin_name', 'futures_first')
    )
    
    # 2. 创建events层（需要batch_service依赖）
    phase_service = PhaseService(batch_service)

    # 2.1 创建资金费率同步服务（每小时全量同步近10天）
    funding_rate_sync_service = FundingRateSyncService(
        collector=batch_service.collector,
        days=10,
        page_limit=1000,
    )
    
    # 3. 注入依赖（service -> events）
    batch_service.set_phase_service(phase_service)
    
    # 4. 创建scheduler层（纯触发，一个调度器两个任务）
    trading_scheduler = TradingScheduler()
    
    # 5. 注入依赖到scheduler（通过callback解耦）
    # Wake callback
    trading_scheduler.set_wake_callback(lambda: batch_service.wake_pending_batches())
    
    # Execute callback
    async def execute_callback():
        await batch_service.trigger_all_running()
    trading_scheduler.set_execute_callback(execute_callback)
    
    # Poll orders callback (spot + futures in one job)
    async def poll_orders_callback():
        await batch_service.poll_order_status()
    trading_scheduler.set_poll_callback(poll_orders_callback)

    # Funding rate history sync callback
    async def funding_sync_callback():
        try:
            await funding_rate_sync_service.sync_recent_window()
        except Exception:
            logger.exception("Funding rate sync job failed")

    trading_scheduler.set_funding_sync_callback(funding_sync_callback)

    # Position reconcile callback
    async def position_reconcile_callback():
        return await batch_service.reconcile_running_positions()

    trading_scheduler.set_position_reconcile_callback(position_reconcile_callback)
    
    # 6. 启动各组件
    await phase_service.start()
    trading_scheduler.start()
    
    logger.info("All services started")
    
    yield
    
    # Shutdown
    logger.info("Shutting down application...")
    
    if trading_scheduler:
        trading_scheduler.stop()
    if phase_service:
        await phase_service.stop()
    
    logger.info("Application shutdown complete")


# Create app with lifespan
app = FastAPI(
    title="Binance Arbitrage Platform API",
    description="API for Binance arbitrage trading with funding rate and savings",
    version="1.0.0",
    lifespan=lifespan
)

# CORS configuration
allow_origins = settings.get('cors_origins', ['http://localhost:3000', 'http://localhost:8000'])

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "X-Request-ID"],
)

# Include routers
app.include_router(api_router, prefix="/api", tags=["api"])


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "name": "Binance Arbitrage Platform",
        "version": "1.0.0",
        "status": "running"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
