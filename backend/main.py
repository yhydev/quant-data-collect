"""
FastAPI application for Binance Arbitrage Platform.
"""
import os
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
from events.phase_service import PhaseService, PhaseServiceConfig


# Global scheduler instances
trading_scheduler: TradingScheduler | None = None
batch_service: BatchExecutionService | None = None
phase_service: PhaseService | None = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan handler."""
    global wake_scheduler, execute_scheduler, batch_service, phase_service
    
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
    batch_service = BatchExecutionService(
        collector_type='binance',
        trader_type='binance',
        order_plugin_name='futures_first'
    )
    
    # 2. 创建events层（需要batch_service依赖）
    phase_service = PhaseService(batch_service)
    
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
    
    # 6. 启动各组件
    await phase_service.start()
    await batch_service.order_polling.start()
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

# CORS configuration - use environment variable for allowed origins
cors_origins = os.getenv('CORS_ORIGINS', 'http://localhost:3000,http://localhost:8000')
allow_origins = [origin.strip() for origin in cors_origins.split(',')]

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