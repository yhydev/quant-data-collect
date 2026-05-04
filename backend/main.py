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
from scheduler.core import WakeScheduler, ExecuteScheduler
from services.batch_service import BatchExecutionService
from events.phase_service import PhaseService, PhaseServiceConfig


# Configure logging
def setup_logging():
    """Setup logging configuration."""
    log_level = os.getenv('LOG_LEVEL', 'INFO')
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


# Global scheduler instances
wake_scheduler: WakeScheduler | None = None
execute_scheduler: ExecuteScheduler | None = None
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
    
    # 1. 创建services层
    batch_service = BatchExecutionService(
        collector=None,  # 由BatchExecutionService内部创建
        trader=None,
        order_plugin=None,
        order_watcher=None
    )
    
    # 2. 创建events层
    phase_service = PhaseService(PhaseServiceConfig())
    
    # 3. 创建scheduler层（纯触发）
    wake_scheduler = WakeScheduler()
    execute_scheduler = ExecuteScheduler()
    
    # 4. 注入依赖到scheduler（通过callback解耦）
    # WakeScheduler callback (同步函数）
    wake_scheduler.set_callback(lambda: batch_service.wake_pending_batches())
    
    # ExecuteScheduler callback (异步函数需要包装）
    async def execute_callback():
        await batch_service.trigger_all_running()
    execute_scheduler.set_callback(execute_callback)
    
    # 5. 启动各组件
    await phase_service.start()
    wake_scheduler.start()
    execute_scheduler.start()
    
    logger.info("All services started")
    
    yield
    
    # Shutdown
    logger.info("Shutting down application...")
    
    if execute_scheduler:
        await execute_scheduler.stop()
    if wake_scheduler:
        wake_scheduler.stop()
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