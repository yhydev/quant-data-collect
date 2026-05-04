"""
FastAPI application for Binance Arbitrage Platform.
"""
import os
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from typing import AsyncGenerator

from .api.routes import router as api_router
from .database import init_db_async
from .core import WakeScheduler, ExecuteScheduler


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


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan handler."""
    global wake_scheduler, execute_scheduler
    
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
    
    # Start wake scheduler (统一唤醒)
    wake_scheduler = WakeScheduler()
    wake_scheduler.start()
    logger.info("Wake scheduler started")
    
    # Start execute scheduler (基于状态机路由)
    execute_scheduler = ExecuteScheduler()
    execute_scheduler.start()
    logger.info("Execute scheduler started with PhaseService")
    
    yield
    
    # Shutdown
    logger.info("Shutting down application...")
    
    if execute_scheduler:
        await execute_scheduler.stop()
    if wake_scheduler:
        wake_scheduler.stop()
    
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