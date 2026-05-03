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
from .core import PositionScheduler, CloseScheduler


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
position_scheduler: PositionScheduler | None = None
close_scheduler: CloseScheduler | None = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan handler."""
    global position_scheduler, close_scheduler
    
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
    
    # Start position scheduler
    position_scheduler = PositionScheduler()
    position_scheduler.start()
    logger.info("Position scheduler started")
    
    # Start close scheduler
    close_scheduler = CloseScheduler()
    close_scheduler.start()
    logger.info("Close scheduler started")
    
    yield
    
    # Shutdown
    logger.info("Shutting down application...")
    
    if position_scheduler:
        await position_scheduler.stop()
    if close_scheduler:
        await close_scheduler.stop()
    
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