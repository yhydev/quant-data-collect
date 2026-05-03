"""
FastAPI application for Binance Arbitrage Platform.
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .api.routes import router as api_router
from .database import init_db
from .core import PositionScheduler, CloseScheduler


app = FastAPI(
    title="Binance Arbitrage Platform API",
    description="API for Binance arbitrage trading with funding rate and savings",
    version="1.0.0"
)

# Enable CORS - restrict to known origins in production
# In production, replace with actual frontend domain
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:8000"],  # Development origins
    # For production, use: allow_origins=["https://your-domain.com"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "X-Request-ID"],
)

# Include routers
app.include_router(api_router, prefix="/api", tags=["api"])


# Scheduler instances
position_scheduler = None
close_scheduler = None


@app.on_event("startup")
async def startup_event():
    """Initialize on startup."""
    # Initialize database
    try:
        init_db()
        print("Database initialized")
    except Exception as e:
        print(f"Database init warning: {e}")
    
    # Start position scheduler
    global position_scheduler
    position_scheduler = PositionScheduler()
    position_scheduler.start()
    
    # Start close scheduler
    global close_scheduler
    close_scheduler = CloseScheduler()
    close_scheduler.start()


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    global position_scheduler, close_scheduler
    if position_scheduler:
        position_scheduler.stop()
    if close_scheduler:
        close_scheduler.stop()


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