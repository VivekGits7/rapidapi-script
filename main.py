"""FastAPI app entry point. Wraps the dumper in a control surface.

Run with:
    guv main:app --reload --host 0.0.0.0 --port 8000

Or use the CLI directly without bringing up FastAPI:
    guv -m dumper.cli run
"""

from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

from config import settings
from error import setup_error_handlers
from limiter import limiter
from logger import get_logger, setup_logging
from middleware.request_logging import RequestLoggingMiddleware, SecurityHeadersMiddleware
from routers.dump import router as dump_router
from services.db import close_db_pool, create_db_pool, execute_command

load_dotenv()
setup_logging()
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage DB pool over the app's lifetime."""
    await create_db_pool()
    logger.info("Lifespan: DB pool ready")
    # Reconcile orphans: a fresh process can have NO live dump task, so any job
    # left 'running' belongs to a dead worker (killed / crashed / reloaded). Mark
    # it 'paused' so /status is honest and the job is resumable.
    reconciled = await execute_command(
        """
        UPDATE rapid_api_dump_jobs
        SET status = 'paused',
            error_message = COALESCE(error_message, 'orphaned — process restarted'),
            stop_requested = FALSE, updated_at = NOW()
        WHERE status = 'running'
        """
    )
    if reconciled and reconciled != "UPDATE 0":
        logger.warning(f"Lifespan: reconciled orphaned running job(s) → paused ({reconciled})")
    yield
    await close_db_pool()
    logger.info("Lifespan: DB pool closed")


app = FastAPI(
    title=settings.APP_NAME,
    description=(
        "Control surface for the RapidAPI Auto Parts Catalog dumper.\n\n"
        "Long-running dumps execute as background asyncio tasks. The actual crawl logic "
        "lives in `dumper/` and is also runnable as a CLI (`guv -m dumper.cli run`)."
    ),
    version="0.1.0",
    lifespan=lifespan,
    docs_url="/docs" if not settings.is_production else None,
    redoc_url="/redoc" if not settings.is_production else None,
    openapi_url="/openapi.json" if not settings.is_production else None,
)

# Rate limiting wired in before custom middleware (LIFO order: last added is outermost)
app.state.limiter = limiter

setup_error_handlers(app)

app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# LIFO: registered last = runs first. CORS outermost, then logging, then security, then rate-limit.
app.add_middleware(SlowAPIMiddleware)
app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(RequestLoggingMiddleware)

if settings.is_development:
    allowed_origins = ["*"]
else:
    allowed_origins = [settings.FRONTEND_URL]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=allowed_origins != ["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(dump_router)


# ==================== HEALTH ====================
@app.get("/", tags=["Health"])
async def root():
    return {
        "message": f"Welcome to {settings.APP_NAME}",
        "version": "0.1.0",
        "docs": "/docs",
        "dump_endpoints": "/api/dump/*",
    }


@app.get("/health", tags=["Health"])
async def health_check():
    return {"status": "healthy", "service": "rapidapi-autoparts-dumper"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.is_development,
    )
