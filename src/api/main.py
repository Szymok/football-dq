"""FastAPI Application Entrypoint."""

import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.api.instrument import init_sentry
from src.api.utils.errors import BaseAPIException, global_exception_handler, base_api_exception_handler
from src.api.routes import quality_routes, reconciliation_routes, golden_routes

# Initialize Sentry before app creation
init_sentry()

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Football DQ API",
    description="Data Quality Pipeline Management API",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register exception handlers
app.add_exception_handler(Exception, global_exception_handler)
app.add_exception_handler(BaseAPIException, base_api_exception_handler)

# Register API Routers
app.include_router(quality_routes.router, prefix="/api/v1/quality", tags=["Quality"])
app.include_router(reconciliation_routes.router, prefix="/api/v1/reconciliation", tags=["Reconciliation"])
app.include_router(golden_routes.router, prefix="/api/v1/golden_record", tags=["Golden Record"])

@app.get("/health")

def health_check():
    """Basic health check endpoint."""
    return {"status": "ok"}
