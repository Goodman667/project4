from fastapi import FastAPI

from app.api import router
from app.ui import router as ui_router


app = FastAPI(
    title="Lightweight Message Broker",
    version="0.1.0",
    description="A minimal course project skeleton for a topic-based message broker.",
    openapi_url="/api/openapi.json",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
)

app.include_router(ui_router)
app.include_router(router)
