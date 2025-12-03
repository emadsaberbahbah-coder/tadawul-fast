from fastapi import FastAPI
from routes.enriched_quote import router as enriched_quote_router
from routes.argaam import router as argaam_router   # or routes_argaam, depending on file name
from routes.ai_analysis import router as ai_analysis_router

app = FastAPI(
    title="Tadawul Fast Bridge – Core Data & Analysis Engine",
    version="1.0.0",
)

app.include_router(enriched_quote_router)
app.include_router(argaam_router)
app.include_router(ai_analysis_router)  # ⬅️ NEW
