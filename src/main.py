from fastapi import FastAPI
from src.api.routers.webhook import router as webhook_router

app = FastAPI(
    title="Post Call Processing Service",
    description="Serviço centralizado EDW pós-chamada e política de retentativas multi-tenant da MindFlow",
    version="1.0.0"
)

app.include_router(webhook_router)


@app.get("/health")
def health_check():
    return {"status": "ok", "service": "post_call_processing"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.main:app", host="0.0.0.0", port=8000, reload=True)
