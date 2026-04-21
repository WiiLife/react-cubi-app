from fastapi.middleware.cors import CORSMiddleware
from api.db.repository import repository
from fastapi import FastAPI

from api.api import router as api_router


app = FastAPI()
app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"]
)

if __name__ == "__main__":
    from api.utils.logging import setup_logging
    import uvicorn    
    import asyncio

    setup_logging(level="DEBUG")

    asyncio.run(repository.insert_csv_data())
    uvicorn.run("api.main:app", port=8000, reload=True)
