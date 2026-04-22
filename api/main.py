from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from api.db.repository import repository
from fastapi import FastAPI

from api.api import router as api_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    await repository.insert_csv_data()  # at startup
    yield
    await repository.client.close() # at shutdown


app = FastAPI()
app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"]
)


if __name__ == "__main__":
    from api.utils.logging import setup_logging
    import uvicorn    
    
    setup_logging(level="DEBUG")
    uvicorn.run("api.main:app", port=8000, reload=True)
