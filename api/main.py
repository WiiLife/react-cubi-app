from api.db.repository import Repository
from fastapi import FastAPI
from pathlib import Path


app = FastAPI()
files_path = Path("./data/cubi_UDSC_01.csv")
repository = Repository(files_path)

@app.get("/")
async def home():
    tables = await repository.tables()
    return {"tables": tables}

if __name__ == "__main__":
    from api.utils.logging import setup_logging
    import uvicorn    
    import asyncio

    setup_logging(level="DEBUG")

    asyncio.run(repository.insert_data())
    uvicorn.run("api.main:app", port=8000, reload=True)
