from fastapi import FastAPI
from api.db import repository


app = FastAPI()

@app.get("/")
async def home():
    tables = await repository.tables()
    return {"tables": tables}

if __name__ == "__main__":
    import api.initiliize
    import uvicorn    
    import asyncio

    asyncio.run(api.initiliize.insert_data())
    uvicorn.run("api.main:app", port=8000, reload=True)
