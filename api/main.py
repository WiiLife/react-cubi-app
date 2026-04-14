from pathlib import Path
from fastapi import FastAPI
from api.db import repository
from api.db.client import dbClient
import asyncio


app = FastAPI()
db_file_path = Path("./cubi_UDSC_01_csv/cubi_UDSC_01.csv")
client = dbClient(db_file_path)

async def insert_data():
    await client.initialize()
    async with client.aquire() as conn:
        conn.sql(
        f"""
        CREATE TABLE my_table AS
        SELECT *
        FROM read_csv_auto({str(db_file_path)});
        """
        )

asyncio.run(insert_data())

@app.get("/")
async def home():
    tables = await repository.tables()
    return {"tables": tables}

if __name__ == "__main__":
    import uvicorn    
    uvicorn.run("api.main:app", port=8000, reload=True)
