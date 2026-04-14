from api.db.client import dbClient
from pathlib import Path


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
