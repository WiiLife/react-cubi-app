from api.main import client
from typing import List


async def tables() -> List[str]:    
    async with client.aquire() as conn:
        res = conn.sql("SHOW TABLES").fetchall()
    table_names: list[str] = [row[0] for row in res]
    return table_names
