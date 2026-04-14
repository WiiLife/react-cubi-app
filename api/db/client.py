import duckdb
import asyncio
from pathlib import Path
from asyncio import Queue
from duckdb import DuckDBPyConnection
from contextlib import asynccontextmanager


class dbClient:

    def __init__(self, file_path: Path | None = None, max_connections: int = 10, **kwargs) -> None:
        self.__pool: Queue[DuckDBPyConnection] = Queue(maxsize=max_connections)
        self.__max_connections = max_connections
        self.__db_path = file_path
        self.__kwargs = kwargs
        self.__initialized = False

    async def initialize(self):
        if self.__initialized:
            return

        for _ in range(self.__max_connections):
            conn = await asyncio.to_thread(duckdb.connect, self.__db_path or ":memory:", **self.__kwargs)
            await self.__pool.put(conn)
            
        self.__initialized = True

    async def close(self):
        if not self.__initialized:
            return

        while not self.__pool.empty():
            conn = await self.__pool.get()
            conn.close()
            
        self.__initialized = False

    @asynccontextmanager
    async def aquire(self):
        if not self.__initialized:
            raise ValueError("connection pool not initialized, call dbClient.initilize()")

        conn = await self.__pool.get()
        try:
            yield conn            
        finally:
            await self.__pool.put(conn)

    async def __aenter__(self):
        conn = self.aquire()
        yield conn
        
    async def __aexit__(self, exc_type, exc, tb):
        await self.close()


if __name__ == "__main__":
    from pathlib import Path

    client = dbClient() # in memory

    async def main():
        await client.initialize()

        # since the db is in memory when its closed the DB is removed
        async with client.aquire() as conn:
            conn.sql("CREATE TABLE test (i INTEGER)")
            conn.sql("INSERT INTO test VALUES (42)")
            conn.sql("INSERT INTO test VALUES (32)")
            conn.sql("INSERT INTO test VALUES (45)")
            conn.table("test").show()

    asyncio.run(main())
