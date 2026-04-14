import duckdb
import asyncio
import logging
from pathlib import Path
from asyncio import Queue
from duckdb import DuckDBPyConnection
from contextlib import asynccontextmanager


class dbClient:

    def __init__(self, db_file_name: Path | None = None, max_connections: int = 10, **kwargs) -> None:
        self.__pool: Queue[DuckDBPyConnection] = Queue(maxsize=max_connections)
        self.__max_connections = max_connections
        self.__db_file_name = db_file_name
        self.__db_file_lock = asyncio.Lock()
        self.__initialized = False
        self.__kwargs = kwargs

        self.__logger = logging.getLogger((__name__).upper())

    async def initialize(self):
        if self.__initialized:
            self.__logger.debug("readonly connections already initilized")
            return

        for _ in range(self.__max_connections):
            conn = await asyncio.to_thread(
                    duckdb.connect, 
                    str(self.__db_file_name) if self.__db_file_name else ":memory:",    # db location
                    True,                                                               # read-only
                    **self.__kwargs                                                     # config
                )
            await self.__pool.put(conn)

        self.__initialized = True
        self.__logger.debug(f"opened {self.__max_connections} readonly connections")

    async def close(self):
        if not self.__initialized:
            self.__logger.debug("readonly never initilized")
            return

        while not self.__pool.empty():
            conn = await self.__pool.get()
            conn.close()

        self.__initialized = False
        self.__logger.debug("readonly connections closed")

    @asynccontextmanager
    async def aquire(self):

        if not self.__initialized:
            async with self.__db_file_lock:
                await self.initialize()

        conn = await self.__pool.get()
        self.__logger.debug("got readonly connection")
        try:
            yield conn            
        finally:
            await self.__pool.put(conn)
            self.__logger.debug("released readonly connection")

    @asynccontextmanager
    async def aquire_write(self):

        if self.__initialized:
            await self.close()

        async with self.__db_file_lock:
            conn = duckdb.connect(
                str(self.__db_file_name) if self.__db_file_name else ":memory:",
                False,
                **self.__kwargs
            )

            try:
                self.__logger.debug("openened main write connection")
                yield conn
            finally:
                conn.close()
                self.__logger.debug("closed main write connection")


if __name__ == "__main__":
    from api.utils.logging import setup_logging
    from pathlib import Path

    setup_logging(level="DEBUG")

    client = dbClient() # in memory
    async def main():
        async with client.aquire_write() as pool:
            pool.sql("CREATE TABLE test (i INTEGER)")
            pool.sql("INSERT INTO test VALUES (42)")
            pool.sql("INSERT INTO test VALUES (32)")
            pool.sql("INSERT INTO test VALUES (45)")
            pool.table("test").show()

    asyncio.run(main())
