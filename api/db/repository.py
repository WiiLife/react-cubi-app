from api.db.client import dbClient
from pathlib import Path
from typing import List
import pandas as pd
import logging


class Repository:

    def __init__(self, db_file_path: Path, file_path: Path | None = None) -> None:
        self.file_path = file_path
        self.db_file_path = db_file_path
        self.client = dbClient(db_file_path)
        self.logger = logging.getLogger((__name__).upper())
        self.tables_columns_cache = {}

    async def _get_column_names(self, table) -> list[str]:
        try:
            async with self.client.aquire() as conn:
                res = conn.sql(f"DESCRIBE {table}").fetchdf()
            return res.iloc[:, 0].to_list()
        except Exception as e:
            self.logger.warning(f"error while getting table columns: {e}")
            return []

    async def _check_tables_exists(self, tables: list[str]):
        if not self.tables_columns_cache:
            for existing_table in await self.tables():
                self.tables_columns_cache[existing_table] = []

        for table in tables:
            if table not in self.tables_columns_cache:
                self.logger.info("Selected table not in DB")
                raise ValueError(f"table: {table} doesnt exist")
        return True

    async def _check_columns_exist(self, table: str, columns: list[str]):
        if not await self._check_tables_exists([table]):
            return 

        if not self.tables_columns_cache[table]:
            self.tables_columns_cache[table] = await self._get_column_names(table)

        for column in columns:    
            if column not in self.tables_columns_cache[table]:
                raise ValueError(f"column: {column} doesnt exist in table: {table}")
        return True

    async def tables(self) -> List[str]:
        if not self.db_file_path.exists():
            raise ValueError("db file path doesnt exist, create it")

        async with self.client.aquire() as conn:
            self.logger.debug("getting tables...")
            res = conn.sql("SHOW TABLES").fetchall()
        table_names: list[str] = [row[0] for row in res]

        if not self.tables_columns_cache:
            for table in table_names:
                self.tables_columns_cache[table] = await self._get_column_names(table)
        return table_names

    async def insert_csv_data(self):
        if not self.file_path:
            raise ValueError("no files path provided for inserting data")

        table_name = self.file_path.stem
        if table_name not in await self.tables():
            async with self.client.aquire_write() as conn:
                self.logger.debug("inserting data...")
                conn.sql(
                    f"""
                    CREATE TABLE {table_name} AS
                    SELECT *
                    FROM read_csv('{str(self.file_path)}');
                    """
                )
            self.logger.debug(f"inserted {table_name} in db")
        else:
            self.logger.debug(f"table: {table_name} already present, skippping...")

    async def get_table_columns(self, table: str):
        await self._check_tables_exists([table])
        return await self._get_column_names(table)

    async def get_unique_values(self, table, column):
        pass

    async def select(self, table: str, columns: List[str]) -> pd.DataFrame:
        await self._check_columns_exist(table, columns)

        exc = f"""
              SELECT {', '.join(f'"{col}"' for col in columns)} FROM "{table}";
              """

        print(exc)

        async with self.client.aquire() as conn:
            res = conn.sql(exc).fetchdf()
        return res

if __name__ == "__main__":
    import asyncio

    repo = Repository(Path("./api/db/db_cubi_ustat.ddb"))
    async def main():
        res = await repo.select("cubi_POL_01", ["anno", "cont_descrizione", "valore"])
        return res

    res = asyncio.run(main())
    print(res)
