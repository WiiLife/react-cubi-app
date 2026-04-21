from api.db.client import dbClient
from pydantic import BaseModel
from pathlib import Path
from typing import List
from enum import Enum
import pandas as pd
import logging
import asyncio


class SQLOperation(Enum):
    SUM = "SUM"
    COUNT = "COUNT"
    AVG = "AVG"

class SQLTypes(Enum):
    INTEGER = "INTEGER"
    DOUBLE = "DOUBLE"
    VARCHAR = "VARCHAR"
    OTHER = "OTHER"

NUMERIC_TYPES = (SQLTypes.INTEGER, SQLTypes.DOUBLE, SQLTypes.VARCHAR)

class Repository:

    def __init__(self, db_file_path: Path, file_path: Path | None = None) -> None:
        self.file_path = file_path
        self.db_file_path = db_file_path
        self.client = dbClient(db_file_path)
        self.logger = logging.getLogger((__name__).upper())
        self.tables_columns_cache = {}

    def _format_list(self, values: List[str]) -> str:
        return ', '.join(f'\"{val}\"' for val in values)

    async def _check_column_type(self, table: str, column: str) -> SQLTypes:
        query = f"""
SELECT data_type
FROM information_schema.columns
WHERE table_name = '{table}' 
AND column_name = '{column}';
"""
        async with self.client.aquire() as conn:
            res = conn.sql(query).fetchall()

        try:
            return SQLTypes(res[0][0])
        except Exception:
            return SQLTypes.OTHER

    async def _get_column_names(self, table) -> list[str]:
        try:
            async with self.client.aquire() as conn:
                res = await asyncio.to_thread(lambda: conn.sql(f"DESCRIBE {table}").fetchdf())
                self.logger.debug(f"got columns for table: {table}")
            return res.iloc[:, 0].to_list()
        except Exception as e:
            self.logger.warning(f"error while getting table columns: {e}")
            return []

    async def _get_unique_values(self, table: str, column: str) -> List[str]:
        async with self.client.aquire() as conn:
            res = await asyncio.to_thread(
                lambda: conn.sql(f"SELECT DISTINCT {column} FROM {table};").fetchall()
            )
        return [col[0] for col in res]

    async def _check_table_exists(self, table: str):
        if not self.tables_columns_cache:
            await self.tables()

        if table not in self.tables_columns_cache:
            self.tables_columns_cache[table] = await self._get_column_names(table)
        return True

    async def _check_columns_exist(self, table: str, columns: list[str]):
        await self._check_table_exists(table)

        if not self.tables_columns_cache[table]:
            self.tables_columns_cache[table] = await self._get_column_names(table)

        cached_columns = set(self.tables_columns_cache[table])
        for column in columns:
            if column not in cached_columns:
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
            tasks = []
            for table in table_names:
                tasks.append(self._get_column_names(table)) 
            columns_list = await asyncio.gather(*tasks)
            self.tables_columns_cache = dict(zip(table_names, columns_list))
            self.logger.debug(f"cached {len(self.tables_columns_cache)} tables and column names")

        return table_names

    async def insert_csv_data(self):
        if not self.file_path:
            raise ValueError("no files path provided for inserting data")

        table_name = self.file_path.stem
        if table_name not in await self.tables():
            async with self.client.aquire_write() as conn:
                self.logger.debug("inserting data...")
                query = f"""
CREATE TABLE {table_name} AS
SELECT *
FROM read_csv('{str(self.file_path)}');
"""
                await asyncio.to_thread(lambda: conn.sql(query))
            self.logger.debug(f"inserted {table_name} in db")
        else:
            self.logger.debug(f"table: {table_name} already present, skippping...")

    async def get_table_columns(self, table: str) -> list[str]:
        await self._check_table_exists(table)
        selected_cols = []
        for col in await self._get_column_names(table):
            if not await self._check_column_type(table, col) in NUMERIC_TYPES:
                selected_cols.append(col)
        return selected_cols

    async def get_table_columns_and_unique_values(self, table: str) -> dict[str, list[str]]:
        await self._check_table_exists(table)

        if not self.tables_columns_cache[table]:
            columns = await self._get_column_names(table)
        else:
            columns = self.tables_columns_cache[table]

        res = {}
        for col in columns:

            col_type = await self._check_column_type(table, col)
            if col_type in NUMERIC_TYPES:
                continue

            unique_values = await self._get_unique_values(table, col)            
            res[col] = unique_values
        return res

    async def select(self, table: str, columns: List[str]) -> pd.DataFrame:
        await self._check_columns_exist(table, columns)

        query = f"SELECT {self._format_list(columns)} FROM \"{table}\";"

        async with self.client.aquire() as conn:
            res = await asyncio.to_thread(lambda: conn.sql(query).fetchdf())
        return res

    async def pivot(
        self,
        table: str,
        columns: List[str],
        operation_column: str = "valore",
        group_by_columns: List[str] | None = None,
        operation: SQLOperation = SQLOperation.SUM,
    ) -> pd.DataFrame:
        await self._check_columns_exist(table, columns)
        operation_column_type = await self._check_column_type(table, operation_column)

        if not operation_column_type in (SQLTypes.INTEGER, SQLTypes.DOUBLE):
            raise ValueError(f"operation column ({operation_column}) needs to be integer or doubble. Currently: {operation_column_type}")

        if group_by_columns:
            await self._check_columns_exist(table, group_by_columns)

        if not group_by_columns:

            if not self.tables_columns_cache[table]:
                all_columns = await self._get_column_names(table)
            else:
                all_columns = self.tables_columns_cache[table]

            group_by_columns = list(set(all_columns) - set(columns) - {operation_column})

        query = f"""
PIVOT {table} 
ON {self._format_list(columns)} 
USING {operation.value}({operation_column}) 
GROUP BY {self._format_list(group_by_columns)};
"""

        async with self.client.aquire() as conn:
            res = await asyncio.to_thread(lambda: conn.sql(query).fetchdf())
        return res

    async def get_table(self, table: str) -> pd.DataFrame:
        await self._check_table_exists(table)

        async with self.client.aquire() as conn:
            res = await asyncio.to_thread(lambda: conn.sql(f"SELECT * FROM {table}").fetchdf())
        return res


# global repository instance for all services
files_path = Path("./data/cubi_UDSC_01.csv")
db_file_path = Path("./api/db/db_cubi_ustat.ddb")
repository = Repository(db_file_path, files_path)

if __name__ == "__main__":
    import asyncio
    from api.utils.logging import setup_logging

    setup_logging(level="DEBUG")

    repo = Repository(Path("./api/db/db_cubi_ustat.ddb"))
    async def main():
        # res = await repo.get_unique_values("cubi_POL_01", "comune_2011")
        res = await repo.pivot("cubi_RIFOS_01", ["nazionalità", "Stato_att"])
        return res

    res = asyncio.run(main())
    print(res)
