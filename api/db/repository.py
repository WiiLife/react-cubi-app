from api.utils.logging import setup_logging
from api.db.client import dbClient
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
        self.tables_columns_cache: dict[str, pd.DataFrame] = {}

    def _format_list(self, values: List[str]) -> str:
        return ', '.join(f'\"{val}\"' for val in values)

    def _asign_type(self, type_str: str):
        try:
            return SQLTypes(type_str)
        except Exception:
            return SQLTypes.OTHER

    async def _check_column_type(self, table: str, column: str) -> SQLTypes:
        rows = self.tables_columns_cache[table]
        col_type = str(rows[rows.name == column])
        return self._asign_type(col_type)

    async def _reset_cache(self):
        self.tables_columns_cache = {}
        self.logger.debug("resetting cache ...")

        async with self.client.aquire() as conn:
            self.logger.debug("getting tables...")
            res = conn.sql("SHOW TABLES").fetchall()
        table_names: list[str] = [row[0] for row in res]

        tasks = []
        for table in table_names:
            tasks.append(self._get_column_types(table))
        columns_type = await asyncio.gather(*tasks)
        self.tables_columns_cache = dict(zip(table_names, columns_type))
        self.logger.debug(f"cached {len(self.tables_columns_cache)} tables and column names")

    async def _get_column_types(self, table) -> pd.DataFrame:
        query = f"""
SELECT name, type 
FROM pragma_table_info('{table}');   
"""

        try:
            async with self.client.aquire() as conn:
                res = await asyncio.to_thread(lambda: conn.sql(query).fetchdf())
                self.logger.debug(f"got columns for table: {table}")
            return res
        except Exception as e:
            raise ValueError(f"error while getting table columns: {e}")

    async def _get_unique_values(self, table: str, column: str) -> List[str]:
        async with self.client.aquire() as conn:
            res = await asyncio.to_thread(
                lambda: conn.sql(f"SELECT DISTINCT {column} FROM {table};").fetchall()
            )
        return [col[0] for col in res]

    async def _check_table_exists(self, table: str):
        if not self.tables_columns_cache:
            await self._reset_cache()
            if table not in self.tables_columns_cache:
                raise ValueError(f"table {table} doesn't exist in DB")

        if table not in self.tables_columns_cache:
            await self._reset_cache()
            if table not in self.tables_columns_cache:
                raise ValueError(f"table {table} doesn't exist in DB")

        return True

    async def _check_columns_exist(self, table: str, columns: list[str]):
        await self._check_table_exists(table)

        rows = self.tables_columns_cache[table]
        cached_columns = rows.iloc[:, 0].to_list()
        for column in columns:
            if column not in cached_columns:
                raise ValueError(f"column: {column} doesnt exist in table: {table}")
        return True

    async def tables(self) -> List[str]:
        if not self.db_file_path.exists():
            raise ValueError("db file path doesnt exist, create it")

        if not self.tables_columns_cache:
            await self._reset_cache()
            return list(self.tables_columns_cache.keys())

        return list(self.tables_columns_cache.keys())

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
        rows = self.tables_columns_cache[table]

        selected_cols = []
        for row in rows.itertuples(index=False):
            col_type = self._asign_type(str(row.type))
            col_name = str(row.name)
            if not col_type in NUMERIC_TYPES:
                selected_cols.append(col_name)
        return selected_cols

    async def get_table_columns_and_unique_values(self, table: str) -> dict[str, list[str]]:
        await self._check_table_exists(table)
        rows = self.tables_columns_cache[table]

        res = {}
        for row in rows.itertuples(index=False):
            col_type = self._asign_type(str(row.type))
            col_name = str(row.name)
            if col_type in NUMERIC_TYPES:
                continue

            unique_values = await self._get_unique_values(table, col_name)
            res[col_name] = unique_values
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
            all_columns = self.tables_columns_cache[table].iloc[:, 0].to_list()
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
        res = await self._check_table_exists(table)

        async with self.client.aquire() as conn:
            res = await asyncio.to_thread(lambda: conn.sql(f"SELECT * FROM {table}").fetchdf())
        return res


# NOTES
# cache should be never reset, only if the write DB connection is closed
# since new tables cannot be found by the user since a list is given, in theory the cache should never be reset
# but just in case it does reset if a table is not found (hence updating the cache with new tables)

# logging for the repository
setup_logging(level="DEBUG")

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
