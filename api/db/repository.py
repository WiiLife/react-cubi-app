from api.db.client import dbClient
from pathlib import Path
from typing import List
import logging


class Repository:

    def __init__(
        self, file_path: Path, db_file_path: Path = Path("./api/db/db_cubi_ustat.ddb")
    ) -> None:
        self.file_path = file_path
        self.db_file_path = db_file_path
        self.client = dbClient(db_file_path)
        self.logger = logging.getLogger((__name__).upper())

    async def tables(self) -> List[str]:

        if not self.db_file_path.exists():
            return []

        async with self.client.aquire() as conn:
            logging.debug("getting tables...")
            res = conn.sql("SHOW TABLES").fetchall()
        table_names: list[str] = [row[0] for row in res]
        return table_names

    async def insert_data(self):
        table_name = self.file_path.stem
        if table_name not in await self.tables():
            async with self.client.aquire_write() as conn:
                logging.debug("inserting data...")
                conn.sql(
                    f"""
                    CREATE TABLE {table_name} AS
                    SELECT *
                    FROM read_csv('{str(self.file_path)}');
                    """
                )
            logging.debug(f"inserted {table_name} in db")
        else:
            logging.debug(f"table: {table_name} already present, skippping...")
