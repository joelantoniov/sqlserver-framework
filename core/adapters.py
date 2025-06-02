#! /usr/bin/env python
# -*- coding: utf-8 -*-

import abc
import asyncio
import logging
import time
from typing import List, Dict, Any, Optional, Tuple, Sequence

import aioodbc 

from core.models import DBConnectionConfig, QueryExecutionMetric 

logger = logging.getLogger(__name__)

class DatabaseAdapter(abc.ABC):
    """Abstract base class for database adapters."""
    def __init__(self, config: DBConnectionConfig):
        self.config = config
        self.pool: Optional[aioodbc.Pool] = None 

    @abc.abstractmethod
    async def connect(self) -> None:
        """Establish database connection pool."""
        pass

    @abc.abstractmethod
    async def disconnect(self) -> None:
        """Close database connection pool."""
        pass

    @abc.abstractmethod
    async def execute_query(self, query: str, params: Optional[Sequence[Any]] = None, fetch_results: bool = False) -> Any:
        """Execute a single SQL query."""
        pass

    @abc.abstractmethod
    async def execute_script(self, script: str) -> bool:
        """Execute a multi-statement SQL script."""
        pass

    @abc.abstractmethod
    async def get_table_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """Get column names and types for a table."""
        pass

    @abc.abstractmethod
    async def get_column_min_max(self, table_name: str, column_name: str) -> Tuple[Optional[Any], Optional[Any]]:
        """Get min and max values for a column."""
        pass

    @abc.abstractmethod
    async def get_column_sample(self, table_name: str, column_name: str, sample_size: int = 100) -> List[Any]:
        """Get a sample of values from a column."""
        pass

    @abc.abstractmethod
    async def check_table_exists(self, table_name: str, schema: str = 'dbo') -> bool:
        """Check if a table exists."""
        pass

    @abc.abstractmethod
    async def check_index_exists(self, table_name: str, index_name: str, schema: str = 'dbo') -> bool:
        """Check if an index exists."""
        pass

    @abc.abstractmethod
    async def check_foreign_key_exists(self, table_name: str, fk_name: str, schema: str = 'dbo') -> bool:
        """Check if a foreign key exists."""
        pass


class SQLServerAdapter(DatabaseAdapter):
    """SQL Server specific database adapter using aioodbc."""

    def _build_conn_string(self) -> str:
        conn_parts = [
            f"Driver={self.config.driver}",
            f"Server={self.config.server}",
            f"Database={self.config.database}",
        ]
        if self.config.username and self.config.password:
            conn_parts.append(f"Uid={self.config.username}")
            conn_parts.append(f"Pwd={self.config.password}")
        else: # Windows Authentication
            conn_parts.append("Trusted_Connection=yes")

        conn_parts.append(f"Encrypt={self.config.encrypt}")
        conn_parts.append(f"TrustServerCertificate={self.config.trust_server_certificate}")
        # Add other necessary parameters for aioodbc if needed
        return ";".join(conn_parts)

    async def connect(self, retries: int = 3, delay: int = 5) -> None:
        conn_str = self._build_conn_string()
        for attempt in range(retries):
            try:
                # autocommit=True is often simpler for DDL, but be mindful of transactional needs
                self.pool = await aioodbc.create_pool(dsn=conn_str, autocommit=True, loop=asyncio.get_event_loop())
                async with self.pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("SELECT 1")
                        await cur.fetchone()
                logger.info(f"Successfully connected to SQL Server: {self.config.database} on {self.config.server}")
                return
            except aioodbc.Error as ex:
                logger.error(f"Error connecting to SQL Server (Attempt {attempt + 1}/{retries}): {ex}")
                if attempt < retries - 1:
                    logger.info(f"Retrying in {delay} seconds...")
                    await asyncio.sleep(delay)
                else:
                    logger.critical("Failed to connect to SQL Server after several retries.")
                    raise

    async def disconnect(self) -> None:
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            logger.info("SQL Server connection pool closed.")
            self.pool = None

    async def execute_query(self, query: str, params: Optional[Sequence[Any]] = None, fetch_results: bool = False) -> Any:
        if not self.pool:
            logger.error("Not connected to database (pool is None). Cannot execute query.")
            raise ConnectionError("Database connection pool is not initialized.")

        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(query, params or ())

                    if fetch_results:
                        columns = [column[0] for column in cur.description] if cur.description else []
                        rows_tuples = await cur.fetchall()
                        return [dict(zip(columns, row)) for row in rows_tuples]
                    else: # For DDL, INSERT, UPDATE, DELETE
                        if query.strip().upper().startswith(("INSERT", "UPDATE", "DELETE")):
                            logger.debug(f"Query executed. Rows affected (approx): {cur.rowcount}")
                        return cur.rowcount
        except aioodbc.Error as e:
            logger.error(f"Error executing query: {query} \nParams: {params} \nError: {e}")
            return None 

    async def execute_script(self, script: str) -> bool:
        if not self.pool:
            logger.error("Not connected to database (pool is None). Cannot execute script.")
            return False
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(script)
            logger.info("SQL script executed successfully.")
            return True
        except aioodbc.Error as e:
            logger.error(f"Error executing SQL script: {e}\nScript: {script[:500]}...")
            return False

    async def get_table_columns(self, table_name: str, schema: str = 'dbo') -> List[Dict[str, Any]]:
        query = """
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION;
        """
        return await self.execute_query(query, (schema, table_name), fetch_results=True) or []

    async def get_column_min_max(self, table_name: str, column_name: str) -> Tuple[Optional[Any], Optional[Any]]:
        query = f"SELECT MIN([{column_name}]), MAX([{column_name}]) FROM dbo.[{table_name}];"
        results = await self.execute_query(query, fetch_results=True)
        if results and len(results) == 1:
            row = results[0]
            if row: # Check if dict is not empty
                values = list(row.values())
                if len(values) == 2:
                    return values[0], values[1]
        return None, None

    async def get_column_sample(self, table_name: str, column_name: str, sample_size: int = 100) -> List[Any]:
        query = f"SELECT TOP ({int(sample_size)}) [{column_name}] FROM dbo.[{table_name}] ORDER BY NEWID();"
        results = await self.execute_query(query, fetch_results=True)
        if results:
            return [row[column_name] for row in results if column_name in row]
        return []

    async def check_table_exists(self, table_name: str, schema: str = 'dbo') -> bool:
        query = "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
        result = await self.execute_query(query, (schema, table_name), fetch_results=True)
        return bool(result)

    async def check_index_exists(self, table_name: str, index_name: str, schema: str = 'dbo') -> bool:
        query = """
        SELECT 1 FROM sys.indexes i
        JOIN sys.objects o ON i.object_id = o.object_id
        JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE o.name = ? AND i.name = ? AND s.name = ?
        """
        result = await self.execute_query(query, (table_name, index_name, schema), fetch_results=True)
        return bool(result)

    async def check_foreign_key_exists(self, table_name: str, fk_name: str, schema: str = 'dbo') -> bool:
        query = """
        SELECT 1 FROM sys.foreign_keys fk
        JOIN sys.objects o ON fk.parent_object_id = o.object_id
        JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE o.name = ? AND fk.name = ? AND s.name = ?
        """
        result = await self.execute_query(query, (table_name, fk_name, schema), fetch_results=True)
        return bool(result)
