#! /usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import random
from datetime import datetime, date, timedelta
from typing import List, Any, Dict, Optional from faker import Faker

from core.models import SchemaConfig, TableSchema, ColumnDefinition
from core.adapters import DatabaseAdapter
from core.schema_manager import SchemaManager 

logger = logging.getLogger(__name__)

class DataGenerator:
    def __init__(self, db_adapter: DatabaseAdapter, schema_config: SchemaConfig,
                 schema_manager: SchemaManager, batch_size: int = 1000):
        self.db_adapter = db_adapter
        self.schema_config = schema_config
        self.schema_manager = schema_manager 
        self.fake = Faker()
        self.batch_size = batch_size

    def _get_faker_method(self, generator_name: str, params: Optional[Dict[str, Any]] = None):
        params = params or {}
        try:
            method = getattr(self.fake, generator_name)
            if generator_name in ["random_int", "random_number", "random_element", "random_elements", "pydecimal"]:
                return lambda: method(**params)
            return method
        except AttributeError:
            logger.warning(f"Faker method '{generator_name}' not found. Using default string.")
            return lambda: "DefaultString"
        except Exception as e:
            logger.error(f"Error getting faker method {generator_name} with params {params}: {e}")
            return lambda: "ErrorGenString"

    async def _generate_row_data(self, table_cfg: TableSchema) -> List[Any]:
        row_data: List[Any] = []
        for col_def in table_cfg.columns:
            if col_def.identity:
                continue 

            if col_def.foreign_key:
                fk_conf = col_def.foreign_key
                ref_table_name = fk_conf['table']
                ref_table_meta = self.schema_manager.tables_metadata.get(ref_table_name)

                if ref_table_meta and ref_table_meta.get('min_id') is not None and ref_table_meta.get('max_id') is not None:
                    min_id, max_id = ref_table_meta['min_id'], ref_table_meta['max_id']
                    try:
                        if min_id == max_id: 
                            fk_value = min_id
                        else: 
                            fk_value = random.randint(min_id, max_id)
                        row_data.append(fk_value)
                    except ValueError: 
                        logger.warning(f"Cannot generate FK for {col_def.name} referencing {ref_table_name}: min_id={min_id}, max_id={max_id}. Using NULL.")
                        row_data.append(None if col_def.nullable else 0) 
                else:
                    logger.warning(f"Ref table {ref_table_name} for FK {col_def.name} has no data/PK metadata. Using NULL.")
                    row_data.append(None if col_def.nullable else 0) 
            elif col_def.generator:
                faker_method = self._get_faker_method(col_def.generator, col_def.params)
                try:
                    value = faker_method()
                    col_type_upper = col_def.type.upper()
                    if col_type_upper == 'INT' and not isinstance(value, int): value = int(value)
                    elif col_type_upper == 'BIT' and not isinstance(value, bool): value = bool(value)
                    elif 'DECIMAL' in col_type_upper and not isinstance(value, (int, float, type(self.fake.pydecimal()))):
                        value = self.fake.pydecimal(left_digits=col_def.precision - col_def.scale if col_def.precision and col_def.scale else 5,
                                                   right_digits=col_def.scale if col_def.scale else 2, positive=True)
                    elif ('DATE' in col_type_upper or 'TIME' in col_type_upper) and isinstance(value, (datetime, date)):
                        pass # Already correct type
                    elif ('DATE' in col_type_upper or 'TIME' in col_type_upper) and isinstance(value, str):
                         try: value = datetime.fromisoformat(value.replace('Z', '+00:00'))
                         except ValueError: value = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')

                    row_data.append(value)
                except Exception as e:
                    logger.error(f"Error generating data for column {col_def.name} using {col_def.generator}: {e}")
                    row_data.append(None if col_def.nullable else "ErrorValue")
            else:
                row_data.append(None if col_def.nullable else "DefaultValue") 
        return row_data

    async def generate_data_for_table(self, table_cfg: TableSchema, schema: str = 'dbo') -> None:
        if table_cfg.row_count == 0:
            logger.info(f"Skipping data generation for table {schema}.{table_cfg.name} (row_count is 0).")
            return

        logger.info(f"Starting data generation for table {schema}.{table_cfg.name}, target rows: {table_cfg.row_count}")

        # Check current row count (optional, can make generation slower)
        # count_query = f"SELECT COUNT(*) FROM [{schema}].[{table_cfg.name}];"
        # current_rows_result = await self.db_adapter.execute_query(count_query, fetch_results=True)
        # current_rows = 0
        # if current_rows_result and current_rows_result[0].get('') is not None: # Result might be unnamed
        #     current_rows = list(current_rows_result[0].values())[0]
        # rows_to_generate = table_cfg.row_count - current_rows
        # if rows_to_generate <= 0:
        #     logger.info(f"Table {schema}.{table_cfg.name} has enough rows. Skipping.")
        #     return
        # logger.info(f"Generating {rows_to_generate} new rows for {schema}.{table_cfg.name}.")

        rows_to_generate = table_cfg.row_count 

        column_names_for_insert = [f"[{col.name}]" for col in table_cfg.columns if not col.identity]
        if not column_names_for_insert:
            logger.info(f"Table {schema}.{table_cfg.name} has only identity columns. SQL Server handles generation.")
            return

        placeholders = ', '.join(['?'] * len(column_names_for_insert))
        insert_sql = f"INSERT INTO [{schema}].[{table_cfg.name}] ({', '.join(column_names_for_insert)}) VALUES ({placeholders});"

        generated_count = 0
        for i in range(0, rows_to_generate, self.batch_size):
            batch_data_tuples: List[tuple] = []
            current_batch_size = min(self.batch_size, rows_to_generate - i)
            for _ in range(current_batch_size):
                row_values = await self._generate_row_data(table_cfg)
                batch_data_tuples.append(tuple(row_values))

            if batch_data_tuples:
                try:
                    async with self.db_adapter.pool.acquire() as conn: # type: ignore
                        async with conn.cursor() as cur:
                            await cur.executemany(insert_sql, batch_data_tuples)
                    generated_count += len(batch_data_tuples)
                    logger.info(f"Inserted {len(batch_data_tuples)} rows into {schema}.{table_cfg.name} (Total: {generated_count}/{rows_to_generate})")
                except Exception as e: # Catch a more general exception from aioodbc if needed
                    logger.error(f"Error bulk inserting into {schema}.{table_cfg.name}: {e}")
                    logger.error(f"Problematic SQL: {insert_sql}")
                    if batch_data_tuples: logger.error(f"Sample data: {batch_data_tuples[0]}")
                    # conn.rollback() if not autocommit
                    break # Stop generation for this table on error
        logger.info(f"Data generation for {schema}.{table_cfg.name} completed. Total rows: {generated_count}.")

    async def generate_all_data(self) -> None:
        if not self.schema_config.tables:
            logger.info("No tables to generate data for.")
            return

        for table_cfg in self.schema_config.tables:
            await self.generate_data_for_table(table_cfg)
            pk_col_is_identity = any(col.primary_key and col.identity for col in table_cfg.columns)
            if pk_col_is_identity:
                await self.schema_manager.refresh_tables_metadata() # Full refresh for simplicity now
        logger.info("All data generation tasks completed.")
