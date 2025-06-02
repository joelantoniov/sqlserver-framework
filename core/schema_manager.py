#! /usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from typing import List, Dict, Any, Optional

from core.models import SchemaConfig, TableSchema, ColumnDefinition, IndexDefinition, IndexType
from core.adapters import DatabaseAdapter 

logger = logging.getLogger(__name__)

class SchemaManager:
    def __init__(self, db_adapter: DatabaseAdapter, schema_config: SchemaConfig):
        self.db_adapter = db_adapter
        self.schema_config = schema_config
        self.tables_metadata: Dict[str, Dict[str, Any]] = {} # Stores PK info {table_name: {pk_col, min_id, max_id}}

    def _get_sql_server_type_str(self, col_def: ColumnDefinition) -> str:
        """Maps ColumnDefinition to SQL Server specific type string."""
        type_name_upper = col_def.type.upper()
        if type_name_upper == "INT": return "INT"
        if type_name_upper == "BIGINT": return "BIGINT"
        if type_name_upper == "VARCHAR":
            length = col_def.length if col_def.length else 'MAX'
            return f"VARCHAR({length})"
        if type_name_upper == "NVARCHAR":
            length = col_def.length if col_def.length else 'MAX'
            return f"NVARCHAR({length})"
        if type_name_upper == "TEXT": return "NVARCHAR(MAX)" # TEXT is deprecated
        if type_name_upper == "DECIMAL":
            precision = col_def.precision if col_def.precision else 18
            scale = col_def.scale if col_def.scale else 2
            return f"DECIMAL({precision}, {scale})"
        if type_name_upper == "FLOAT": return "FLOAT"
        if type_name_upper == "BIT": return "BIT" # SQL Server uses BIT for boolean
        if type_name_upper == "BOOLEAN": return "BIT"
        if type_name_upper == "DATE": return "DATE"
        if type_name_upper == "DATETIME": return "DATETIME2"
        if type_name_upper == "DATETIME2": return "DATETIME2"
        if type_name_upper == "TIMESTAMP": return "DATETIME2" # TIMESTAMP in SQL Server is rowversion

        logger.warning(f"Unknown data type '{col_def.type}' for column '{col_def.name}', defaulting to NVARCHAR(MAX).")
        return "NVARCHAR(MAX)"

    async def create_all_schemas(self, recreate_schema_on_run: bool = False) -> None:
        if not self.schema_config.tables:
            logger.info("No tables defined in schema configuration.")
            return

        if recreate_schema_on_run:
            logger.info("Dropping existing tables (if they exist) as per configuration...")
            for table_cfg in reversed(self.schema_config.tables): # Drop in reverse for FKs
                await self.drop_table(table_cfg.name)

        for table_cfg in self.schema_config.tables:
            await self.create_table(table_cfg)

        for table_cfg in self.schema_config.tables:
            await self.create_indexes_for_table(table_cfg)
            await self.create_foreign_keys_for_table(table_cfg)
        logger.info("Schema creation process completed.")
        await self.refresh_tables_metadata()

    async def drop_table(self, table_name: str, schema: str = 'dbo') -> None:
        if await self.db_adapter.check_table_exists(table_name, schema):
            query = f"DROP TABLE [{schema}].[{table_name}];" # Use schema
            logger.info(f"Dropping table {schema}.{table_name}...")
            if await self.db_adapter.execute_query(query) is not None:
                 logger.info(f"Table {schema}.{table_name} dropped successfully.")
            else:
                 logger.warning(f"Failed to drop table {schema}.{table_name}.")
        else:
            logger.info(f"Table {schema}.{table_name} does not exist, skipping drop.")

    async def create_table(self, table_cfg: TableSchema, schema: str = 'dbo') -> None:
        if await self.db_adapter.check_table_exists(table_cfg.name, schema):
            logger.info(f"Table {schema}.{table_cfg.name} already exists. Skipping creation.")
            return

        columns_sql_parts: List[str] = []
        primary_key_cols: List[str] = []

        for col_def in table_cfg.columns:
            col_sql_type = self._get_sql_server_type_str(col_def)
            col_sql = f"[{col_def.name}] {col_sql_type}"
            if col_def.identity:
                col_sql += " IDENTITY(1,1)"
            if not col_def.nullable and not col_def.primary_key: # PKs are implicitly NOT NULL
                col_sql += " NOT NULL"
            elif col_def.primary_key: # PKs are always NOT NULL
                 col_sql += " NOT NULL"

            if col_def.default is not None:
                default_val = col_def.default
                if isinstance(default_val, str) and col_sql_type.upper().startswith(("NVARCHAR", "VARCHAR", "DATE", "DATETIME")):
                    default_val = f"'{default_val.replace("'", "''")}'" # Basic SQL injection prevention for defaults
                col_sql += f" DEFAULT {default_val}"

            if col_def.unique and not col_def.primary_key: # Handle unique constraint for non-PK columns
                # This creates a separate unique constraint. If part of a multi-col unique index, define in table_cfg.indexes
                col_sql += " UNIQUE"

            columns_sql_parts.append(col_sql)
            if col_def.primary_key:
                primary_key_cols.append(f"[{col_def.name}]")

        pk_constraint_sql = ""
        if primary_key_cols:
            pk_name = f"PK_{table_cfg.name}"
            pk_constraint_sql = f", CONSTRAINT [{pk_name}] PRIMARY KEY ({', '.join(primary_key_cols)})"

        full_table_sql = f"CREATE TABLE [{schema}].[{table_cfg.name}] ({', '.join(columns_sql_parts)}{pk_constraint_sql});"

        logger.info(f"Creating table {schema}.{table_cfg.name}...")
        if await self.db_adapter.execute_query(full_table_sql) is not None:
            logger.info(f"Table {schema}.{table_cfg.name} created successfully.")
        else:
            logger.error(f"Failed to create table {schema}.{table_cfg.name}. SQL: {full_table_sql}")


    async def create_indexes_for_table(self, table_cfg: TableSchema, schema: str = 'dbo') -> None:
        for index_def in table_cfg.indexes:
            if await self.db_adapter.check_index_exists(table_cfg.name, index_def.name, schema):
                logger.info(f"Index {index_def.name} on table {schema}.{table_cfg.name} already exists. Skipping.")
                continue

            cols_str = ', '.join([f"[{col}]" for col in index_def.columns])

            index_type_str = ""
            if index_def.type == IndexType.CLUSTERED: index_type_str = "CLUSTERED"
            elif index_def.type == IndexType.NONCLUSTERED: index_type_str = "NONCLUSTERED"
            elif index_def.type == IndexType.UNIQUE: index_type_str = "UNIQUE NONCLUSTERED" # Default unique to nonclustered
            elif index_def.type == IndexType.COLUMNSTORE_CLUSTERED: index_type_str = "CLUSTERED COLUMNSTORE"
            elif index_def.type == IndexType.COLUMNSTORE_NONCLUSTERED: index_type_str = "NONCLUSTERED COLUMNSTORE"
            else: index_type_str = "NONCLUSTERED" # Default

            if index_def.unique and index_def.type not in [IndexType.UNIQUE, IndexType.CLUSTERED]: # Ensure UNIQUE keyword if specified and not already implied
                if "UNIQUE" not in index_type_str:
                     index_type_str = "UNIQUE " + index_type_str

            # For COLUMNSTORE NONCLUSTERED, columns are specified. For CLUSTERED COLUMNSTORE, they are not in CREATE INDEX.
            index_cols_for_create = cols_str
            if index_def.type == IndexType.COLUMNSTORE_CLUSTERED:
                index_cols_for_create = "" # No columns listed for CREATE CLUSTERED COLUMNSTORE INDEX
            elif index_def.type == IndexType.COLUMNSTORE_NONCLUSTERED and not cols_str:
                 logger.error(f"Nonclustered columnstore index {index_def.name} requires columns. Skipping.")
                 continue


            query = f"CREATE {index_type_str} INDEX [{index_def.name}] ON [{schema}].[{table_cfg.name}]"
            if index_cols_for_create: # Add columns if applicable for the index type
                query += f" ({index_cols_for_create})"

            if index_def.include:
                include_str = ', '.join([f"[{col}]" for col in index_def.include])
                query += f" INCLUDE ({include_str})"

            if index_def.type == IndexType.FILTERED and index_def.filtered_predicate:
                query += f" WHERE {index_def.filtered_predicate}"
            query += ";"

            logger.info(f"Creating index {index_def.name} on {schema}.{table_cfg.name}: {query}")
            if await self.db_adapter.execute_query(query) is not None:
                logger.info(f"Index {index_def.name} created successfully.")
            else:
                logger.error(f"Failed to create index {index_def.name}.")

    async def create_foreign_keys_for_table(self, table_cfg: TableSchema, schema: str = 'dbo') -> None:
        for col_def in table_cfg.columns:
            if col_def.foreign_key:
                fk_conf = col_def.foreign_key
                fk_col_name = col_def.name
                ref_table = fk_conf['table']
                ref_col = fk_conf['column']
                fk_name = f"FK_{table_cfg.name}_{fk_col_name}_{ref_table}"

                if await self.db_adapter.check_foreign_key_exists(table_cfg.name, fk_name, schema):
                    logger.info(f"FK {fk_name} on {schema}.{table_cfg.name} for {fk_col_name} already exists. Skipping.")
                    continue

                query = (f"ALTER TABLE [{schema}].[{table_cfg.name}] ADD CONSTRAINT [{fk_name}] "
                         f"FOREIGN KEY ([{fk_col_name}]) REFERENCES [{schema}].[{ref_table}]([{ref_col}]);")

                logger.info(f"Creating FK {fk_name} on {schema}.{table_cfg.name} for {fk_col_name}...")
                if await self.db_adapter.execute_query(query) is not None:
                    logger.info(f"FK {fk_name} created successfully.")
                else:
                    logger.error(f"Failed to create FK {fk_name}.")

    async def refresh_tables_metadata(self) -> None:
        logger.info("Refreshing table metadata (min/max IDs for FKs)...")
        self.tables_metadata = {}
        for table_cfg in self.schema_config.tables:
            pk_column_def: Optional[ColumnDefinition] = None
            for col in table_cfg.columns:
                if col.primary_key and col.identity: # Assuming single PK identity col for FK generation
                    pk_column_def = col
                    break

            if pk_column_def:
                min_id, max_id = await self.db_adapter.get_column_min_max(table_cfg.name, pk_column_def.name)
                if min_id is not None and max_id is not None:
                    self.tables_metadata[table_cfg.name] = {'pk_column': pk_column_def.name, 'min_id': min_id, 'max_id': max_id}
                    logger.debug(f"Metadata for {table_cfg.name}: PK={pk_column_def.name}, MinID={min_id}, MaxID={max_id}")
                else:
                    logger.debug(f"No data or PK values yet for {table_cfg.name} to determine min/max ID. Min: {min_id}, Max: {max_id}")
                    self.tables_metadata[table_cfg.name] = {'pk_column': pk_column_def.name, 'min_id': None, 'max_id': None}
            else:
                logger.debug(f"No identity PK found for table {table_cfg.name} for metadata caching.")
        logger.info("Table metadata refresh complete.")
