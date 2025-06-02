#! /usr/bin/env python
# -*- coding: utf-8 -*-

import yaml
import logging
from typing import Dict, Any
from core.models import (
    DBConnectionConfig, ColumnDefinition, IndexDefinition, TableSchema, SchemaConfig,
    QueryParamGeneratorConfig, QueryDefinition, WorkloadConfig, MonitoringMetricConfig,
    MonitoringConfig, RecommendationHeuristicConfig, RecommendationConfig,
    SimulationParameters, FullConfig, LogLevel, IndexType
)

logger = logging.getLogger(__name__)

class ConfigLoader:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.raw_config: Dict[str, Any] = self._load_raw_config()
        self.full_config: FullConfig = self._parse_full_config()

    def _load_raw_config(self) -> Dict[str, Any]:
        try:
            with open(self.config_path, 'r') as f:
                config_data = yaml.safe_load(f)
            logger.info(f"Raw configuration loaded successfully from {self.config_path}")
            return config_data
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {self.config_path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML configuration file {self.config_path}: {e}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred while loading raw config {self.config_path}: {e}")
            raise

    def _parse_db_connection(self, data: Dict[str, Any]) -> DBConnectionConfig:
        return DBConnectionConfig(**data)

    def _parse_column_definition(self, data: Dict[str, Any]) -> ColumnDefinition:
        fk_data = data.pop('foreign_key', None)
        col_def = ColumnDefinition(**data)
        if fk_data:
            col_def.foreign_key = fk_data
        return col_def

    def _parse_index_definition(self, data: Dict[str, Any]) -> IndexDefinition:
        if 'type' in data and isinstance(data['type'], str):
            try:
                data['type'] = IndexType[data['type'].upper()]
            except KeyError:
                logger.warning(f"Invalid index type '{data['type']}' for index '{data.get('name')}'. Defaulting to NONCLUSTERED.")
                data['type'] = IndexType.NONCLUSTERED
        return IndexDefinition(**data)

    def _parse_table_schema(self, data: Dict[str, Any]) -> TableSchema:
        columns_data = data.get('columns', [])
        indexes_data = data.get('indexes', [])

        columns = [self._parse_column_definition(col) for col in columns_data]
        indexes = [self._parse_index_definition(idx) for idx in indexes_data]

        return TableSchema(
            name=data['name'],
            columns=columns,
            row_count=data.get('row_count', 0),
            indexes=indexes
        )

    def _parse_schema_config(self, data: Dict[str, Any]) -> SchemaConfig:
        tables_data = data.get('tables', [])
        tables = [self._parse_table_schema(tbl) for tbl in tables_data]
        return SchemaConfig(tables=tables)

    def _parse_query_param_generator(self, data: Dict[str, Any]) -> QueryParamGeneratorConfig:
        return QueryParamGeneratorConfig(**data)

    def _parse_query_definition(self, data: Dict[str, Any]) -> QueryDefinition:
        param_gens_data = data.get('param_generators', [])
        param_gens = [self._parse_query_param_generator(pg) for pg in param_gens_data]
        return QueryDefinition(
            name=data['name'],
            template=data['template'],
            weight=data.get('weight', 1),
            param_generators=param_gens
        )

    def _parse_workload_config(self, data: Dict[str, Any]) -> WorkloadConfig:
        queries_data = data.get('queries', [])
        queries = [self._parse_query_definition(q) for q in queries_data]
        return WorkloadConfig(
            name=data['name'],
            type=data['type'],
            enabled=data.get('enabled', True),
            duration_seconds=data.get('duration_seconds', 60),
            concurrency=data.get('concurrency', 1),
            queries=queries
        )

    def _parse_monitoring_metric_config(self, data: Dict[str, Any]) -> MonitoringMetricConfig:
        return MonitoringMetricConfig(**data)

    def _parse_monitoring_config(self, data: Dict[str, Any]) -> MonitoringConfig:
        dbms_metrics_data = data.get('dbms_metrics', [])
        dbms_metrics = [self._parse_monitoring_metric_config(m) for m in dbms_metrics_data]
        return MonitoringConfig(
            os_metrics=data.get('os_metrics', []),
            dbms_metrics=dbms_metrics,
            monitoring_interval_seconds=data.get('monitoring_interval_seconds', 5)
        )

    def _parse_recommendation_heuristic(self, data: Dict[str, Any]) -> RecommendationHeuristicConfig:
        return RecommendationHeuristicConfig(**data)

    def _parse_recommendation_config(self, data: Dict[str, Any]) -> RecommendationConfig:
        heuristics_data = data.get('heuristics', [])
        heuristics = [self._parse_recommendation_heuristic(h) for h in heuristics_data]
        return RecommendationConfig(heuristics=heuristics)

    def _parse_simulation_parameters(self, data: Dict[str, Any]) -> SimulationParameters:
        log_level_str = data.get('log_level', 'INFO').upper()
        try:
            log_level_enum = LogLevel[log_level_str]
        except KeyError:
            logger.warning(f"Invalid log level '{log_level_str}'. Defaulting to INFO.")
            log_level_enum = LogLevel.INFO

        return SimulationParameters(
            global_duration_seconds=data.get('global_duration_seconds', 300),
            data_generation_batch_size=data.get('data_generation_batch_size', 1000),
            log_level=log_level_enum,
            output_directory=data.get('output_directory', "simulation_results"),
            recreate_schema_on_run=data.get('recreate_schema_on_run', True)
        )

    def _parse_full_config(self) -> FullConfig:
        return FullConfig(
            db_connection=self._parse_db_connection(self.raw_config['db_connection']),
            schema_config=self._parse_schema_config(self.raw_config['schema_config']),
            workloads=[self._parse_workload_config(wc) for wc in self.raw_config.get('workloads', [])],
            monitoring=self._parse_monitoring_config(self.raw_config.get('monitoring', {})),
            recommendation_config=self._parse_recommendation_config(self.raw_config.get('recommendation_config', {})),
            simulation_parameters=self._parse_simulation_parameters(self.raw_config.get('simulation_parameters', {}))
        )

    def get_config(self) -> FullConfig:
        return self.full_config
