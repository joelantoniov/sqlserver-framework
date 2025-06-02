#! /usr/bin/env python
# -*- coding: utf-8 -*-

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Dict, Any, Optional, Union
from datetime import datetime

class LogLevel(Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

class IndexType(Enum):
    CLUSTERED = "CLUSTERED"
    NONCLUSTERED = "NONCLUSTERED"
    UNIQUE = "UNIQUE"
    FILTERED = "FILTERED"
    COLUMNSTORE_CLUSTERED = "COLUMNSTORE_CLUSTERED"
    COLUMNSTORE_NONCLUSTERED = "COLUMNSTORE_NONCLUSTERED"

@dataclass
class DBConnectionConfig:
    server: str
    database: str
    username: Optional[str] = None
    password: Optional[str] = None
    driver: str = '{ODBC Driver 17 for SQL Server}' # We use Driver 17 by Default, but can be overridden
    encrypt: str = 'yes' # This is common for Azure SQL
    trust_server_certificate: str = 'yes' # Also common for Azure SQL

@dataclass
class ColumnDefinition:
    name: str
    type: str # SQL data type e.g., INT, NVARCHAR(50), DECIMAL(10,2)
    primary_key: bool = False
    identity: bool = False
    nullable: bool = True
    unique: bool = False # For unique constraints/indexes on single columns
    default: Optional[Any] = None
    generator: Optional[str] = None
    params: Dict[str, Any] = field(default_factory=dict) # Params for Faker generator
    length: Optional[int] = None # For VARCHAR, NVARCHAR
    precision: Optional[int] = None # For DECIMAL
    scale: Optional[int] = None # For DECIMAL
    foreign_key: Optional[Dict[str, str]] = None # e.g., {'table': 'RefTable', 'column': 'RefColumn'}

@dataclass
class IndexDefinition:
    name: str
    columns: List[str]
    type: IndexType = IndexType.NONCLUSTERED
    unique: bool = False
    include: Optional[List[str]] = None # For covering indexes
    filtered_predicate: Optional[str] = None # For filtered indexes

@dataclass
class TableSchema:
    name: str
    columns: List[ColumnDefinition]
    row_count: int = 0
    indexes: List[IndexDefinition] = field(default_factory=list)
    # partitioning_column: Optional[str] = None
    # columnstore_index: Optional[IndexDefinition] = None # For table-wide columnstore

@dataclass
class SchemaConfig:
    tables: List[TableSchema]

@dataclass
class QueryParamGeneratorConfig:
    type: str # e.g., "random_int_from_column_range", "random_from_column_sample", "date_range"
    table: Optional[str] = None
    column: Optional[str] = None
    sample_size: Optional[int] = None
    start_days_ago: Optional[int] = None
    end_days_ago: Optional[int] = None

@dataclass
class QueryDefinition:
    name: str
    template: str
    weight: int = 1
    param_generators : List[QueryParamGeneratorConfig] = field(default_factory=list)
    # fetch_results: bool = True # Could be added to control if SELECT results are fetched

@dataclass
class WorkloadConfig:
    name: str
    type: str # e.g., "OLTP", "OLAP"
    enabled: bool = True
    duration_seconds: int = 60
    concurrency: int = 1
    queries: List[QueryDefinition] = field(default_factory=list)

@dataclass
class MonitoringMetricConfig:
    name: str
    query: str # SQL query for DBMS metric
    frequency_seconds: int = 60

@dataclass
class MonitoringConfig:
    os_metrics: List[str] = field(default_factory=list) # e.g., ['cpu_percent', 'memory_percent']
    dbms_metrics: List[MonitoringMetricConfig] = field(default_factory=list)
    monitoring_interval_seconds: int = 5 # Default interval for OS metrics and check for DBMS

@dataclass
class RecommendationHeuristicConfig:
    name: str
    dmv: str # Name of the DMV metric to check (from MonitoringConfig)
    condition: str # A string condition to evaluate (e.g., "avg_user_impact > 80")
    recommendation_template: str

@dataclass
class SimulationParameters:
    global_duration_seconds: int = 300
    data_generation_batch_size: int = 1000
    log_level: LogLevel = LogLevel.INFO
    output_directory: str = "simulation_results"
    recreate_schema_on_run: bool = True

@dataclass
class FullConfig:
    db_connection: DBConnectionConfig
    schema_config: SchemaConfig
    workloads: List[WorkloadConfig]
    monitoring: MonitoringConfig
    recommendation_config: RecommendationConfig
    simulation_parameters: SimulationParameters

@dataclass
class ResourceMetric: # OS Level
    timestamp: datetime
    cpu_percent: Optional[float] = None
    memory_percent: Optiona[float] = None
    disk_io_read_bytes: Optional[int] = None
    disk_io_write_bytes: Optional[int] = None

@dataclass
class DBMSMetricData: # Generic holder for DMV row data
    metric_name: str
    timestamp: datetime
    data: Dict[str, Any] # Actual row from DMV

@dataclass
class QueryExecutionMetric:
    timestamp: datetime
    workload_name: str
    query_name: str
    query_template: str
    parameters: Optional[List[Any]] = None
    duration_ms: float = 0.0
    rows_affected_or_fetched: Optional[int] = None
    success: bool = True
