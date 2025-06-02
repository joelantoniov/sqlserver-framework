#! /usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import time
import random
import asyncio
from datetime import datetime, timedelta
from typing import List, Any, Dict, Optional, Tuple

from core.models import WorkloadConfig, QueryDefinition, QueryParamGeneratorConfig, QueryExecutionMetric
from core.adapters import DatabaseAdapter
from core.schema_manager import SchemaManager 
from core.metrics_collector import MetricsCollector

logger = logging.getLogger(__name__)

class WorkloadExecutor:
    def __init__(self, db_adapter: DatabaseAdapter, workloads_config: List[WorkloadConfig],
                 schema_manager: SchemaManager, metrics_collector: MetricsCollector):
        self.db_adapter = db_adapter
        self.workloads_config = workloads_config
        self.schema_manager = schema_manager
        self.metrics_collector = metrics_collector
        self._param_cache: Dict[Tuple, List[Any]] = {}

    async def _generate_query_param(self, param_gen_config: QueryParamGeneratorConfig) -> Any:
        gen_type = param_gen_config.type

        if gen_type == "random_int_from_column_range":
            if not param_gen_config.table or not param_gen_config.column:
                logger.warning("Missing table/column for random_int_from_column_range. Returning 1.")
                return 1
            meta = self.schema_manager.tables_metadata.get(param_gen_config.table)
            if meta and meta.get('min_id') is not None and meta.get('max_id') is not None:
                try: return random.randint(meta['min_id'], meta['max_id'])
                except ValueError: 
                    logger.warning(f"Range invalid for {param_gen_config.table}.{param_gen_config.column}. Default 1.")
                    return 1
            logger.warning(f"Metadata for {param_gen_config.table} or range for {param_gen_config.column} missing. Default 1.")
            return 1

        elif gen_type == "random_from_column_sample":
            if not param_gen_config.table or not param_gen_config.column:
                logger.warning("Missing table/column for random_from_column_sample. Returning 'default'.")
                return "default_sample_value"

            sample_size = param_gen_config.sample_size or 100
            cache_key = (gen_type, param_gen_config.table, param_gen_config.column, sample_size)

            if cache_key not in self._param_cache:
                sample_values = await self.db_adapter.get_column_sample(
                    param_gen_config.table, param_gen_config.column, sample_size
                )
                self._param_cache[cache_key] = sample_values if sample_values else ["default_if_empty"]

            cached_sample = self._param_cache[cache_key]
            return random.choice(cached_sample) if cached_sample else "default_sample_value"

        elif gen_type == "date_range":
            start_days = param_gen_config.start_days_ago if param_gen_config.start_days_ago is not None else 30
            end_days = param_gen_config.end_days_ago if param_gen_config.end_days_ago is not None else 0
            start_date = datetime.now() - timedelta(days=start_days)
            end_date = datetime.now() - timedelta(days=end_days)
            return start_date.strftime('%Y-%m-%d %H:%M:%S'), end_date.strftime('%Y-%m-%d %H:%M:%S')

        logger.warning(f"Unknown param generator type: {gen_type}. Returning None.")
        return None

    async def _execute_single_query_instance(self, workload_name: str, query_def: QueryDefinition) -> None:
        params_list: List[Any] = []
        for p_gen_conf in query_def.param_generators:
            param_val = await self._generate_query_param(p_gen_conf)
            if isinstance(param_val, tuple): # For multi-value generators like date_range
                params_list.extend(list(param_val))
            else:
                params_list.append(param_val)

        start_time = time.perf_counter()
raw_results = await self.db_adapter.execute_query(query_def.template, tuple(params_list), fetch_results=True)
        duration_ms = (time.perf_counter() - start_time) * 1000

        rows_affected_or_fetched: Optional[int] = None
        if isinstance(raw_results, list): # SELECT query
            rows_affected_or_fetched = len(raw_results)
        elif isinstance(raw_results, int): # DML query (rowcount)
            rows_affected_or_fetched = raw_results

        success = raw_results is not None 

        metric = QueryExecutionMetric(
            timestamp=datetime.now(),
            workload_name=workload_name,
            query_name=query_def.name,
            query_template=query_def.template,
            parameters=params_list,
            duration_ms=duration_ms,
            rows_affected_or_fetched=rows_affected_or_fetched,
            success=success
        )
        self.metrics_collector.log_query_execution(metric)

        if not success:
            logger.error(f"Query failed: {query_def.name} in workload {workload_name}")
        else:
            logger.debug(f"Executed {query_def.name} in {duration_ms:.2f}ms. Params: {params_list}. Rows: {rows_affected_or_fetched}")

    async def _run_workload_task(self, workload_cfg: WorkloadConfig, stop_event: asyncio.Event) -> None:
        logger.info(f"Starting task for workload: {workload_cfg.name}")

        weighted_queries: List[QueryDefinition] = []
        for q_def in workload_cfg.queries:
            weighted_queries.extend([q_def] * q_def.weight)

        if not weighted_queries:
            logger.warning(f"No queries in workload {workload_cfg.name}. Task exiting.")
            return

        task_start_time = time.time()
        while not stop_event.is_set() and (time.time() - task_start_time) < workload_cfg.duration_seconds:
            query_to_run = random.choice(weighted_queries)
            try:
                await self._execute_single_query_instance(workload_cfg.name, query_to_run)
            except Exception as e:
                logger.error(f"Exception in workload task {workload_cfg.name} for query {query_to_run.name}: {e}", exc_info=True)

            await asyncio.sleep(random.uniform(0.05, 0.5)) # Small random delay

        logger.info(f"Workload task {workload_cfg.name} finished or was stopped.")

    async def run_all_workloads(self, global_duration_seconds: int) -> None:
        self._param_cache.clear() 
        active_workloads = [wl for wl in self.workloads_config if wl.enabled]
        if not active_workloads:
            logger.info("No enabled workloads to run.")
            return

        logger.info(f"Starting all enabled workloads. Global duration: {global_duration_seconds}s.")

        global_stop_event = asyncio.Event()
        tasks = []

        for workload_cfg in active_workloads:
            for _ in range(workload_cfg.concurrency):
                task = asyncio.create_task(self._run_workload_task(workload_cfg, global_stop_event))
                tasks.append(task)

        try:
            await asyncio.wait_for(asyncio.sleep(global_duration_seconds), timeout=global_duration_seconds + 5)
        except asyncio.TimeoutError:
            logger.info("Global simulation duration reached via timeout.")
        finally:
            logger.info("Global simulation time up. Signaling all workload tasks to stop.")
            global_stop_event.set()
            await asyncio.gather(*tasks, return_exceptions=True) 
            logger.info("All workload tasks have completed.")
