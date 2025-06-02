#! /usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import json
import os
from datetime import datetime
from typing import Any

from core.models import QueryExecutionMetric, ResourceMetric, DBMSMetricData

logger = logging.getLogger(__name__)

class MetricsCollector:
    def __init__(self, output_dir: str = "simulation_results"):
        self.output_dir = output_dir
        self.run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.run_output_dir = os.path.join(self.output_dir, self.run_timestamp)

        self._setup_output_dirs()

        self.query_log_path = os.path.join(self.run_output_dir, "query_executions.jsonl")
        self.system_log_path = os.path.join(self.run_output_dir, "system_metrics.jsonl")
        self.dbms_log_path = os.path.join(self.run_output_dir, "dbms_metrics.jsonl")
        self.recommendation_log_path = os.path.join(self.run_output_dir, "recommendations.txt")

        logger.info(f"Metrics will be stored in: {self.run_output_dir}")

    def _setup_output_dirs(self) -> None:
        try:
            if not os.path.exists(self.output_dir):
                os.makedirs(self.output_dir)
            if not os.path.exists(self.run_output_dir):
                os.makedirs(self.run_output_dir)
        except OSError as e:
            logger.error(f"Error creating output directories {self.run_output_dir}: {e}", exc_info=True)
            raise

    def _default_json_serializer(self, obj: Any) -> Any:
        if isinstance(obj, datetime):
            return obj.isoformat()
        try:
            return str(obj)
        except Exception:
             return f"<unserializable_object type='{type(obj).__name__}'>"


    def _append_to_jsonl_file(self, file_path: str, data_dict: Dict[str, Any]) -> None:
        try:
            with open(file_path, 'a') as f:
                json.dump(data_dict, f, default=self._default_json_serializer)
                f.write('\n')
        except IOError as e:
            logger.error(f"IOError writing to log file {file_path}: {e}", exc_info=True)
        except TypeError as e:
            logger.error(f"TypeError serializing data for {file_path}: {e}. Data: {data_dict}", exc_info=True)

    def log_query_execution(self, query_data: QueryExecutionMetric) -> None:
        from dataclasses import asdict # Late import to avoid circular dependency if models are complex
        logger.debug(f"Query Metric: {query_data.query_name} took {query_data.duration_ms:.2f}ms")
        self._append_to_jsonl_file(self.query_log_path, asdict(query_data))

    def log_system_metric(self, system_data: ResourceMetric) -> None:
        from dataclasses import asdict
        logger.debug(f"OS Metric: CPU {system_data.cpu_percent}%, Mem {system_data.memory_percent}%")
        self._append_to_jsonl_file(self.system_log_path, asdict(system_data))

    def log_dbms_metric(self, dbms_data: DBMSMetricData) -> None:
        from dataclasses import asdict
        logger.debug(f"DBMS Metric: {dbms_data.metric_name} - {str(dbms_data.data)[:100]}...")
        self._append_to_jsonl_file(self.dbms_log_path, asdict(dbms_data))

    def log_recommendation(self, recommendation_text: str) -> None:
        logger.info(f"RECOMMENDATION: {recommendation_text}")
        try:
            with open(self.recommendation_log_path, 'a') as f:
                f.write(f"[{datetime.now().isoformat()}] {recommendation_text}\n")
        except IOError as e:
            logger.error(f"Error writing recommendation to {self.recommendation_log_path}: {e}", exc_info=True)

    def get_collected_dbms_metrics(self, metric_name_filter: Optional[str] = None) -> List[DBMSMetricData]:
        collected: List[DBMSMetricData] = []
        try:
            with open(self.dbms_log_path, 'r') as f:
                for line in f:
                    try:
                        entry_dict = json.loads(line)
                        if entry_dict.get('metric_name') == metric_name_filter or metric_name_filter is None:
                            collected.append(DBMSMetricData(
                                metric_name=entry_dict['metric_name'],
                                timestamp=datetime.fromisoformat(entry_dict['timestamp']),
                                data=entry_dict['data']
                            ))
                    except (json.JSONDecodeError, KeyError, TypeError) as e:
                        logger.warning(f"Skipping malformed line in {self.dbms_log_path}: {line.strip()}. Error: {e}")
        except FileNotFoundError:
            logger.info(f"DBMS metrics file {self.dbms_log_path} not found.")
        except Exception as e:
            logger.error(f"Error reading DBMS metrics from {self.dbms_log_path}: {e}", exc_info=True)
        return collected
