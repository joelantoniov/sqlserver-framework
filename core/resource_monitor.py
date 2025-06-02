#! /usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import psutil
import time
import threading 
import asyncio
from datetime import datetime
from typing import List, Optional, Any, Dict

from core.models import MonitoringConfig, ResourceMetric, DBMSMetricData
from core.adapters import DatabaseAdapter 
from core.metrics_collector import MetricsCollector

logger = logging.getLogger(__name__)

class ResourceMonitorBase(abc.ABC):
    @abc.abstractmethod
    async def start_monitoring(self) -> None: pass

    @abc.abstractmethod
    async def stop_monitoring(self) -> None: pass

class SystemResourceMonitor(ResourceMonitorBase):
    def __init__(self, monitoring_config: MonitoringConfig,
                 db_adapter: DatabaseAdapter, metrics_collector: MetricsCollector):
        self.monitoring_config = monitoring_config
        self.db_adapter = db_adapter # For DBMS metrics
        self.metrics_collector = metrics_collector

        self._stop_event = threading.Event()
        self._monitor_thread: Optional[threading.Thread] = None
        self._dbms_metric_last_run: Dict[str, float] = {}

    def _collect_os_metrics_sync(self) -> Optional[ResourceMetric]:
        os_metrics_to_collect = self.monitoring_config.os_metrics
        if not os_metrics_to_collect: return None

        data = ResourceMetric(timestamp=datetime.now())
        try:
            if 'cpu_percent' in os_metrics_to_collect: data.cpu_percent = psutil.cpu_percent(interval=None)
            if 'memory_percent' in os_metrics_to_collect: data.memory_percent = psutil.virtual_memory().percent
            if 'disk_io_counters' in os_metrics_to_collect:
                io = psutil.disk_io_counters()
                if io:
                    data.disk_io_read_bytes = io.read_bytes
                    data.disk_io_write_bytes = io.write_bytes
            return data
        except Exception as e:
            logger.error(f"Error collecting OS metrics: {e}", exc_info=True)
            return None 

    async def _collect_one_dbms_metric(self, metric_cfg_name: str, query: str) -> None:
        logger.debug(f"Collecting DBMS metric: {metric_cfg_name}")
        try:
            results = await self.db_adapter.execute_query(query, fetch_results=True)
            if results is not None: # Can be empty list
                for row_data in results: # Each row is a metric point
                    dbms_metric = DBMSMetricData(
                        metric_name=metric_cfg_name,
                        timestamp=datetime.now(),
                        data=dict(row_data)
                    )
                    self.metrics_collector.log_dbms_metric(dbms_metric)
            else:
                logger.warning(f"Failed to fetch DBMS metric '{metric_cfg_name}'. Query returned None.")
        except Exception as e:
            logger.error(f"Error collecting DBMS metric {metric_cfg_name}: {e}", exc_info=True)

    def _monitoring_loop_sync(self) -> None:
        logger.info(f"Resource monitoring loop started. Interval: {self.monitoring_config.monitoring_interval_seconds}s")
        # Initialize psutil cpu_percent before loop
        if 'cpu_percent' in self.monitoring_config.os_metrics:
            psutil.cpu_percent(interval=None)

        # Get the current asyncio loop for scheduling DBMS metric collection
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        while not self._stop_event.is_set():
            # Collect OS Metrics (Synchronous)
            os_data = self._collect_os_metrics_sync()
            if os_data:
                self.metrics_collector.log_system_metric(os_data)

            # Collect DBMS Metrics (Asynchronously, scheduled from this thread)
            now = time.time()
            for metric_cfg in self.monitoring_config.dbms_metrics:
                last_run = self._dbms_metric_last_run.get(metric_cfg.name, 0)
                if (now - last_run) >= metric_cfg.frequency_seconds:
                    # Schedule the async DB call in the event loop of this thread
                    asyncio.run_coroutine_threadsafe(
                        self._collect_one_dbms_metric(metric_cfg.name, metric_cfg.query),
                        loop
                    )
                    self._dbms_metric_last_run[metric_cfg.name] = now

            self._stop_event.wait(self.monitoring_config.monitoring_interval_seconds)

        loop.call_soon_threadsafe(loop.stop) # Stop the event loop when monitoring stops
        logger.info("Resource monitoring loop stopped.")


    async def start_monitoring(self) -> None:
        if self._monitor_thread and self._monitor_thread.is_alive():
            logger.warning("Monitoring is already running.")
            return

        self._stop_event.clear()
        self._dbms_metric_last_run = {cfg.name: 0 for cfg in self.monitoring_config.dbms_metrics}

        self._monitor_thread = threading.Thread(target=self._monitoring_loop_sync, daemon=True)
        self._monitor_thread.start()
        logger.info("System resource monitoring started in a separate thread.")


    async def stop_monitoring(self) -> None:
        if self._monitor_thread and self._monitor_thread.is_alive():
            logger.info("Stopping resource monitoring...")
            self._stop_event.set()
            self._monitor_thread.join(timeout=10)
            if self._monitor_thread.is_alive():
                logger.warning("Monitoring thread did not stop in time.")
            else:
                logger.info("Resource monitoring stopped successfully.")
            self._monitor_thread = None
        else:
            logger.info("Monitoring not running or already stopped.")
