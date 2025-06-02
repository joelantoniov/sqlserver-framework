#! /usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import logging
import asyncio 
import os
from datetime import datetime

from core.models import FullConfig, LogLevel
from core.config_loader import ConfigLoader
from core.adapters import SQLServerAdapter 
from core.schema_manager import SchemaManager
from core.data_generator import DataGenerator
from core.workload_executor import WorkloadExecutor
from core.resource_monitor import SystemResourceMonitor 
from core.metrics_collector import MetricsCollector
from analysis.performance_analyzer import PerformanceAnalyzer
from analysis.recommendation_engine import RecommendationEngine
from utils.logger import setup_logger

# Global logger, configured after loading sim params
main_logger: Optional[logging.Logger] = None

async def run_simulation(config: FullConfig) -> None:
    """Main asynchronous simulation logic."""
    global main_logger # Allow modification of the global logger

    sim_params = config.simulation_parameters

    # Setup main logger using fully parsed config
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S") # For unique log file per run
    log_file_name = os.path.join(sim_params.output_directory, run_ts, "simulation_main.log")
    main_logger = setup_logger('SQLAdvisorMain', log_file_name, level=sim_params.log_level)
    main_logger.info(f"Main logger initialized. Log level: {sim_params.log_level.value}")
    main_logger.info(f"Output directory for this run: {os.path.join(sim_params.output_directory, run_ts)}")


    db_adapter = SQLServerAdapter(config.db_connection)
    metrics_collector = MetricsCollector(output_dir=sim_params.output_directory)


    try:
        await db_adapter.connect()
        main_logger.info("Database connection established.")

        schema_manager = SchemaManager(db_adapter, config.schema_config)

        main_logger.info("Starting Schema Management Phase...")
        await schema_manager.create_all_schemas(recreate_schema_on_run=sim_params.recreate_schema_on_run)
        main_logger.info("Schema Management Phase Completed.")

        await schema_manager.refresh_tables_metadata()

        main_logger.info("Starting Data Generation Phase...")
        data_generator = DataGenerator(db_adapter, config.schema_config, schema_manager,
                                       batch_size=sim_params.data_generation_batch_size)
        await data_generator.generate_all_data()
        main_logger.info("Data Generation Phase Completed.")

        await schema_manager.refresh_tables_metadata() 

        main_logger.info("Starting Resource Monitoring...")
        # Pass db_adapter to SystemResourceMonitor if it needs to collect DBMS metrics directly
        resource_monitor = SystemResourceMonitor(config.monitoring, db_adapter, metrics_collector)
        await resource_monitor.start_monitoring() 

        main_logger.info("Starting Workload Execution Phase...")
        workload_executor = WorkloadExecutor(db_adapter, config.workloads, schema_manager, metrics_collector)
        await workload_executor.run_all_workloads(global_duration_seconds=sim_params.global_duration_seconds)
        main_logger.info("Workload Execution Phase Completed.")

        main_logger.info("Stopping Resource Monitoring...")
        await resource_monitor.stop_monitoring() 
        main_logger.info("Resource Monitoring Stopped.")

        main_logger.info("Starting Performance Analysis Phase...")
        analyzer = PerformanceAnalyzer(metrics_collector)
        await analyzer.analyze() 
        main_logger.info("Performance Analysis Phase Completed.")

        main_logger.info("Starting Recommendation Generation Phase...")
        recommender = RecommendationEngine(metrics_collector, config.recommendation_config)
        recommendations = await recommender.generate_recommendations() 
        if recommendations:
            main_logger.info(f"Generated {len(recommendations)} recommendations. See logs.")
        else:
            main_logger.info("No specific recommendations were generated.")
        main_logger.info("Recommendation Generation Phase Completed.")

    except ConnectionError as conn_err: 
        if main_logger: main_logger.critical(f"Database connection failed: {conn_err}", exc_info=True)
        else: print(f"Database connection failed: {conn_err}")
    except Exception as e:
        if main_logger: main_logger.critical(f"An unexpected error occurred during simulation: {e}", exc_info=True)
        else: print(f"An unexpected error occurred during simulation: {e}")
    finally:
        if db_adapter:
            await db_adapter.disconnect()
            if main_logger: main_logger.info("Database connection closed.")
        if main_logger: main_logger.info("Simulation run finished.")

        if main_logger:
            for handler in main_logger.handlers[:]:
                handler.close()
                main_logger.removeHandler(handler)

async def main_async(config_path_arg: str) -> None:
    """Async entry point."""
    # Initial minimal logger for config loading, before full config is parsed
    bootstrap_log_file = os.path.join("simulation_results", "bootstrap_sim.log") # Temp log file
    pre_logger = setup_logger('BootstrapLogger', bootstrap_log_file, level=LogLevel.INFO) # Use Enum

    try:
        pre_logger.info(f"Loading configuration from: {config_path_arg}")
        config_loader = ConfigLoader(config_path_arg)
        full_config = config_loader.get_config()
        pre_logger.info("Configuration parsed successfully.")
        for handler in pre_logger.handlers[:]:
            handler.close()
            pre_logger.removeHandler(handler)
        try: 
            if os.path.exists(bootstrap_log_file) and os.path.getsize(bootstrap_log_file) < 200 : # Small arbitrary size
                 os.remove(bootstrap_log_file)
                 if os.path.exists(os.path.dirname(bootstrap_log_file)) and not os.listdir(os.path.dirname(bootstrap_log_file)):
                    os.rmdir(os.path.dirname(bootstrap_log_file))

        except OSError:
            pass 

    except Exception as e:
        pre_logger.critical(f"Failed to load or parse configuration: {e}", exc_info=True)
        for handler in pre_logger.handlers[:]:
            handler.close()
            pre_logger.removeHandler(handler)
        return

    await run_simulation(full_config)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SQL Server Performance Advisor & Workload Simulator (Async)")
    parser.add_argument("--config", type=str, required=True, help="Path to the YAML configuration file.")
    args = parser.parse_args()

    asyncio.run(main_async(args.config))
