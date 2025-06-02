# SQL Server Performance Advisor & Workload Simulator v1.0

This framework is designed to model database schemas, generate realistic data,
simulate workloads, monitor performance, and provide insights for SQL Server,
using modern Python features like asyncio, dataclasses, and type hinting.

## Project Structure

- `main.py`: The main asynchronous entry point to run simulations.
- `config/`: Contains YAML configuration files. `schema_example.yaml` is a template.
- `core/`: Core modules:
    - `models.py`: Dataclasses and Enums for configuration and metrics.
    - `config_loader.py`: Loads YAML into dataclasses.
    - `adapters.py`: Abstract `DatabaseAdapter` and `SQLServerAdapter` (using `aioodbc`).
    - `schema_manager.py`: Manages schema creation/modification (async).
    - `data_generator.py`: Generates synthetic data (async).
    - `workload_executor.py`: Executes defined workloads (async).
    - `resource_monitor.py`: Abstract `ResourceMonitorBase` and `SystemResourceMonitor`.
    - `metrics_collector.py`: Collects and stores performance metrics.
- `analysis/`: Modules for analyzing metrics and generating recommendations.
- `utils/`: Utility functions like logging.
- `requirements.txt`: Python package dependencies.
- `simulation_results/`: Default directory for storing logs and metrics (created on run).

## Prerequisites

1.  **Python 3.8+**
2.  **SQL Server Instance:** Accessible (local, on-prem, or Azure SQL).
3.  **ODBC Driver for SQL Server:** Ensure it's installed (e.g., 'ODBC Driver 17 for SQL Server').
    `aioodbc` will use this driver.
4.  **Permissions:** The database user needs permissions to create tables, indexes,
    read DMVs, and execute queries. For OS metrics, the script needs appropriate
    permissions.

## Setup

1.  **Create a virtual environment (recommended):**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\\Scripts\\activate
    ```

2.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Configure Database Connection:**
    Edit `config/schema_example.yaml`:
    - Update `db_connection` section with your SQL Server details.
    - The specified `database` should exist.

## Running a Simulation

1.  **Define your schema, data, and workloads:**
    Modify `config/schema_example.yaml` according to the defined dataclasses in `core/models.py`.

2.  **Run the main script:**
    ```bash
    python main.py --config config/schema_example.yaml
    ```

## Output

- **Logs:** Printed to the console and to log files in `simulation_results/<run_timestamp>/`.
- **Metrics:** Collected metrics (query, system, DBMS) stored in JSONL files in the run-specific output directory.
- **Recommendations:** Heuristic-based recommendations printed to console and logged.

## Key Changes in this Version

- **Asynchronous Operations:** Core database interactions, workload execution, and parts of monitoring now use `asyncio` for improved concurrency. `aioodbc` is used for SQL Server.
- **Dataclasses:** Configuration and metric structures are defined using `@dataclass` for better organization and type safety.
- **Abstract Base Classes (ABCs):** `DatabaseAdapter` and `ResourceMonitorBase` provide a clear contract for different implementations.
- **Type Hinting:** Extensive type hints are used throughout the codebase.
- **Enums:** Used for predefined sets of values like log levels or index types.
- **Modular Structure:** Maintained and refined.
