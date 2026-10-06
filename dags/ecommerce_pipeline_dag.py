"""
ecommerce_pipeline_dag.py
==========================
Airflow DAG for E-Commerce Data Pipeline

Schedule: Daily at midnight
Tasks:
    1. extract_and_load  -> API se data fetch -> Snowflake RAW
    2. dbt_run           -> Staging + Marts models run
    3. dbt_test          -> Data quality checks
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# ─────────────────────────────────────────────
# PATHS — sab kuch yahan update karo
# ─────────────────────────────────────────────
PROJECT_PATH = "/mnt/c/Users/lenovo/Desktop/learning Project"
DBT_PATH     = f"{PROJECT_PATH}/ecommerce_dbt"
ENV_PATH     = f"{PROJECT_PATH}/.env"
DBT_BIN      = f"{PROJECT_PATH}/airflow_env/bin/dbt"  # ← exact dbt path

# ─────────────────────────────────────────────
# DEFAULT ARGS
# ─────────────────────────────────────────────
default_args = {
    "owner"           : "mudassir",
    "retries"         : 2,
    "retry_delay"     : timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry"  : False,
    "depends_on_past" : False,
}

# ─────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────
with DAG(
    dag_id            = "ecommerce_pipeline",
    description       = "Extract from DummyJSON API -> Snowflake -> dbt",
    default_args      = default_args,
    start_date        = datetime(2024, 1, 1),
    schedule_interval = "0 0 * * *",    # daily at midnight
    catchup           = False,
    tags              = ["ecommerce", "snowflake", "dbt"],
) as dag:

    # ─────────────────────────────────────────
    # TASK 1: Extract from API + Load to Snowflake
    # ─────────────────────────────────────────
    def run_extraction():
        import sys
        import os
        import importlib.util
        from typing import List, Dict

        # Project path add karo
        sys.path.insert(0, PROJECT_PATH)

        # .env load karo
        from dotenv import load_dotenv
        load_dotenv(ENV_PATH)

        # app.py load karo
        spec   = importlib.util.spec_from_file_location(
            "app",
            os.path.join(PROJECT_PATH, "app.py")
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # run() call karo
        module.run()

    extract_load_task = PythonOperator(
        task_id         = "extract_and_load",
        python_callable = run_extraction,
    )

    # ─────────────────────────────────────────
    # TASK 2: dbt run (staging + marts)
    # ─────────────────────────────────────────
    dbt_run_task = BashOperator(
        task_id      = "dbt_run",
        bash_command = (
            f"cd '{DBT_PATH}' && "
            f"'{DBT_BIN}' run --profiles-dir '{DBT_PATH}'"
        ),
    )

    # ─────────────────────────────────────────
    # TASK 3: dbt test (data quality)
    # ─────────────────────────────────────────
    dbt_test_task = BashOperator(
        task_id      = "dbt_test",
        bash_command = (
            f"cd '{DBT_PATH}' && "
            f"'{DBT_BIN}' test --profiles-dir '{DBT_PATH}'"
        ),
    )

    # ─────────────────────────────────────────
    # Task order: extract -> dbt run -> dbt test
    # ─────────────────────────────────────────
    extract_load_task >> dbt_run_task >> dbt_test_task