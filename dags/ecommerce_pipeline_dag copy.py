# """
# ecommerce_pipeline_dag.py
# ==========================
# Airflow DAG for E-Commerce Data Pipeline

# Schedule: Daily at midnight
# Tasks:
#     1. extract_and_load  → API se data fetch → Snowflake RAW
#     2. dbt_run           → Staging + Marts models run
#     3. dbt_test          → Data quality checks
# """

# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator

# # ─────────────────────────────────────────────
# # DEFAULT ARGS
# # ─────────────────────────────────────────────
# default_args = {
#     "owner"           : "mudassir",
#     "retries"         : 2,                        # fail hone pe 2 baar retry
#     "retry_delay"     : timedelta(minutes=5),     # retry se pehle 5 min wait
#     "email_on_failure": False,
#     "email_on_retry"  : False,
#     "depends_on_past" : False,
# }

# # ─────────────────────────────────────────────
# # DAG DEFINITION
# # ─────────────────────────────────────────────
# with DAG(
#     dag_id          = "ecommerce_pipeline",
#     description     = "Extract from DummyJSON API → Snowflake → dbt",
#     default_args    = default_args,
#     start_date      = datetime(2024, 1, 1),
#     schedule_interval = "0 0 * * *",              # daily at midnight
#     catchup         = False,                      # purane runs skip karo
#     tags            = ["ecommerce", "snowflake", "dbt"],
# ) as dag:

#     # ─────────────────────────────────────────
#     # TASK 1: Extract from API + Load to Snowflake
#     # ─────────────────────────────────────────
#     def run_extraction():
#         """
#         DummyJSON API se data fetch karo aur Snowflake RAW mein load karo.
#         Yeh wahi script hai jo humne pehle likhi thi.
#         """
#         import sys
#         import os

#         # Project path add karo
#         project_path = "/mnt/c/Users/lenovo/Desktop/learning Project"
#         sys.path.insert(0, project_path)

#         # .env load karo
#         from dotenv import load_dotenv
#         load_dotenv(os.path.join(project_path, ".env"))

#         # Extraction script import karke run karo
#         import importlib.util
#         spec = importlib.util.spec_from_file_location(
#             "extract_and_load",
#             os.path.join(project_path, "app.py")
#         )
#         module = importlib.util.module_from_spec(spec) 
#         spec.loader.exec_module(module)  
#         module.run()

#     extract_load_task = PythonOperator(
#         task_id         = "extract_and_load",
#         python_callable = run_extraction,
#     )

#     # ─────────────────────────────────────────
#     # TASK 2: dbt run (staging + marts)
#     # ─────────────────────────────────────────
#     dbt_run_task = BashOperator(
#         task_id      = "dbt_run",
#         bash_command = (
#             "cd '/mnt/c/Users/lenovo/Desktop/learning Project/ecommerce_dbt' && "
#             "dbt run --profiles-dir '/mnt/c/Users/lenovo/Desktop/learning Project/ecommerce_dbt'"
#         ),
#     )

#     # ─────────────────────────────────────────
#     # TASK 3: dbt test (data quality)
#     # ─────────────────────────────────────────
#     dbt_test_task = BashOperator(
#         task_id      = "dbt_test",
#         bash_command = (
#             "cd '/mnt/c/Users/lenovo/Desktop/learning Project/ecommerce_dbt' && "
#             "dbt test --profiles-dir '/mnt/c/Users/lenovo/Desktop/learning Project/ecommerce_dbt'"
#         ),
#     )

#     # ─────────────────────────────────────────
#     # TASK ORDER (Dependencies)
#     # ─────────────────────────────────────────
#     #
#     #  extract_and_load  →  dbt_run  →  dbt_test
#     #
#     extract_load_task >> dbt_run_task >> dbt_test_task