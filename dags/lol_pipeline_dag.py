from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

# DAG parameters
default_args = {
    "owner": "luca",
    "retries": 1,
}

# DAG configuration
with DAG(
    dag_id="lol_ingestion",
    description="Ingestion LoL matches and timelines - end to end until the Gold layer",
    default_args=default_args,
    start_date=datetime(2026, 8, 1), # to change once we're gonna start the schedule
    schedule=None,
    catchup=False,
    tags=["lol", "ingestion"],
) as dag:

    # Ingestion: Download and archive data from RIOT API
    fetch_and_archive = BashOperator(
        task_id="fetch_and_archive",
        bash_command="python /opt/airflow/scripts/ingestion.py",
        env={
            "RIOT_API_KEY": "{{ var.value.get('riot_api_key', '') }}",
            "STORAGE_BACKEND": "{{ var.value.get('storage_backend', 'local') }}",
            "ARCHIVE_DIR": "{{ var.value.get('archive_dir', '/opt/airflow/data/archive') }}",
            "ARCHIVE_DB_PATH": "{{ var.value.get('archive_db_path', '/opt/airflow/data/archive/archive.db') }}",
            "AZURE_STORAGE_CONNECTION_STRING": "{{ var.value.get('azure_storage_connection_string', '') }}",
            "AZURE_CONTAINER_NAME": "{{ var.value.get('azure_container_name', 'raw-data') }}",
        },
        append_env=True,
    )

    # Sync: Transfer raw data from archive (local or azure) to databricks
    sync_to_databricks = BashOperator(
            task_id="sync_to_databricks",
            bash_command="python /opt/airflow/scripts/sync_to_databricks.py",
            env={
                "STORAGE_BACKEND": "{{ var.value.get('storage_backend', 'local') }}",
                "ARCHIVE_DIR": "{{ var.value.get('archive_dir', '/opt/airflow/data/archive') }}",
                "ARCHIVE_DB_PATH": "{{ var.value.get('archive_db_path', '/opt/airflow/data/archive/archive.db') }}",
                "AZURE_STORAGE_CONNECTION_STRING": "{{ var.value.get('azure_storage_connection_string', '') }}",
                "AZURE_CONTAINER_NAME": "{{ var.value.get('azure_container_name', 'raw-data') }}",
                "DATABRICKS_HOST": "{{ var.value.get('databricks_host', '') }}",
                "DATABRICKS_TOKEN": "{{ var.value.get('databricks_token', '') }}",
            },
            append_env=True,
        )

    # Bronze Layer: Data ingestion in Delta tables
    run_bronze = DatabricksSubmitRunOperator(
        task_id="run_bronze",
        databricks_conn_id="databricks_default",
        tasks=[
            {
                "task_key": "bronze_task",
                "notebook_task": {
                    "notebook_path": "/Workspace/Repos/Projects/LoL/scripts/bronze"
                },
            }
        ],
    )

    # Silver Layer: Data cleaning and transformation
    run_silver = DatabricksSubmitRunOperator(
        task_id="run_silver",
        databricks_conn_id="databricks_default",
        tasks=[
            {
                "task_key": "silver_task",
                "notebook_task": {"notebook_path": "/Workspace/Repos/Projects/LoL/scripts/silver"},
            }
        ],
    )

    # Gold Layer: Data aggregation
    run_gold = DatabricksSubmitRunOperator(
        task_id="run_gold",
        databricks_conn_id="databricks_default",
        tasks=[
            {
                "task_key": "gold_task",
                "notebook_task": {"notebook_path": "/Workspace/Repos/Projects/LoL/scripts/gold"},
            }
        ],
    )

    fetch_and_archive >> sync_to_databricks >> run_bronze >> run_silver >> run_gold