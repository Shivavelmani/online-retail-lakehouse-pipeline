from airflow.sdk import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta


# ---------------------------------------------------------
# Databricks notebook paths
# ---------------------------------------------------------

bronze_notebook_path = "/Workspace/Users/shivavelmani@gmail.com/online-retail-lakehouse-pipeline-aws-airflow/01_bronze_ingestion"
silver_notebook_path = "/Workspace/Users/shivavelmani@gmail.com/online-retail-lakehouse-pipeline-aws-airflow/02_silver_transformation"
gold_notebook_path = "/Workspace/Users/shivavelmani@gmail.com/online-retail-lakehouse-pipeline-aws-airflow/03_gold_analytics"
validation_notebook_path = "/Workspace/Users/shivavelmani@gmail.com/online-retail-lakehouse-pipeline-aws-airflow/04_pipeline_validation"
failure_handler_notebook_path = "/Workspace/Users/shivavelmani@gmail.com/online-retail-lakehouse-pipeline-aws-airflow/05_failure_handler"


# ---------------------------------------------------------
# Callback functions
# ---------------------------------------------------------

def record_success(context):
    ti = context["ti"]

    ti.xcom_push(key="status", value="SUCCESS")
    ti.xcom_push(key="error", value="")


def record_failure(context):
    ti = context["ti"]

    exception = context.get("exception")

    ti.xcom_push(key="status", value="FAILED")
    ti.xcom_push(key="error", value=str(exception) if exception else "Task failed")


# ---------------------------------------------------------
# DAG
# ---------------------------------------------------------

with DAG(
    dag_id="online_retail_pipeline",
    start_date=datetime(2026, 10, 6),
    schedule=None,
    catchup=False,
    params={
        "batch_id": "2009-12"
    }
) as dag:

    # -----------------------------------------------------
    # Bronze
    # -----------------------------------------------------

    bronze_task = DatabricksSubmitRunOperator(
        task_id="bronze",
        databricks_conn_id="databricks_default",

	retries=2,
        retry_delay=timedelta(seconds=30),
        
        on_success_callback=record_success,
        on_failure_callback=record_failure,

        tasks=[
            {
                "task_key": "bronze_ingestion",
                "notebook_task": {
                    "notebook_path": bronze_notebook_path,
                    "base_parameters": {
			"bronze_table_path": "{{ var.value.bronze_table_path }}",
                        "batch_id": "{{ params.batch_id }}",
                        "run_id": "{{ dag_run.run_id }}"
                    }
                }
            }
        ],
        performance_target="STANDARD"
    )


    # -----------------------------------------------------
    # Silver
    # -----------------------------------------------------

    silver_task = DatabricksSubmitRunOperator(
        task_id="silver",
        databricks_conn_id="databricks_default",

	retries=2,
        retry_delay=timedelta(seconds=30),

        on_success_callback=record_success,
        on_failure_callback=record_failure,

        tasks=[
            {
                "task_key": "silver_transformation",
                "notebook_task": {
                    "notebook_path": silver_notebook_path,
                    "base_parameters": {
			"silver_table_path": "{{ var.value.silver_table_path }}",
                        "batch_id": "{{ params.batch_id }}",
                        "run_id": "{{ dag_run.run_id }}"
                    }
                }
            }
        ],
        performance_target="STANDARD"
    )


    # -----------------------------------------------------
    # Gold
    # -----------------------------------------------------

    gold_task = DatabricksSubmitRunOperator(
        task_id="gold",
        databricks_conn_id="databricks_default",

        retries=2,
        retry_delay=timedelta(seconds=30),

        on_success_callback=record_success,
        on_failure_callback=record_failure,

        tasks=[
            {
                "task_key": "gold_analytics",
                "notebook_task": {
                    "notebook_path": gold_notebook_path,
                    "base_parameters": {
			"gold_table_path": "{{ var.value.gold_table_path }}",
                        "batch_id": "{{ params.batch_id }}",
                        "run_id": "{{ dag_run.run_id }}"
                    }
                }
            }
        ],
        performance_target="STANDARD"
    )


    # -----------------------------------------------------
    # Validation
    # -----------------------------------------------------

    validation_task = DatabricksSubmitRunOperator(
        task_id="validation",
        databricks_conn_id="databricks_default",

        retries=2,
        retry_delay=timedelta(seconds=30),

        on_success_callback=record_success,
        on_failure_callback=record_failure,

        tasks=[
            {
                "task_key": "pipeline_validation",
                "notebook_task": {
                    "notebook_path": validation_notebook_path,
                    "base_parameters": {
                        "batch_id": "{{ params.batch_id }}",
                        "run_id": "{{ dag_run.run_id }}"
                    }
                }
            }
        ],
        performance_target="STANDARD"
    )


    # -----------------------------------------------------
    # Failure Handler
    # -----------------------------------------------------

    failure_handler_task = DatabricksSubmitRunOperator(
        task_id="failure_handler",
        databricks_conn_id="databricks_default",

	# Failure handler must NOT retry.
        retries=0,

        # Run only when at least one direct upstream task fails.
        trigger_rule="one_failed",

        tasks=[
            {
                "task_key": "failure_handler",
                "notebook_task": {
                    "notebook_path": failure_handler_notebook_path,
                    "base_parameters": {
                        "batch_id": "{{ params.batch_id }}",
                        "run_id": "{{ dag_run.run_id }}",

                        "bronze_status": "{{ task_instance.xcom_pull(task_ids='bronze', key='status') or 'UPSTREAM_FAILED' }}",
                        "bronze_error": "{{ task_instance.xcom_pull(task_ids='bronze', key='error') or '' }}",

                        "silver_status": "{{ task_instance.xcom_pull(task_ids='silver', key='status') or 'UPSTREAM_FAILED' }}",
                        "silver_error": "{{ task_instance.xcom_pull(task_ids='silver', key='error') or '' }}",

                        "gold_status": "{{ task_instance.xcom_pull(task_ids='gold', key='status') or 'UPSTREAM_FAILED' }}",
                        "gold_error": "{{ task_instance.xcom_pull(task_ids='gold', key='error') or '' }}",

                        "validation_status": "{{ task_instance.xcom_pull(task_ids='validation', key='status') or 'UPSTREAM_FAILED' }}",
                        "validation_error": "{{ task_instance.xcom_pull(task_ids='validation', key='error') or '' }}"
                    }
                }
            }
        ],
        performance_target="STANDARD"
    )


    # -----------------------------------------------------
    # Pipeline dependencies
    # -----------------------------------------------------

    bronze_task >> silver_task >> gold_task >> validation_task

    # Failure Handler watches ALL four pipeline tasks
    [
        bronze_task,
        silver_task,
        gold_task,
        validation_task
    ] >> failure_handler_task