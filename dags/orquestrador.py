from airflow import DAG
from datetime import datetime
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from docker.types import Mount

with DAG(
    dag_id="tfl",
    start_date=datetime(2024, 1, 1),
    schedule="*/15 * * * *",
    catchup=False
) as dag:

    run_pipeline = AwsGlueJobOperator(
    task_id="run_tfl_main",
    job_name = 'tfl-main',
    iam_role_name = 'mwaa',
    region_name = 'us-east-1'
)