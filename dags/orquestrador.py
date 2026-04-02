from airflow import DAG
from datetime import datetime
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

with DAG(
    dag_id="tfl",
    start_date=datetime(2024, 1, 1),
    schedule="*/15 * * * *",
    catchup=False
) as dag:

    run_pipeline = DockerOperator(
    task_id="run_pipeline",
    image="airflow_spark:2.7",
    auto_remove="success",
    docker_url="unix://var/run/docker.sock",
    network_mode="bridge",
    mounts=[
        Mount(
            source="C:/Users/jonat/.aws",
            target="/root/.aws",
            type="bind"
        )
    ],
    mount_tmp_dir=False
)