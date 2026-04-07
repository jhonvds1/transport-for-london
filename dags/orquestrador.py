from airflow import DAG
from datetime import datetime
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator

# Definição da DAG (pipeline de orquestração)
with DAG(
    dag_id="tfl",  # Nome da DAG no Airflow
    start_date=datetime(2024, 1, 1),  # Data inicial para execução da DAG
    schedule="*/15 * * * *",  # Executa a cada 15 minutos
    catchup=False  # Evita execução retroativa de DAGs passadas
) as dag:

    # Task responsável por executar o job do AWS Glue
    run_pipeline = AwsGlueJobOperator(
        task_id="run_tfl_main",  # Nome da tarefa dentro da DAG
        job_name='tfl-main',  # Nome do job previamente criado no AWS Glue
        iam_role_name='mwaa',  # Role IAM usada para executar o job (permissões AWS)
        region_name='us-east-1'  # Região onde o Glue está configurado
    )