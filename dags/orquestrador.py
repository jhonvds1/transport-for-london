from airflow import DAG
from datetime import datetime
from airflow.decorators import task
from extract.extract import run_extract
from transform.transform import run_transform
from load.load import run_load


with DAG(
    dag_id = "tfl",
    start_date = datetime(2024, 1, 1),
    schedule = "* * * * *",
    catchup = False
) as dag:
    @task
    def extract():
        run_extract()
    
    @task
    def transform():
        run_transform()

    # @task
    # def load():
    #     run_load()

    te = extract()
    tt = transform()
    # tl = load()

    te >> tt # >> tl