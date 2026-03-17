from pyspark.sql import DataFrame
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

load_logger = logging.getLogger("LOAD")


def save_data(df: tuple, name: str):
    load_logger.info(f"Iniciando load de {name}")
    print(df)
    print(type(df))
    load_logger.info(f"Finalizando load de {name}")

def run_load(data: dict):
    load_logger.info("Iniciando processo de carga de dados")
    save_data(data['arrivals'], 'arrivals')
    save_data(data['bikepoint'], 'bikepoint')
    save_data(data['tubestatus'], 'tubestatus')
    load_logger.info("Finalizando processo de carga de dados")