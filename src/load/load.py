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

def run_load(data: dict):
    save_data(data['arrivals'], 'arrivals')
    save_data(data['bikepoint'], 'bikepoint')
    save_data(data['tubestatus'], 'tubestatus')