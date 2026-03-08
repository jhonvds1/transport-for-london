import logging
# import pandas as pd
from pathlib import Path
import json
from pyspark.sql import SparkSession, DataFrame


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger_transform = logging.getLogger("TRANSFORM")

spark = SparkSession.builder.appName("tfl_transform").getOrCreate()


def read_raw_data(folder: str) -> list:
    logger_transform.info(f"coletando dados de {folder}")

    files = Path(folder).glob("*.json")

    data = []

    for file in files:
        with open(file) as f:
            data.extend(json.load(f))

    logger_transform.info(f"{len(data)} dados de coletados {folder}")
    

    return data

def transform_list_df(data: list) -> DataFrame:
    return spark.read.json("data/raw/bikepoint/")

def transform_bikepoint(df: DataFrame) -> DataFrame:
    ...

def transform_arrivals(df: DataFrame) -> DataFrame:
    ...

def transform_status(df: DataFrame) -> DataFrame:
    ...


def run_transform():
    data_list = read_raw_data("data/raw/bikepoint")
    data_df = transform_list_df(data_list)
    df_transformed_bikepoint = transform_bikepoint(data_df)
    print(data_df)

    # data_list = read_raw_data("data/raw/tubestatus")
    # data_df = transform_list_df(data_list)
    # df_transformed_tube_status = transform_bikepoint(data_df)


run_transform()