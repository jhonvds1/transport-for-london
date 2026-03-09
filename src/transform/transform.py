import logging
# import pandas as pd
from pathlib import Path
import json
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger_transform = logging.getLogger("TRANSFORM")

spark = SparkSession.builder.appName("tfl_transform").getOrCreate()



def read_data(folder: str) -> DataFrame:

    for file in Path(folder).rglob("*.json"):
        logger_transform.info(f"Recebendo dados do arquivo: {file}")

    df = spark.read.option("recursiveFileLookup", "true").option("multiLine", "true").json(folder)

    logger_transform.info(f"Dados coletados de {folder}: {df.count()}")
    
    return df

def transform_bikepoint(df: DataFrame) -> DataFrame:
    ...

def transform_arrivals(df: DataFrame) -> DataFrame:
    ...

def transform_status(df: DataFrame) -> DataFrame:
    ...


def run_transform():
    # bikepoint_df = read_data("data/raw/bikepoint")
    # df_transformed_bikepoint = transform_bikepoint(bikepoint_df)

    # tubestatus_df = read_data("data/raw/tubestatus")
    # df_transformed_tube_status = transform_bikepoint(tubestatus_df)

    arrivals_df = read_data("data/raw/arrivals")
    # df_transformed_arrivals = transform_bikepoint(arrivals_df)

    arrivals_df.printSchema()

run_transform()