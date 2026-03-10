import logging
# import pandas as pd
from pathlib import Path
import json
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode, isnull, sum


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
    # count = df.select([
    #     sum(col(c).isNull().cast("int")).alias(c)
    #     for c in df.columns
    # ])

    # count.show()

    # df_exploded = df.select(explode("additionalProperties").alias("prop"))

    # df_exploded = df_exploded.select(
    #     col("prop.key").alias("key"),
    #     col("prop.value").alias("value")
    # )

    # count = df_exploded.select([
    #     sum(col(c).isNull().cast("int")).alias(c)
    #     for c in df_exploded.columns
    # ])

    # count.show()

    #TODO: Conferir: Tipos de dados, consistencia, outliers, formatacao strings, 

    ...

def transform_arrivals(df: DataFrame) -> DataFrame:
    count = df.select([
        sum(col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ])

    count.show()

def transform_status(df: DataFrame) -> DataFrame:
    
    df_exploded = df.select(explode("lineStatuses").alias("status"))
    df_exploded.printSchema()
    



def run_transform():
    # bikepoint_df = read_data("data/raw/bikepoint")
    # df_transformed_bikepoint = transform_bikepoint(bikepoint_df)

    tubestatus_df = read_data("data/raw/tubestatus")
    df_transformed_tube_status = transform_status(tubestatus_df)

    # arrivals_df = read_data("data/raw/arrivals")
    # df_transformed_arrivals = transform_bikepoint(arrivals_df)


run_transform()