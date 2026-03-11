import logging
# import pandas as pd
from pathlib import Path
import json
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode, first, lit


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

    df_exploded = df.select("id", "commonName", explode("additionalProperties").alias("prop"))

    df_bike = df_exploded.select(
        "id", "commonName", 
        col("prop.key"),
        col("Prop.value")
        )

    df_wide = df_bike.groupBy("id", "commonName") \
            .pivot("key", ["TerminalName", "NbBikes", "NbEmptyDocks", "NbDocks", "NbStandardBikes", "NbEBikes"]) \
            .agg(first("value"))

    df_wide = df_wide.withColumn("mode", lit("bike"))
    df_wide = df_wide.withColumn("platform_name", lit(None))
    df_wide = df_wide.withColumn("direction", lit(None))

    df_wide.show()

        

def transform_arrivals(df: DataFrame) -> DataFrame:
    df = df.select("id", "naptanId", "timeToStation", "vehicleId", "lineId", "lineName", "modeName", "stationName", "platformName", "direction", "timestamp")


def transform_status(df: DataFrame) -> DataFrame:
    df_exploded = df.select(
        "name", "modeName",
        explode("lineStatuses").alias("prop")
    )

    df_status = df_exploded.select(
        "name", "modeName",
        col("prop.lineId").alias("lineId"),
        col("prop.statusSeverityDescription").alias("status"),
        col("prop.reason").alias("reason"),
        explode("prop.validityPeriods").alias("time")
    )

    df_final = df_status.select(
        "name", "modeName", "lineId", "status", "reason",
        col("time.fromDate").alias("start_time"),
        col("time.toDate").alias("end_time")
    )


def run_transform():
    bikepoint_df = read_data("data/raw/bikepoint")
    df_transformed_bikepoint = transform_bikepoint(bikepoint_df)

    # tubestatus_df = read_data("data/raw/tubestatus")
    # df_transformed_tube_status = transform_status(tubestatus_df)

    # arrivals_df = read_data("data/raw/arrivals")
    # df_transformed_arrivals = transform_bikepoint(arrivals_df)


run_transform()