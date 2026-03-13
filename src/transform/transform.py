import logging
# import pandas as pd
from pathlib import Path
import json
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode, first, lit, month, year, day, hour, date_trunc


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

def transform_bikepoint(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    logger_transform.info("Iniciando tranformacao do bikepoint")
    
    logger_transform.info("Explodindo dados do bikepoint")

    df_exploded = df.select("id", "commonName", explode("additionalProperties").alias("prop"))

    logger_transform.info("Selecionando dados do bikepoint")

    df_bike = df_exploded.select(
        "id", "commonName", 
        col("prop.key"),
        col("prop.value"),
        col("prop.modified")
        )

    df_final = df_bike.groupBy("id", "commonName", "modified") \
            .pivot("key", ["TerminalName", "NbBikes", "NbEmptyDocks", "NbDocks", "NbStandardBikes", "NbEBikes"]) \
            .agg(first("value"))

    df_final = df_final.withColumn("mode", lit("bike"))

    logger_transform.info("Removendo colunas nulas")

    not_null_columns = ['id', 'commonName', 'TerminalName', 'NbBikes', 'NbEmptyDocks', 'NbDocks', 'NbStandardBikes', 'NbEBikes', 'mode', 'modified']

    df_final = df_final.dropna(subset=not_null_columns)

    logger_transform.info("Removendo id's duplicados")

    df_final = df_final.drop_duplicates(subset=["id"])

    logger_transform.info("Realizando Cast de tipos")

    df_final = df_final.withColumn("NbBikes", col("NbBikes").cast("int")) \
        .withColumn("NbEmptyDocks", col("NbEmptyDocks").cast("int")) \
        .withColumn("NbDocks", col("NbDocks").cast("int")) \
        .withColumn("NbStandardBikes", col("NbStandardBikes").cast("int")) \
        .withColumn("NbEBikes", col("NbEBikes").cast("int")) \
        .withColumn("modified", col("modified").cast("timestamp")
    )

    logger_transform.info("Realizando logica de negocios")

    df_final = df_final.filter(
        (col("NbEmptyDocks") >= 0) &
        (col("NbDocks") >= 0) &
        (col("NbStandardBikes") >= 0) &
        (col("NbEBikes") >= 0) 
    )

    df_final = df_final.filter(
        col("NbBikes") + col("NbEmptyDocks") == col("NbDocks")
    )

    logger_transform.info("Dim_station criada com sucesso!")

    dim_station = df_final.select("id", "commonName", "mode")

    logger_transform.info("fact_bike_status criada com sucesso!")

    
    fact_bike_status = df_final.select("id", "NbBikes", "NbEmptyDocks", "NbDocks", "NbStandardBikes", "NbEBikes")

    logger_transform.info("Finalizando tranformacao do bikepoint")

    dim_time = df_final.select("modified")

    dim_time = dim_time.withColumn("modified", date_trunc("hour", col("modified")))

    dim_time = dim_time.drop_duplicates()

    dim_time = dim_time \
    .withColumn("year", year(col("modified"))) \
    .withColumn("month", month(col("modified"))) \
    .withColumn("day", day(col("modified"))) \
    .withColumn("hour", hour(col("modified")))


    dim_time = dim_time.withColumnRenamed("modified", "date")
    
    return dim_station,  dim_time, fact_bike_status

def transform_arrivals(df: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    logger_transform.info("Iniciando tranformacao do arrivals")
    
    df = df.select("id", "naptanId", "timeToStation", "vehicleId", "lineId", "lineName", "modeName", "stationName", "platformName", "direction", "timestamp")

    logger_transform.info("Colunas necessarias selecionadas")

    dim_vehicle = df.select("vehicleId")

    fact_arrival = df.select("id", "naptanId", "vehicleId", "timeToStation", "lineId")

    dim_line = df.select("lineId", "lineName", "modeName")

    dim_station = df.select("naptanId", "stationName", "modeName")

    logger_transform.info("Tabelas criadas")

    dim_vehicle = dim_vehicle.dropna()
    fact_arrival = fact_arrival.dropna()
    dim_line = dim_line.dropna()
    dim_station = dim_station.dropna()

    logger_transform.info("Removendo valores nulos")

    fact_arrival = fact_arrival.drop_duplicates(subset=['id'])
    dim_vehicle = dim_vehicle.drop_duplicates(subset=['vehicleId'])
    dim_line = dim_line.drop_duplicates(subset=['lineId'])
    dim_station = dim_station.drop_duplicates(subset=['naptanId'])

    logger_transform.info("Removendo valores duplicados")


    fact_arrival = fact_arrival.withColumn("id", col("id").cast("bigint"))

    logger_transform.info("Realizando cast")

    logger_transform.info("Finalizando tranformacao do arrivals")

    return dim_vehicle, dim_line, dim_station, fact_arrival

def transform_status(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    logger_transform.info("Iniciando tranformacao do tubestatus")
    
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

    logger_transform.info("Selecionando colunas necessárias")


    df_final = df_final.dropna(subset=["lineId", "start_time", "end_time"])

    logger_transform.info("Removendo valores nulos")

    dim_line = df_final.select(
        col("lineId"),
        col("name"),
        col("modeName")
    )

    dim_line = dim_line.drop_duplicates(["lineId"])

    fact_tube_status = df_final.select("lineId", "start_time", "end_time", "status", "reason")

    fact_tube_status = fact_tube_status.withColumn("start_time", col("start_time").cast("timestamp"))
    fact_tube_status = fact_tube_status.withColumn("end_time", col("end_time").cast("timestamp"))

    logger_transform.info("Realizando cast")

    logger_transform.info("Finalizando tranformacao do tubestatus")

    return dim_line, fact_tube_status

def load_trusted_data(path) -> None:
    logger_transform.info(f"Iniciando carga de dados trusted em {path}")

    logger_transform.info("Finalizando carga de dados com sucesso")

def run_transform() -> None:
    logger_transform.info("Processo de transformacao iniciando!")

    bikepoint_df = read_data("data/raw/bikepoint")
    df_transformed_bikepoint = transform_bikepoint(bikepoint_df)

    # tubestatus_df = read_data("data/raw/tubestatus")
    # df_transformed_tube_status = transform_status(tubestatus_df)

    # arrivals_df = read_data("data/raw/arrivals")
    # df_transformed_arrivals = transform_arrivals(arrivals_df)

    logger_transform.info("Processo de transformacao finalizado!")

    # return df_transformed_arrivals, df_transformed_bikepoint, df_transformed_tube_status

run_transform()