import logging
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
import boto3
from pyspark.sql.functions import (
    col, explode, first, lit,
    month, year, day, hour,
    date_trunc, to_date, date_format,
    sha2, concat_ws
)

# Configuração padrão de logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger_transform = logging.getLogger("TRANSFORM")

def read_data(spark: SparkSession, path: str):
    return spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(path)

def transform_bikepoint(df: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
    """
    Realiza transformação dos dados de BikePoint.
    Cria as dimensões e fato relacionadas ao status das bicicletas.
    """

    try:

        logger_transform.info("Iniciando transformação: BIKEPOINT")

        # Explode das propriedades para transformar estrutura aninhada em linhas
        logger_transform.info("Explodindo additionalProperties")

        df_exploded = df.select(
            "id",
            "commonName",
            explode("additionalProperties").alias("prop")
        )

        # Normaliza propriedades
        df_bike = df_exploded.select(
            "id",
            "commonName",
            col("prop.key"),
            col("prop.value"),
            col("prop.modified")
        )

        logger_transform.info("Pivotando propriedades do BikePoint")

        df_final = (
            df_bike
            .groupBy("id", "commonName", "modified")
            .pivot(
                "key",
                [
                    "TerminalName",
                    "NbBikes",
                    "NbEmptyDocks",
                    "NbDocks",
                    "NbStandardBikes",
                    "NbEBikes"
                ]
            )
            .agg(first("value"))
        )

        df_final = df_final.withColumn("mode", lit("bike"))

        # Remove registros incompletos
        logger_transform.info("Removendo registros com colunas obrigatórias nulas")

        required_columns = [
            'id', 'commonName', 'TerminalName',
            'NbBikes', 'NbEmptyDocks', 'NbDocks',
            'NbStandardBikes', 'NbEBikes', 'mode', 'modified'
        ]

        df_final = df_final.dropna(subset=required_columns)

        # Remove duplicatas
        logger_transform.info("Removendo duplicatas por id")

        df_final = df_final.drop_duplicates(subset=["id", "modified"])

        # Cast de tipos
        logger_transform.info("Realizando cast de tipos numéricos")

        df_final = (
            df_final
            .withColumn("NbBikes", col("NbBikes").cast("int"))
            .withColumn("NbEmptyDocks", col("NbEmptyDocks").cast("int"))
            .withColumn("NbDocks", col("NbDocks").cast("int"))
            .withColumn("NbStandardBikes", col("NbStandardBikes").cast("int"))
            .withColumn("NbEBikes", col("NbEBikes").cast("int"))
        )

        # Regras de negócio
        logger_transform.info("Aplicando validações de regra de negócio")

        df_final = df_final.filter(
            (col("NbEmptyDocks") >= 0) &
            (col("NbDocks") >= 0) &
            (col("NbStandardBikes") >= 0) &
            (col("NbEBikes") >= 0)
        )

        df_final = df_final.filter(
            col("NbBikes") + col("NbEmptyDocks") == col("NbDocks")
        )

        # Dimensão estação
        dim_station = df_final.select("id", "commonName")

        dim_station = dim_station.withColumnRenamed("id", "station_id") \
        .withColumnRenamed("commonName", "station_name")

        dim_station = dim_station.dropDuplicates(["station_id"])

        logger_transform.info("Dimensão dim_station criada")

        # Dimensão tempo
        dim_time = df_final.select("modified")

        dim_time = dim_time.withColumn(
            "modified",
            date_trunc("hour", col("modified"))
        )

        dim_time = dim_time.drop_duplicates()

        dim_time = (
            dim_time
            .withColumn("date", to_date(col("modified")))
            .withColumn("time_id", date_format(col("modified"), "yyyyMMddHH").cast("int"))
            .withColumn("year", year(col("date")))
            .withColumn("month", month(col("date")))
            .withColumn("day", day(col("date")))
            .withColumn("hour", hour(col("modified")))
            .drop("modified")
        )

        logger_transform.info("Dimensão dim_time criada")

        # Fato status das bicicletas
        fact_bike_status = df_final.select(
            "id",
            "NbBikes",
            "NbEmptyDocks",
            "NbDocks",
            "NbStandardBikes",
            "NbEBikes",
            "modified"
        )

        fact_bike_status = fact_bike_status\
        .withColumnRenamed("NbBikes", "bikes") \
        .withColumnRenamed("NbEmptyDocks", "empty_docks") \
        .withColumnRenamed("NbDocks", "total_docks") \
        .withColumnRenamed("NbEBikes", "ebikes") \
        .withColumnRenamed("NbStandardBikes", "standard_bikes") \
        .withColumnRenamed("id", "station_id") \
        .withColumn("modified", date_trunc("hour", col("modified"))) \
        .withColumn("time_id", date_format(col("modified"), "yyyyMMddHH").cast("int"))\
        .drop("modified")

        fact_bike_status = fact_bike_status.dropDuplicates(["station_id", "time_id"])

        fact_bike_status = fact_bike_status.orderBy("time_id")

        logger_transform.info("Fato fact_bike_status criado")

        logger_transform.info("Transformação BIKEPOINT finalizada com sucesso")

        return dim_station, dim_time, fact_bike_status

    except Exception as e:
        logger_transform.error(f"Erro na transformação BIKEPOINT: {e}")
        raise

def transform_arrivals(df: DataFrame)-> tuple[DataFrame, DataFrame, DataFrame, DataFrame, DataFrame]:

    try:

        logger_transform.info("Iniciando transformação: ARRIVALS")

        df = df.select(
            "id",
            "naptanId",
            "timeToStation",
            "vehicleId",
            "lineId",
            "lineName",
            "modeName",
            "stationName",
            "platformName",
            "direction",
            "expectedArrival"
        )

        logger_transform.info("Colunas necessárias selecionadas")

        dim_vehicle = df.select("vehicleId")

        dim_vehicle = dim_vehicle.withColumnRenamed("vehicleId", "vehicle_id")

        dim_line = df.select("lineId", "lineName", "modeName")

        dim_line = dim_line \
        .withColumnRenamed("lineId", "line_id") \
        .withColumnRenamed("lineName", "line_name") \
        .withColumnRenamed("modeName", "mode") 


        dim_station = df.select("naptanId", "stationName")

        dim_station = dim_station \
        .withColumnRenamed("naptanId", "station_id") \
        .withColumnRenamed("stationName", "station_name")


        dim_time = df.select("expectedArrival")

        dim_time = (
            dim_time
            .withColumn("expectedArrival", date_trunc("hour", col("expectedArrival")))
            .withColumn("time_id", date_format(col("expectedArrival"), "yyyyMMddHH").cast("int"))
            .withColumn("year", year(col("expectedArrival")))
            .withColumn("month", month(col("expectedArrival")))
            .withColumn("day", day(col("expectedArrival")))
            .withColumn("hour", hour(col("expectedArrival")))
            .withColumn("date", to_date(col("expectedArrival")))
            .drop("expectedArrival")
        )

        fact_arrival = df.select(
            "id",
            "naptanId",
            "vehicleId",
            "timeToStation",
            "lineId",
            "expectedArrival"
        )

        fact_arrival = fact_arrival\
        .withColumnRenamed("id", "fact_arrival_id") \
        .withColumnRenamed("naptanId", "station_id") \
        .withColumnRenamed("vehicleId", "vehicle_id") \
        .withColumnRenamed("timeToStation", "time_to_station") \
        .withColumnRenamed("lineId", "line_id") \
        .withColumn("expectedArrival", date_trunc("hour", col("expectedArrival"))) \
        .withColumn("time_id", date_format(col("expectedArrival"), "yyyyMMddHH").cast("int")) \
        .drop("expectedArrival")
        

        logger_transform.info("Estruturas de dimensão e fato criadas")

        # limpeza
        dim_vehicle = dim_vehicle.dropna(subset=["vehicle_id"])
        dim_vehicle = dim_vehicle.drop_duplicates(subset=["vehicle_id"])
        dim_line = dim_line.drop_duplicates(["line_id"])
        dim_station = dim_station.drop_duplicates(["station_id"])
        fact_arrival = fact_arrival.drop_duplicates(["fact_arrival_id"])
        dim_time = dim_time.drop_duplicates(["time_id"])

        logger_transform.info("Dados limpos e deduplicados")

        # cast
        fact_arrival = fact_arrival.withColumn(
            "fact_arrival_id",
            col("fact_arrival_id").cast("bigint")
        )

        logger_transform.info("Cast de tipos aplicado")

        logger_transform.info("Dimensão tempo criada")

        logger_transform.info("Transformação ARRIVALS finalizada")

        return dim_vehicle, dim_line, dim_station, dim_time, fact_arrival

    except Exception as e:
        logger_transform.error(f"Erro na transformação ARRIVALS: {e}")
        raise

def transform_status(df: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:

    try:
        logger_transform.info("Iniciando transformação: TUBESTATUS")

        # 🔹 Explode inicial
        df_exploded = df.select(
            "name",
            "modeName",
            explode("lineStatuses").alias("prop")
        )

        # 🔹 Explode de status + validityPeriods
        df_status = df_exploded.select(
            "name",
            "modeName",
            col("prop.lineId").alias("lineId"),
            col("prop.statusSeverityDescription").alias("status"),
            col("prop.reason").alias("reason"),
            explode("prop.validityPeriods").alias("time")
        )

        # 🔹 Dataset final base
        df_final = df_status.select(
            "name",
            "modeName",
            "lineId",
            "status",
            "reason",
            col("time.fromDate").alias("start_time"),
            col("time.toDate").alias("end_time")
        ).dropna(subset=["lineId"])

        logger_transform.info("Selecionadas colunas necessárias")

        # =========================================================
        # 🟢 DIM_LINE
        # =========================================================
        dim_line = (
            df_final
            .select("lineId", "name", "modeName")
            .dropDuplicates(["lineId"])
            .withColumnRenamed("lineId", "line_id")
            .withColumnRenamed("name", "line_name")
            .withColumnRenamed("modeName", "mode")
        )

        logger_transform.info("Dimensão linha criada")

        # =========================================================
        # 🟢 DIM_TIME (ÚNICA)
        # =========================================================
        dim_time = (
            df_final.select(col("start_time").alias("time"))
            .union(df_final.select(col("end_time").alias("time")))
            .dropna()
        )

        dim_time = (
            dim_time
            .withColumn("time", date_trunc("hour", col("time")))
            .withColumn("time_id", date_format(col("time"), "yyyyMMddHH").cast("int"))
            .withColumn("year", year(col("time")))
            .withColumn("month", month(col("time")))
            .withColumn("day", day(col("time")))
            .withColumn("hour", hour(col("time")))
            .withColumn("date", to_date(col("time")))
            .drop("time")
            .dropDuplicates(["time_id"])
        )

        logger_transform.info("Dimensão tempo criada")

        # =========================================================
        # 🟢 FACT_TUBE_STATUS
        # =========================================================
        fact_tube_status = (
            df_final
            .select("lineId", "status", "reason", "start_time", "end_time")
            .withColumnRenamed("lineId", "line_id")
            .withColumn("start_time", date_trunc("hour", col("start_time")))
            .withColumn("end_time", date_trunc("hour", col("end_time")))
            .withColumn("start_time_id", date_format(col("start_time"), "yyyyMMddHH").cast("int"))
            .withColumn("end_time_id", date_format(col("end_time"), "yyyyMMddHH").cast("int"))
            .drop("start_time", "end_time")
        )

        # 🔹 Remover duplicados controlados
        fact_tube_status = fact_tube_status.dropDuplicates(
            ["line_id", "start_time_id", "end_time_id", "status"]
        )

        # 🔹 ID da fato (hash mais robusto)
        fact_tube_status = fact_tube_status.withColumn(
            "fact_tube_status_id",
            sha2(
                concat_ws(
                    "||",
                    col("line_id"),
                    col("start_time_id"),
                    col("end_time_id"),
                    col("status")
                ),
                256
            )
        )

        logger_transform.info("Fato fact_tube_status criada")
        logger_transform.info("Transformação TUBESTATUS finalizada")

        return dim_line, dim_time, fact_tube_status

    except Exception as e:
        logger_transform.error(f"Erro na transformação TUBESTATUS: {e}")
        raise

    logger_transform.info("Iniciando processo de carga de dados")

    names_map = {
        "arrivals": [
            "dim_vehicle",
            "dim_line",
            "dim_station",
            "dim_time",
            "fact_arrival"
        ],
        "bikepoint": [
            "dim_station",
            "dim_time",
            "fact_bike"
        ],
        "tubestatus": [
            "dim_line",
            "dim_time",
            "fact_status"
        ]
    }

    for nome, dfs in data.items():
        load_refined(dfs, nome, names_map[nome], spark)

    logger_transform.info("Finalizando processo de carga de dados")

def run_transform(spark: SparkSession):

    try:
        logger_transform.info("Pipeline de transformação iniciado")

        bikepoint_df = read_data(spark, "s3a://tfl-port/raw/bikepoint/")
        bikepoint_tables = transform_bikepoint(bikepoint_df)

        tubestatus_df = read_data(spark, "s3a://tfl-port/raw/tubestatus/")
        tubestatus_tables = transform_status(tubestatus_df)

        arrivals_df = read_data(spark, "s3a://tfl-port/raw/arrivals/")
        arrivals_tables = transform_arrivals(arrivals_df)

        logger_transform.info("Pipeline de transformação finalizado com sucesso")

        return {
            "arrivals": arrivals_tables,
            "bikepoint": bikepoint_tables,
            "tubestatus": tubestatus_tables
        }

    except Exception as e:
        logger_transform.error(f"Falha no pipeline de transformação: {e}")
        raise