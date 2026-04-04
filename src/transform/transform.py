import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, explode, first, lit,
    month, year, day, hour,
    date_trunc, to_date, date_format,
    sha2, concat_ws
)
from pyspark.sql.utils import AnalysisException

# ============================
# Recebe argumentos do Glue
# ============================
# Glue passa parâmetros para o job, como JOB_NAME
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# ============================
# Inicializa contextos do Glue
# ============================
# SparkContext e GlueContext são obrigatórios no Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session  # SparkSession já disponível
job = Job(glueContext)  # Job do Glue

# ============================
# Configuração de logs
# ============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger_transform = logging.getLogger("TRANSFORM")

# ============================
# Função para ler dados do S3
# ============================
def read_data(spark, path: str) -> DataFrame | None:
    """
    Lê arquivos JSON de uma pasta no S3 e retorna DataFrame.
    Retorna None se a pasta estiver vazia ou houver erro.
    """
    try:
        df = spark.read.option("multiline", "true").json(path)
        if df.rdd.isEmpty():
            logger_transform.warning(f"Pasta vazia: {path}")
            return None
        return df
    except AnalysisException:
        logger_transform.warning(f"Erro ao ler pasta: {path}")
        return None

# ============================
# Transformação BIKEPOINT
# ============================
def transform_bikepoint(df: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
    """
    Transforma dados de bikepoint em dimensões e fato:
    - dim_station
    - dim_time
    - fact_bike_status
    """
    try:
        logger_transform.info("Iniciando transformação: BIKEPOINT")

        # Explode propriedades aninhadas
        df_exploded = df.select("id", "commonName", explode("additionalProperties").alias("prop"))

        # Seleciona e normaliza colunas
        df_bike = df_exploded.select("id", "commonName", col("prop.key"), col("prop.value"), col("prop.modified"))

        # Pivot para transformar chaves em colunas
        df_final = (
            df_bike
            .groupBy("id", "commonName", "modified")
            .pivot("key", ["TerminalName", "NbBikes", "NbEmptyDocks", "NbDocks", "NbStandardBikes", "NbEBikes"])
            .agg(first("value"))
        )

        df_final = df_final.withColumn("mode", lit("bike"))

        # Remove registros com colunas obrigatórias nulas
        required_columns = ['id', 'commonName', 'TerminalName', 'NbBikes', 'NbEmptyDocks', 'NbDocks',
                            'NbStandardBikes', 'NbEBikes', 'mode', 'modified']
        df_final = df_final.dropna(subset=required_columns)

        # Remove duplicatas
        df_final = df_final.drop_duplicates(subset=["id", "modified"])

        # Cast de colunas numéricas
        df_final = (
            df_final
            .withColumn("NbBikes", col("NbBikes").cast("int"))
            .withColumn("NbEmptyDocks", col("NbEmptyDocks").cast("int"))
            .withColumn("NbDocks", col("NbDocks").cast("int"))
            .withColumn("NbStandardBikes", col("NbStandardBikes").cast("int"))
            .withColumn("NbEBikes", col("NbEBikes").cast("int"))
        )

        # Validações de regra de negócio
        df_final = df_final.filter(
            (col("NbEmptyDocks") >= 0) &
            (col("NbDocks") >= 0) &
            (col("NbStandardBikes") >= 0) &
            (col("NbEBikes") >= 0)
        )
        df_final = df_final.filter(col("NbBikes") + col("NbEmptyDocks") == col("NbDocks"))

        # Dimensão estação
        dim_station = df_final.select("id", "commonName") \
            .withColumnRenamed("id", "station_id") \
            .withColumnRenamed("commonName", "station_name") \
            .dropDuplicates(["station_id"])
        logger_transform.info("Dimensão dim_station criada")

        # Dimensão tempo
        dim_time = df_final.select("modified") \
            .withColumn("modified", date_trunc("hour", col("modified"))) \
            .drop_duplicates() \
            .withColumn("date", to_date(col("modified"))) \
            .withColumn("time_id", date_format(col("modified"), "yyyyMMddHH").cast("int")) \
            .withColumn("year", year(col("date"))) \
            .withColumn("month", month(col("date"))) \
            .withColumn("day", day(col("date"))) \
            .withColumn("hour", hour(col("modified"))) \
            .drop("modified")
        logger_transform.info("Dimensão dim_time criada")

        # Fato status das bicicletas
        fact_bike_status = df_final.select("id", "NbBikes", "NbEmptyDocks", "NbDocks", "NbStandardBikes", "NbEBikes", "modified") \
            .withColumnRenamed("NbBikes", "bikes") \
            .withColumnRenamed("NbEmptyDocks", "empty_docks") \
            .withColumnRenamed("NbDocks", "total_docks") \
            .withColumnRenamed("NbEBikes", "ebikes") \
            .withColumnRenamed("NbStandardBikes", "standard_bikes") \
            .withColumnRenamed("id", "station_id") \
            .withColumn("modified", date_trunc("hour", col("modified"))) \
            .withColumn("time_id", date_format(col("modified"), "yyyyMMddHH").cast("int")) \
            .drop("modified") \
            .dropDuplicates(["station_id", "time_id"]) \
            .orderBy("time_id")
        logger_transform.info("Fato fact_bike_status criado")

        logger_transform.info("Transformação BIKEPOINT finalizada com sucesso")
        return dim_station, dim_time, fact_bike_status

    except Exception as e:
        logger_transform.error(f"Erro na transformação BIKEPOINT: {e}")
        raise

# ============================
# Transformação ARRIVALS
# ============================
def transform_arrivals(df: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame, DataFrame]:
    """
    Transforma dados de arrivals em dimensões e fato:
    - dim_vehicle
    - dim_line
    - dim_station
    - dim_time
    - fact_arrival
    """
    try:
        logger_transform.info("Iniciando transformação: ARRIVALS")

        df = df.select(
            "id", "naptanId", "timeToStation", "vehicleId", "lineId",
            "lineName", "modeName", "stationName", "platformName",
            "direction", "expectedArrival"
        )

        # Dimensões
        dim_vehicle = df.select("vehicleId").withColumnRenamed("vehicleId", "vehicle_id").dropna().drop_duplicates(["vehicle_id"])
        dim_line = df.select("lineId", "lineName", "modeName").withColumnRenamed("lineId", "line_id") \
            .withColumnRenamed("lineName", "line_name").withColumnRenamed("modeName", "mode") \
            .drop_duplicates(["line_id"])
        dim_station = df.select("naptanId", "stationName").withColumnRenamed("naptanId", "station_id") \
            .withColumnRenamed("stationName", "station_name").drop_duplicates(["station_id"])
        dim_time = df.select("expectedArrival") \
            .withColumn("expectedArrival", date_trunc("hour", col("expectedArrival"))) \
            .withColumn("time_id", date_format(col("expectedArrival"), "yyyyMMddHH").cast("int")) \
            .withColumn("year", year(col("expectedArrival"))) \
            .withColumn("month", month(col("expectedArrival"))) \
            .withColumn("day", day(col("expectedArrival"))) \
            .withColumn("hour", hour(col("expectedArrival"))) \
            .withColumn("date", to_date(col("expectedArrival"))) \
            .drop("expectedArrival").drop_duplicates(["time_id"])

        # Fato arrivals
        fact_arrival = df.select("id", "naptanId", "vehicleId", "timeToStation", "lineId", "expectedArrival") \
            .withColumnRenamed("id", "fact_arrival_id") \
            .withColumnRenamed("naptanId", "station_id") \
            .withColumnRenamed("vehicleId", "vehicle_id") \
            .withColumnRenamed("timeToStation", "time_to_station") \
            .withColumnRenamed("lineId", "line_id") \
            .withColumn("expectedArrival", date_trunc("hour", col("expectedArrival"))) \
            .withColumn("time_id", date_format(col("expectedArrival"), "yyyyMMddHH").cast("int")) \
            .drop("expectedArrival") \
            .drop_duplicates(["fact_arrival_id"])

        logger_transform.info("Transformação ARRIVALS finalizada")
        return dim_vehicle, dim_line, dim_station, dim_time, fact_arrival

    except Exception as e:
        logger_transform.error(f"Erro na transformação ARRIVALS: {e}")
        raise

# ============================
# Transformação TUBESTATUS
# ============================
def transform_status(df: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
    """
    Transforma dados de tubestatus em dimensões e fato:
    - dim_line
    - dim_time
    - fact_tube_status
    """
    try:
        logger_transform.info("Iniciando transformação: TUBESTATUS")

        # Explode lineStatuses
        df_exploded = df.select("name", "modeName", explode("lineStatuses").alias("prop"))
        df_status = df_exploded.select(
            "name", "modeName",
            col("prop.lineId").alias("lineId"),
            col("prop.statusSeverityDescription").alias("status"),
            col("prop.reason").alias("reason"),
            explode("prop.validityPeriods").alias("time")
        )

        # Dataset final base
        df_final = df_status.select(
            "name", "modeName", "lineId", "status", "reason",
            col("time.fromDate").alias("start_time"),
            col("time.toDate").alias("end_time")
        ).dropna(subset=["lineId"])

        # Dimensão linha
        dim_line = df_final.select("lineId", "name", "modeName").drop_duplicates(["lineId"]) \
            .withColumnRenamed("lineId", "line_id") \
            .withColumnRenamed("name", "line_name") \
            .withColumnRenamed("modeName", "mode")

        # Dimensão tempo
        dim_time = df_final.select(col("start_time").alias("time")).union(df_final.select(col("end_time").alias("time"))) \
            .dropna() \
            .withColumn("time", date_trunc("hour", col("time"))) \
            .withColumn("time_id", date_format(col("time"), "yyyyMMddHH").cast("int")) \
            .withColumn("year", year(col("time"))) \
            .withColumn("month", month(col("time"))) \
            .withColumn("day", day(col("time"))) \
            .withColumn("hour", hour(col("time"))) \
            .withColumn("date", to_date(col("time"))) \
            .drop("time").drop_duplicates(["time_id"])

        # Fato status
        fact_tube_status = df_final.select("lineId", "status", "reason", "start_time", "end_time") \
            .withColumnRenamed("lineId", "line_id") \
            .withColumn("start_time", date_trunc("hour", col("start_time"))) \
            .withColumn("end_time", date_trunc("hour", col("end_time"))) \
            .withColumn("start_time_id", date_format(col("start_time"), "yyyyMMddHH").cast("int")) \
            .withColumn("end_time_id", date_format(col("end_time"), "yyyyMMddHH").cast("int")) \
            .drop("start_time", "end_time") \
            .drop_duplicates(["line_id", "start_time_id", "end_time_id", "status"]) \
            .withColumn("fact_tube_status_id",
                        sha2(concat_ws("||", col("line_id"), col("start_time_id"), col("end_time_id"), col("status")), 256))

        logger_transform.info("Transformação TUBESTATUS finalizada")
        return dim_line, dim_time, fact_tube_status

    except Exception as e:
        logger_transform.error(f"Erro na transformação TUBESTATUS: {e}")
        raise

# ============================
# Função principal para rodar pipeline
# ============================
def run_transform():
    """
    Executa pipeline completo de transformação usando SparkSession do Glue
    e retorna um dicionário com DataFrames transformados.
    """
    try:
        logger_transform.info("Pipeline de transformação iniciado")

        # SparkSession do Glue
        spark = glueContext.spark_session

        # BIKEPOINT
        bikepoint_df = read_data(spark, "s3://tfl-port/raw/bikepoint/")
        bikepoint_tables = transform_bikepoint(bikepoint_df) if bikepoint_df else None

        # TUBESTATUS
        tubestatus_df = read_data(spark, "s3://tfl-port/raw/tubestatus/")
        tubestatus_tables = transform_status(tubestatus_df) if tubestatus_df else None

        # ARRIVALS
        arrivals_df = read_data(spark, "s3://tfl-port/raw/arrivals/")
        arrivals_tables = transform_arrivals(arrivals_df) if arrivals_df else None

        logger_transform.info("Pipeline de transformação finalizado com sucesso")

        return {
            "arrivals": arrivals_tables,
            "bikepoint": bikepoint_tables,
            "tubestatus": tubestatus_tables
        }

    except Exception as e:
        logger_transform.error(f"Falha no pipeline de transformação: {e}")
        raise
    """
    Executa pipeline completo de transformação e retorna dicionário com DataFrames.
    """
    try:
        logger_transform.info("Pipeline de transformação iniciado")

        bikepoint_df = read_data(spark, "s3://tfl-port/raw/bikepoint/")
        bikepoint_tables = transform_bikepoint(bikepoint_df) if bikepoint_df else None

        tubestatus_df = read_data(spark, "s3://tfl-port/raw/tubestatus/")
        tubestatus_tables = transform_status(tubestatus_df) if tubestatus_df else None

        arrivals_df = read_data(spark, "s3://tfl-port/raw/arrivals/")
        arrivals_tables = transform_arrivals(arrivals_df) if arrivals_df else None

        logger_transform.info("Pipeline de transformação finalizado com sucesso")

        return {
            "arrivals": arrivals_tables,
            "bikepoint": bikepoint_tables,
            "tubestatus": tubestatus_tables
        }

    except Exception as e:
        logger_transform.error(f"Falha no pipeline de transformação: {e}")
        raise

# ============================
# Inicializa e comita o job
# ============================
job.init(args['JOB_NAME'], args)

transformed_data = run_transform()

job.commit()