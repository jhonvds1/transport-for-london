# load_s3_glue.py
import sys
import logging
from pyspark.sql.functions import sha2, concat_ws
from typing import Tuple

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# =========================
# Recebe argumentos do Glue (JOB_NAME é obrigatório)
# =========================
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# =========================
# Inicializa contexto Spark/Glue
# =========================
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# =========================
# Logger
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
load_logger = logging.getLogger("LOAD")

# =========================
# Função auxiliar para salvar DataFrames
# =========================
def save_data(dfs: Tuple, dataset_name: str, names: list):
    """
    Salva uma tupla de DataFrames no S3 em refined/<dataset_name>/<table_name>.
    Cria ID hash, remove duplicados e aplica anti-join com dados existentes.
    """
    load_logger.info(f"Iniciando load de {dataset_name}")
    base_path = "s3a://tfl-port/refined"

    for name_df, df in zip(names, dfs):
        if df is None:
            load_logger.warning(f"{name_df} é None, pulando...")
            continue

        path = f"{base_path}/{dataset_name}/{name_df}"

        # 1️⃣ Criar ID hash
        df = df.withColumn("id", sha2(concat_ws("||", *df.columns), 256))

        # 2️⃣ Remover duplicados no batch atual
        df = df.dropDuplicates(["id"])
        total_batch = df.count()

        # 3️⃣ Anti-join com dados existentes
        try:
            df_existing = spark.read.parquet(path)
            if "id" in df_existing.columns:
                df = df.join(df_existing.select("id"), on="id", how="left_anti")
            else:
                load_logger.warning(f"{name_df} sem coluna 'id' antiga, ignorando deduplicação")
        except Exception:
            load_logger.info(f"Primeira carga para {dataset_name}/{name_df}")

        total_novos = df.count()

        # 4️⃣ Salvar no S3
        (
            df.write
            .mode("append")
            .option("compression", "snappy")
            .parquet(path)
        )

        load_logger.info(
            f"{dataset_name}/{name_df} → Batch: {total_batch} | Novos inseridos: {total_novos}"
        )

    load_logger.info(f"Finalizando load de {dataset_name}")

# =========================
# Função principal de load
# =========================
def run_load(data: dict):
    """
    Recebe dict de DataFrames transformados e salva no S3 refined.
    data exemplo:
    {
        'arrivals': (dim_vehicle, dim_line, dim_station, dim_time, fact_arrival),
        'bikepoint': (dim_station, dim_time, fact_bike),
        'tubestatus': (dim_line, dim_time, fact_status)
    }
    """
    load_logger.info("Iniciando processo de carga de dados")

    names_map = {
        "arrivals": ["dim_vehicle", "dim_line", "dim_station", "dim_time", "fact_arrival"],
        "bikepoint": ["dim_station", "dim_time", "fact_bike"],
        "tubestatus": ["dim_line", "dim_time", "fact_status"]
    }

    for dataset_name, dfs in data.items():
        if dfs is None:
            load_logger.warning(f"{dataset_name} vazio, pulando...")
            continue
        save_data(dfs, dataset_name, names_map[dataset_name])

    load_logger.info("Finalizando processo de carga de dados")

# =========================
# Commit do Glue Job
# =========================
job.commit()