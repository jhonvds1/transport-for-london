# load_s3_glue.py
import logging
from pyspark.sql.functions import sha2, concat_ws
from typing import Tuple

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
def save_data(spark, dfs: Tuple, dataset_name: str, names: list):
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
def run_load(spark, data: dict):
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
        save_data(spark, dfs, dataset_name, names_map[dataset_name])

    load_logger.info("Finalizando processo de carga de dados")