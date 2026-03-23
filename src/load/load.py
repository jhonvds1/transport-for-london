import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, concat_ws
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

load_logger = logging.getLogger("LOAD")


def save_data(dfs: tuple, name: str, names: list, spark: SparkSession):
    load_logger.info(f"Iniciando load de {name}")
    base_path = "data/refined"

    for name_df, df in zip(names, dfs):

        path = f"{base_path}/{name}/{name_df}"
        path_obj = Path(path)

        # 🔥 1. Criar ID único
        df = df.withColumn(
            "id",
            sha2(concat_ws("||", *df.columns), 256)
        )

        # 🔥 2. Remover duplicados no batch
        df = df.dropDuplicates(["id"])

        total_batch = df.count()

        # 🔥 3. Verificar se já existe e aplicar anti-join
        if path_obj.exists():
            df_existing = spark.read.parquet(path)

            if "id" in df_existing.columns:
                df = df.join(df_existing.select("id"), on="id", how="left_anti")
            else:
                load_logger.warning(f"{name_df} sem coluna id antiga, ignorando deduplicação")
        else:
            load_logger.info(f"Primeira carga para {name}/{name_df}")

        total_novos = df.count()

        # 🔥 4. Salvar
        (
            df.write.mode("append")
            .option("compression", "snappy")
            .parquet(path)
        )

        load_logger.info(
            f"{name}/{name_df} → Batch: {total_batch} | Novos inseridos: {total_novos}"
        )

    load_logger.info(f"Finalizando load de {name}")


def run_load(data: dict, spark: SparkSession):
    load_logger.info("Iniciando processo de carga de dados")

    spark = SparkSession.builder.appName("spark").getOrCreate()

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
        save_data(dfs, nome, names_map[nome], spark)

    load_logger.info("Finalizando processo de carga de dados")