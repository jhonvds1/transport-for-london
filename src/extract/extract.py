import sys
import json
import logging
from datetime import datetime
from pathlib import Path

import requests
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# -------------------------------
# Configuração de logging
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger_extract = logging.getLogger("EXTRACT")

# -------------------------------
# Função para buscar dados da API e salvar no S3
# -------------------------------
def fetch_and_save(url: str, bucket: str, key: str) -> int:
    """
    Faz requisição GET em uma URL, salva resultado no S3 e retorna quantidade de registros.
    """
    response = requests.get(url, timeout=30)
    response.raise_for_status()  # Lança erro se status != 200

    data = response.json()

    if not data:
        logger_extract.warning(f"Nenhum dado retornado para {url}, não salvando.")
        return 0

    s3 = boto3.client("s3")

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data, indent=4),
        ContentType="application/json"
    )

    return len(data)

# -------------------------------
# Funções de extração
# -------------------------------
def get_bikepoints() -> None:
    try:
        logger_extract.info("Iniciando extração BikePoint")
        url = "https://api.tfl.gov.uk/BikePoint"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        bucket = "tfl-port"
        key = f"raw/bikepoint/bikepoint_{timestamp}.json"
        path = f"s3://{bucket}/{key}"  # Apenas para log

        saved = fetch_and_save(url, bucket, key)
        logger_extract.info(f"{saved} registros salvos em {path}")
        logger_extract.info("Finalizando processo de extração de BikePoint")
    except Exception as e:
        logger_extract.error(f"Erro BikePoint: {e}")
        raise

def get_line_status() -> None:
    try:
        logger_extract.info("Iniciando extração de status da linha")
        url = "https://api.tfl.gov.uk/Line/Mode/tube/Status"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        bucket = "tfl-port"  # corrigido typo
        key = f"raw/tubestatus/tubestatus_{timestamp}.json"
        path = f"s3://{bucket}/{key}"  # Apenas para log

        saved = fetch_and_save(url, bucket, key)
        logger_extract.info(f"{saved} registros salvos em {path}")
        logger_extract.info("Finalizando extração de status da linha")
    except Exception as e:
        logger_extract.error(f"Erro status da linha: {e}")
        raise

def get_yellow_messages() -> None:
    stop_ids = [
        "490005183E",
        "490008660N",
        "490013767D",
        "490000254S"
    ]

    bucket = "tfl-port"

    for stop_id in stop_ids:
        try:
            logger_extract.info(f"Iniciando extração chegada de: {stop_id}")
            url = f"https://api.tfl.gov.uk/StopPoint/{stop_id}/Arrivals"
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            key = f"raw/arrivals/{stop_id}/arrivals_{timestamp}.json"
            path = f"s3://{bucket}/{key}"  # Apenas para log

            saved = fetch_and_save(url, bucket, key)
            logger_extract.info(f"{saved} registros salvos em {path}")
            logger_extract.info(f"Finalizando extração chegada de: {stop_id}")
        except Exception as e:
            logger_extract.error(f"Erro dados de chegada: {e} em {stop_id}")

# -------------------------------
# Função principal de extração
# -------------------------------
def run_extract() -> None:
    logger_extract.info("Iniciando extração de dados")
    get_bikepoints()
    get_line_status()
    get_yellow_messages()
    logger_extract.info("Finalizando extração de dados")
