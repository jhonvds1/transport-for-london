import requests
import json
from pathlib import Path
import logging
from datetime import datetime
import boto3


logging.basicConfig(
    level=logging.INFO,
    format= "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger_extract = logging.getLogger("EXTRACT")

def fetch_and_save(url: str, path: Path) -> int:
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    data = response.json()

    s3 = boto3.client("s3")

    s3.put_object(
        Bucket = "tfl-port",
        Key = str(path),
        Body = json.dumps(data, indent = 4),
        ContentType = "application/json"
    )

    return len(data)


def get_bikepoints() -> None:
    try:
        logger_extract.info("Iniciando extração BikePoint")
       
        url = "https://api.tfl.gov.uk/BikePoint"

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        path = Path(f"raw/bikepoint/bikepoint_{timestamp}.json")

        saved = fetch_and_save(url, path)
        
        logger_extract.info(f"{saved} registros salvos em {path}")

        logger_extract.info("Finalizando processo de extracao de dados da api de bikepoint")
    except Exception as e:
        logger_extract.error(f"Erro BikePoint: {e}")
        raise

def get_line_status() -> None:
    logger_extract.info("Iniciando extracao de status da linha")
    try:
        url = f"https://api.tfl.gov.uk/Line/Mode/tube/Status"

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        path = Path(f"raw/tubestatus/tubestatus_{timestamp}.json")

        saved = fetch_and_save(url, path)

        logger_extract.info(f"{saved} registros salvos em {path}")

        logger_extract.info("Finalizando extracao de status da linha")

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
    
    for id in stop_ids:
        
        logger_extract.info(f"Iniciando extracao chegada de: {id}")

        try:
            url = f"https://api.tfl.gov.uk/StopPoint/{id}/Arrivals"

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            path = Path(f"raw/arrivals/{id}/arrivals_{timestamp}.json")

            saved = fetch_and_save(url, path)
            
            logger_extract.info(f"{saved} registros salvos em {path}")

            logger_extract.info(f"Finalizando extracao chegada de: {id}")

        except Exception as e:
            logger_extract.error(f"Erro dados de chegada: {e} em {id}")


def run_extract() -> None:
    logger_extract.info("Iniciando extracao de dados")
    get_bikepoints()
    get_line_status()
    get_yellow_messages()
    logger_extract.info("Finalizando extracao de dados")







