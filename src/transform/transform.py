import logging
import pandas as pd
import logging
from pathlib import Path
import json


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger_transform = logging.getLogger("TRANSFORM")

def read_raw_data(folder: str) -> list:
    files = Path(folder).glob("*.json")

    data = []

    for file in files:
        with open(file) as f:
            data.extend(json.load(f))

    return data

def transform_list_df(data: list) -> pd.DataFrame:
    return pd.DataFrame(data)

def transform_bikepoint():
    ...

def transform_arrivals():
    ...

def transform_status():
    ...


def run_transform():
    data_list = read_raw_data("data/raw/bikepoint")
    data_df = transform_list_df(data_list)
    print(data_df.keys())

run_transform()