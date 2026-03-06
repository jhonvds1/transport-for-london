from src.extract.extract import run_extract
from src.transform.transform import run_transform
from src.load.load import run_load
import logging




def run_pipeline():
    run_extract()
    run_transform()
    run_load()


if __name__ == "__main__":
    run_pipeline()