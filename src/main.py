from src.extract.extract import run_extract
from src.transform.transform import run_transform
from src.load.load import run_load
import logging
from pyspark.sql import SparkSession


def run_pipeline():

    spark = SparkSession.builder.appName("spark").getOrCreate()
    run_extract()
    data = run_transform(spark)
    run_load(data, spark)


if __name__ == "__main__":
    run_pipeline()