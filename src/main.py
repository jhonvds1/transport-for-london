from src.extract.extract import run_extract
from src.transform.transform import run_transform
from src.load.load_s3 import run_load
from pyspark.sql import SparkSession


def run_pipeline():

    spark = SparkSession.builder \
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    ) \
    .getOrCreate()

    run_extract()
    data = run_transform(spark)
    run_load(data, spark)


if __name__ == "__main__":
    run_pipeline()