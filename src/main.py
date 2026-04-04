import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from src.extract.extract import run_extract
from src.transform.transform import run_transform
from src.load.load_s3 import run_load

# 🔧 configuração de log
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# args do Glue
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

logger.info("Inicializando contexto do Glue")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

try:
    logger.info("Iniciando etapa de EXTRAÇÃO")
    run_extract()
    logger.info("Extração finalizada com sucesso")

    logger.info("Iniciando etapa de TRANSFORMAÇÃO")
    data = run_transform(spark)
    logger.info("Transformação finalizada com sucesso")

    logger.info("Iniciando etapa de LOAD")
    run_load(data, spark)
    logger.info("Carga finalizada com sucesso")

    logger.info("Finalizando job")
    job.commit()

except Exception as e:
    logger.error(f"Erro no pipeline: {str(e)}", exc_info=True)
    raise