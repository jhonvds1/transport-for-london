import sys
import logging

# libs do Glue (runtime gerenciado com Spark)
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# módulos do pipeline (empacotados via zip no S3)
from src.extract.extract import run_extract
from src.transform.transform import run_transform
from src.load.load_s3 import run_load


# configuração de logging (CloudWatch)
# nível INFO é suficiente pra acompanhar execução sem poluir
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# parâmetros do job (JOB_NAME é obrigatório no Glue)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])


# inicialização do contexto Spark/Glue
# SparkSession já é gerenciada pelo Glue (não criar manualmente)
logger.info("Inicializando contexto Spark/Glue")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


# inicialização do job (necessário para controle de execução e commit)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


try:
    # =========================
    # EXTRAÇÃO (INGESTÃO)
    # =========================
    logger.info("Iniciando etapa de extração")

    # responsável por ingestão de dados (ex: API, raw S3)
    # idealmente escreve em camada raw
    run_extract()

    logger.info("Extração concluída")


    # =========================
    # TRANSFORMAÇÃO
    # =========================
    logger.info("Iniciando etapa de transformação")

    # transforma dados usando Spark (limpeza, schema, regras de negócio)
    # retorna DataFrame pronto para persistência
    data = run_transform(spark)

    logger.info("Transformação concluída")


    # =========================
    # LOAD (PERSISTÊNCIA)
    # =========================
    logger.info("Iniciando etapa de carga")

    # persiste dados transformados (ex: S3 trusted/refined, parquet)
    run_load(data, spark)

    logger.info("Carga concluída")


    # finalização do job (marca sucesso no Glue)
    logger.info("Finalizando job com sucesso")
    job.commit()


except Exception as e:
    # captura qualquer falha no pipeline
    # exc_info=True inclui stacktrace completo no CloudWatch
    logger.error("Falha na execução do pipeline", exc_info=True)

    # propaga erro para o Glue marcar execução como FAILED
    raise