"""
Sesión 1 - Lab 02: ETL Batch con AWS Glue
------------------------------------------
Lee el CSV crudo catalogado por el Crawler, aplica control de calidad y
transformación de tipos, y escribe el resultado en Parquet particionado.

IMPORTANTE: este script NO registra la tabla en el Data Catalog.
Escribir archivos en S3 y catalogarlos son dos cosas distintas.
Por eso el Lab 02 necesita un segundo Crawler al final.

Uso: pegar en Glue Studio -> Script editor (motor Spark) y ejecutar.
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_date

# ---------------------------------------------------------------------------
# CONFIGURACIÓN - cambia BUCKET por el nombre de tu bucket
# ---------------------------------------------------------------------------
BUCKET = "bsg-glue-lab-TUSINICIALES"
DATABASE = "demo_batch_db"
TABLA_RAW = "transacciones"
RUTA_CURATED = f"s3://{BUCKET}/curated/transacciones/"

# ---------------------------------------------------------------------------
# Inicialización del contexto de Glue
# ---------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Logger de Glue en vez de print(): aparece en vivo mientras el job corre.
# Los print() se quedan bufereados y suelen salir todos juntos al final.
# El prefijo [ETL] permite filtrar nuestros mensajes entre los miles de
# INFO que emite Spark. Todo esto va al log group /aws-glue/jobs/error.
logger = glueContext.get_logger()

# ---------------------------------------------------------------------------
# 1. EXTRACT - leer desde el Data Catalog, no desde la ruta S3
#    Leemos por nombre de tabla: si mañana el CSV cambia de ruta, el script
#    sigue funcionando sin tocarlo.
# ---------------------------------------------------------------------------
dyf = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE,
    table_name=TABLA_RAW,
)
df = dyf.toDF()

logger.info("[ETL] Filas leídas desde raw: %s" % df.count())
logger.info("[ETL] Esquema de origen: %s" % df.schema.simpleString())

# ---------------------------------------------------------------------------
# 2. TRANSFORM
# ---------------------------------------------------------------------------
# Control de calidad: los montos negativos son devoluciones mal registradas
# y no deben llegar a la capa curated.
df_clean = df.filter(col("amount") > 0)

# Transformación de tipos: el Crawler infirió transaction_date como string
# porque en un CSV todo es texto. Aquí lo convertimos a date de verdad.
df_clean = df_clean.withColumn(
    "transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd")
)

logger.info("[ETL] Filas después del control de calidad: %s" % df_clean.count())

# ---------------------------------------------------------------------------
# 3. LOAD - Parquet particionado por fecha
#    partitionBy crea una carpeta por cada valor distinto de la columna.
#    Athena luego puede leer solo las carpetas que necesita.
# ---------------------------------------------------------------------------
df_clean.write.mode("overwrite").partitionBy("transaction_date").parquet(RUTA_CURATED)

job.commit()
