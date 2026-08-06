# Sesión 1 — Demo: Pipeline ETL Batch con AWS Glue (S3 → Glue → Athena)

Corresponde a la "Actividad Práctica" de la diapositiva 17. Duración objetivo: ~55-65 min dentro del bloque de práctica de la sesión (incluye los dos escenarios de catalogación, ver nota de tiempo más abajo).

## Objetivo de la demo
Mostrar el flujo completo: CSV crudo en S3 → Crawler cataloga → Glue Job (PySpark) limpia y transforma → Parquet particionado en S3-Curated → consulta en Athena.

Además, esta demo muestra **dos formas válidas de catalogar la zona curated**, para que los estudiantes entiendan el mecanismo y la buena práctica de producción en la misma sesión:
- **Escenario A** — Segundo Crawler (más visual, ideal para enseñar qué hace un crawler).
- **Escenario B** — `enableUpdateCatalog` dentro del propio Job (lo que se usaría en producción real).

## Prerrequisitos (hacer 1-2 días antes, no en vivo)
1. Cuenta AWS con permisos de administrador o rol con acceso a S3, Glue, Athena, IAM, CloudWatch.
2. Región recomendada: `us-east-1` (mayor disponibilidad de features de Glue).
3. Bucket S3 exclusivo para la demo: `s3://bsg-demo-glue/` con esta estructura:
   ```
   raw/transacciones/
   curated/transacciones/       <- Escenario A (crawler)
   curated_v2/transacciones/    <- Escenario B (enableUpdateCatalog)
   scripts/
   temp/
   ```
4. Rol IAM para Glue: `AWSGlueServiceRole-Demo`
   - Trust policy: servicio `glue.amazonaws.com`
   - Políticas: `AWSGlueServiceRole` (managed) + una inline con `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` sobre tu bucket.

## Dataset de ejemplo
Crea `transacciones.csv` (a propósito con montos negativos y fechas en texto, para justificar la limpieza):

```csv
transaction_id,customer_id,amount,transaction_date
1001,C001,150.50,2025-01-05
1002,C002,-45.00,2025-01-05
1003,C003,320.75,2025-01-06
1004,C001,-12.30,2025-01-06
1005,C004,89.99,2025-01-07
1006,C002,210.00,2025-01-07
1007,C005,-5.50,2025-01-08
1008,C003,999.00,2025-01-08
1009,C004,45.25,2025-01-09
1010,C005,60.10,2025-01-09
```

Súbelo a `s3://bsg-demo-glue/raw/transacciones/transacciones.csv` (Consola S3 o `aws s3 cp`).

## Pasos en vivo

### 1. Glue Crawler sobre raw (Ingesta y catalogado — Bloque 2 del sílabo)
Este paso es común a ambos escenarios: la zona raw **siempre** se cataloga con crawler, porque es datos que no controlas (llegan de fuera).

- Glue Console → **Crawlers** → **Create crawler**
- Nombre: `crawler-transacciones-raw`
- Data source: S3, path `s3://bsg-demo-glue/raw/transacciones/`
- IAM role: `AWSGlueServiceRole-Demo`
- Target database: crea una nueva `demo_batch_db`
- Ejecuta el crawler → verifica en **Tables** que apareció `transacciones` con las columnas inferidas (muestra esto como el "Data Catalog" poblado automáticamente, conecta con diapositiva 8).

---

### 2. Escenario A — Glue Job + segundo Crawler

```python
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

dyf = glueContext.create_dynamic_frame.from_catalog(
    database="demo_batch_db", table_name="transacciones"
)
df = dyf.toDF()

# Control de calidad: filtrar montos negativos (Bloque 2)
df_clean = df.filter(col("amount") > 0)

# Transformación de tipos: string -> date (Bloque 1)
df_clean = df_clean.withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd"))

# Escritura particionada por fecha en formato Parquet (Bloque 4)
df_clean.write.mode("overwrite").partitionBy("transaction_date") \
    .parquet("s3://bsg-demo-glue/curated/transacciones/")

job.commit()
```

- Job name: `job-transacciones-escenario-a`. IAM role: `AWSGlueServiceRole-Demo`. Type: Spark. Glue version 4.0. Worker type: G.1X, 2 workers.
- **Run** el job y, mientras corre, muestra en **CloudWatch Logs** (Bloque 3 — Observabilidad) cómo aparecen los logs en vivo, filtrados por `Job Run ID`.
- Verifica el resultado: `aws s3 ls s3://bsg-demo-glue/curated/transacciones/ --recursive` — aparecen carpetas `transaction_date=2025-01-05/`, etc. (particionado real), **pero todavía no existen como tabla consultable**.
- Crea un segundo crawler `crawler-transacciones-curated` apuntando a `curated/transacciones/`, mismo `demo_batch_db`. Corre el crawler.
- **Punto pedagógico:** antes de correr este crawler, intenta consultar `transacciones_curated` en Athena — no existe. Después de correr el crawler, sí. Esto demuestra en vivo que **escribir archivos en S3 no los registra automáticamente en el Data Catalog.**

---

### 3. Escenario B — el mismo Job, pero catalogando él mismo (buena práctica de producción)

```python
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, to_date

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

dyf = glueContext.create_dynamic_frame.from_catalog(
    database="demo_batch_db", table_name="transacciones"
)
df = dyf.toDF()

df_clean = df.filter(col("amount") > 0)
df_clean = df_clean.withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd"))

dyf_clean = DynamicFrame.fromDF(df_clean, glueContext, "dyf_clean")

# Aquí está la diferencia clave: el propio Job registra la tabla y sus
# particiones en el Data Catalog al momento de escribir. No hace falta
# un segundo crawler.
sink = glueContext.getSink(
    connection_type="s3",
    path="s3://bsg-demo-glue/curated_v2/transacciones/",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["transaction_date"],
)
sink.setFormat("glueparquet")
sink.setCatalogInfo(catalogDatabase="demo_batch_db", catalogTableName="transacciones_curated_prod")
sink.writeFrame(dyf_clean)

job.commit()
```

- Job name: `job-transacciones-escenario-b`. Misma configuración de rol/workers que el anterior.
- **Run** el job. En cuanto termina (sin correr ningún crawler adicional), ve directo a Athena y consulta `transacciones_curated_prod` — **ya existe y ya tiene las particiones registradas**.
- **Punto pedagógico:** compara el tiempo total — Escenario A necesitó Job + esperar Crawler (varios minutos extra); Escenario B quedó listo apenas terminó el Job.

---

### 4. Consultas en Athena (Bloque 4 — Almacenamiento y análisis)

```sql
-- Escenario A (vía crawler)
SELECT * FROM transacciones_curated LIMIT 10;

-- Escenario B (vía enableUpdateCatalog)
SELECT * FROM transacciones_curated_prod LIMIT 10;

-- Mismo resultado agregado en ambas, para confirmar que el dato es idéntico
SELECT customer_id, SUM(amount) AS total_gastado
FROM transacciones_curated_prod
GROUP BY customer_id
ORDER BY total_gastado DESC;
```

Verifica que **no aparecen montos negativos** en ninguna de las dos tablas (el control de calidad funcionó igual en ambos escenarios).

## Comparación A vs B (cierre de esta parte de la demo)

| | Escenario A (2 crawlers) | Escenario B (`enableUpdateCatalog`) |
|---|---|---|
| Pasos | Job → esperar Crawler → tabla lista | Job → tabla lista |
| Costo extra | ~$0.05-$0.08 por el crawler adicional | $0 |
| Esquema de la tabla | Inferido por el crawler (heurística) | Exactamente el que definió tu código |
| ¿Detecta si OTRO proceso escribe ahí? | Sí | No — solo cataloga lo que ese Job escribió |
| ¿Cuándo usarlo? | Zonas donde no controlas quién escribe (típicamente raw) | Zonas donde tu propio pipeline es el único dueño (típicamente curated) |

**Mensaje de cierre para los estudiantes:** "Escenario A no está mal — de hecho así lo vimos primero para entender bien qué hace un Crawler. Pero en producción, cuando *tú* controlas el Job que escribe los datos, el Escenario B es la buena práctica: mismo resultado, menos costo, menos latencia y menos piezas que orquestar. La regla general es *'cataloga con crawler lo que no controlas, cataloga en el job lo que sí controlas'*."

## Nota de tiempo
Hacer ambos escenarios completos suma ~15-20 min extra sobre la versión de un solo escenario. Si vas corto de tiempo, puedes dejar el Job del Escenario B corriendo en segundo plano mientras explicas la tabla comparativa, y solo mostrar el resultado en Athena al final.

## Limpieza post-clase (evitar costos)
```bash
aws s3 rm s3://bsg-demo-glue --recursive
aws glue delete-crawler --name crawler-transacciones-raw
aws glue delete-crawler --name crawler-transacciones-curated
aws glue delete-job --job-name job-transacciones-escenario-a
aws glue delete-job --job-name job-transacciones-escenario-b
aws glue delete-database --name demo_batch_db
```
