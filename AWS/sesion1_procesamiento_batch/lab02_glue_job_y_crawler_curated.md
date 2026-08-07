# Lab 02 — ETL con Glue Job + segundo Crawler

**Duración:** ~35 minutos
**Requisito:** haber completado el [Lab 01](lab01_crawler_y_data_catalog.md)

## Qué vas a construir

```
   demo_batch_db.transacciones          (tabla del Lab 01)
              │
              │  ① Glue Job (PySpark)
              │     · filtra montos negativos
              │     · convierte texto -> date
              │     · escribe Parquet particionado
              ▼
   s3://TU-BUCKET/curated/transacciones/
        transaction_date=2025-01-05/part-....parquet
        transaction_date=2025-01-06/part-....parquet
        ...
              │
              │  ② Segundo Crawler        <- ¡este paso es necesario!
              ▼
   demo_batch_db.curated_transacciones
              │
              ▼
           Athena
```

## Qué vas a aprender

- Escribir un **Glue Job en PySpark** que hace Extract, Transform y Load
- Por qué **escribir archivos en S3 no los registra en el Data Catalog**
- Qué es el **particionado** y por qué ahorra dinero
- Leer logs en **CloudWatch** para diagnosticar un job en la nube

---

## Paso 1 — Crear el Job

1. Consola → **AWS Glue** → **ETL jobs** → **Script editor**
2. **Engine:** *Spark* · **Options:** *Start fresh* → **Create script**
3. Borra el contenido de ejemplo y pega el código de [`scripts/etl_transacciones.py`](scripts/etl_transacciones.py)
4. **Cambia la línea del bucket** por el tuyo:

```python
BUCKET = "bsg-glue-lab-TUSINICIALES"   # <-- tu bucket aquí
```

5. Pestaña **Job details** y configura:

| Campo | Valor |
|---|---|
| **Name** | `job-transacciones-etl` |
| **IAM Role** | `AWSGlueServiceRole-Lab` |
| **Type** | Spark |
| **Glue version** | Glue 4.0 |
| **Language** | Python 3 |
| **Worker type** | G.1X |
| **Requested number of workers** | 2 |
| **Job bookmark** | **Disable** |
| **Number of retries** | **0** |

6. **Save**

> **Job bookmark → Disable.** Los *bookmarks* hacen que Glue recuerde qué datos ya procesó, para no repetir trabajo. Es útil en producción, pero en un lab te va a confundir: la segunda ejecución no procesaría nada y parecería que el job está roto.
>
> **Number of retries → 0.** Por defecto Glue reintenta al fallar. En un lab eso solo significa esperar el doble antes de ver el error.

---

## Paso 2 — Entender el código antes de ejecutarlo

No lo ejecutes todavía. Lee estas tres partes:

### Extract — leer del catálogo, no de la ruta

```python
dyf = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE,
    table_name=TABLA_RAW,
)
```

Fíjate en que **no hay ninguna ruta S3 aquí**. Leemos por nombre de tabla, y el Data Catalog resuelve dónde están los archivos y cómo interpretarlos.

Esto es justo para lo que sirve el catálogo: si mañana los datos se mueven de carpeta o cambian de CSV a JSON, este código **no se toca**. Solo se actualiza el catálogo.

### Transform — las dos correcciones del Lab 01

```python
df_clean = df.filter(col("amount") > 0)
```
Control de calidad: fuera los montos negativos que arruinaban el `SUM()`.

```python
df_clean = df_clean.withColumn(
    "transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd")
)
```
Aquí arreglamos lo que el crawler no pudo adivinar: `string` → `date` de verdad.

### Load — Parquet particionado

```python
df_clean.write.mode("overwrite").partitionBy("transaction_date").parquet(RUTA_CURATED)
```

Tres decisiones en una sola línea:

- **`.parquet(...)`** — formato columnar comprimido, en vez de CSV. Athena lee solo las columnas que la consulta pide, así que escanea (y cobra) mucho menos.
- **`.partitionBy("transaction_date")`** — crea una carpeta por fecha. Una consulta con `WHERE transaction_date = '2025-01-05'` lee **una sola carpeta** en vez de todo el dataset. Esto se llama *partition pruning*.
- **`.mode("overwrite")`** — reemplaza el contenido anterior. **Ojo: en Spark "overwrite" significa borrar todo y volver a escribir**, por eso el rol necesita `s3:DeleteObject`.

---

## Paso 3 — Ejecutar y observar

1. **Run** (arriba a la derecha)
2. Ve a la pestaña **Runs**. El estado pasa por `Running` → `Succeeded` (2-3 minutos)
3. **Mientras corre**, abre el enlace **Error logs** del run. Se abre CloudWatch en el stream con el `Job Run ID`.

Filtra el stream por `[ETL]` y verás los tres mensajes que emite nuestro script:

```
[ETL] Filas leídas desde raw: 10
[ETL] Esquema de origen: struct<transaction_id:bigint,customer_id:string,...>
[ETL] Filas después del control de calidad: 7
```

### Dónde van a parar los mensajes del código

Que los mensajes salgan en **Error logs** y no en **Output logs** parece un error, pero no lo es. Glue reparte los logs en dos grupos de CloudWatch, y confundirlos es la causa más común de *"mis mensajes no aparecen por ningún lado"*:

| Log group | Qué contiene | Enlace en la consola |
|---|---|---|
| `/aws-glue/jobs/output` | la salida estándar: los `print()` de Python | **Output logs** |
| `/aws-glue/jobs/error` | los logs de Spark y los de `glueContext.get_logger()` | **Error logs** |

En este lab usamos el logger de Glue en vez de `print()` por dos razones prácticas: aparece **en vivo** mientras el job corre (los `print()` quedan bufereados y suelen salir todos juntos al final, cuando ya no sirven para diagnosticar), y cada línea trae marca de tiempo y nivel.

El precio es que cae en `/error`, mezclado con miles de líneas de Spark. Por eso prefijamos cada mensaje con `[ETL]`: filtrando por esa cadena pasas de 3.000 líneas a las tres que te importan.

> Si tu job tiene activado *continuous logging*, los mensajes no están en ninguno de esos dos grupos sino en **`/aws-glue/jobs/logs-v2`**, en el stream `<job-run-id>-driver`.

### Por qué esto importa

En tu laptop, cuando un script falla, ves el error en la terminal. **En la nube no hay terminal.** El job corre en máquinas que no controlas, que se crean y se destruyen solas.

CloudWatch **es** tu terminal. Si no sabes leerlo, no puedes diagnosticar nada. Por eso la observabilidad no es un extra: es la única forma de saber qué pasó.

> Vas a ver muchísimas líneas `INFO` de Spark, Drools y del analizador de logs de AWS. **Casi todas son ruido.** Para encontrar un error real, busca con Ctrl+F: `Traceback`, `Py4JJavaError`, `AnalysisException` o `Caused by`.

---

## Paso 4 — Verificar los archivos en S3

Ve a S3 → tu bucket → `curated/transacciones/`

Deberías ver **cinco carpetas**, no archivos sueltos:

```
curated/transacciones/
├── transaction_date=2025-01-05/   part-00000-....snappy.parquet
├── transaction_date=2025-01-06/   part-00000-....snappy.parquet
├── transaction_date=2025-01-07/   part-00000-....snappy.parquet
├── transaction_date=2025-01-08/   part-00000-....snappy.parquet
└── transaction_date=2025-01-09/   part-00000-....snappy.parquet
```

Ese formato `columna=valor` en el nombre de carpeta se llama **Hive-style partitioning**, y es un estándar que entienden Athena, Spark, Presto y prácticamente todo el ecosistema.

Los datos ya están ahí: limpios, transformados y particionados. **Pero todavía no puedes consultarlos.**

---

## Paso 5 — La demostración clave

Antes de crear el segundo crawler, ve a Athena y ejecuta:

```sql
SELECT * FROM demo_batch_db.curated_transacciones LIMIT 10;
```

**Va a fallar:**

```
TABLE_NOT_FOUND: line 1:15: Table 'awsdatacatalog.demo_batch_db.curated_transacciones' does not exist
```

**No es un error tuyo. Es el punto más importante de toda la sesión.**

Los archivos Parquet existen. Los acabas de ver en S3. Están bien escritos y bien particionados. Pero **escribir archivos en S3 no los registra en el Data Catalog** — son dos operaciones completamente separadas.

Sin entrada en el catálogo, Athena no sabe que esos archivos existen, ni dónde están, ni qué columnas tienen. Para Athena, esa tabla no existe.

> Guarda ese mensaje de error mentalmente. Cuando en un trabajo real alguien diga *"pero si el job corrió bien, ¿por qué no veo la tabla?"*, la respuesta suele ser exactamente esta.

---

## Paso 6 — Crear el segundo Crawler

Ahora sí, catalogamos la zona curated. Es el mismo procedimiento del Lab 01, con **dos diferencias importantes**:

1. Glue → **Crawlers** → **Create crawler**
2. **Name:** `crawler-transacciones-curated`
3. **Data source:** S3 → `s3://bsg-glue-lab-TUSINICIALES/curated/transacciones/`
4. **IAM role:** `AWSGlueServiceRole-Lab`
5. **Target database:** `demo_batch_db`
6. **Table name prefix:** `curated_`  — **no lo olvides**
7. **Frequency:** On demand → **Create crawler** → **Run crawler**

### Por qué el prefijo es obligatorio

Un crawler nombra la tabla según **la última carpeta de la ruta S3**. Tu ruta termina en `.../transacciones/`, así que sin prefijo crearía una tabla llamada `transacciones`… que es **exactamente el nombre de la tabla de raw** que ya existe en `demo_batch_db`.

El resultado es que el crawler **sobreescribe la tabla de raw** para que apunte a la carpeta `curated/`. A partir de ahí tu Job empieza a leerse a sí mismo, y el pipeline se rompe de una forma bastante confusa de diagnosticar.

Con el prefijo `curated_`, la tabla se llama `curated_transacciones` y no hay colisión.

**Si ya lo corriste sin prefijo:** entra a Data Catalog → Tables → `transacciones` → mira el campo **Location**. Si dice `curated/`, bórrala, vuelve a correr `crawler-transacciones-raw` para recrear la de raw, y luego el de curated con el prefijo puesto.

---

## Paso 7 — Consultar la tabla curada

Vuelve a Athena y repite **la misma consulta que falló** en el paso 5:

```sql
SELECT * FROM demo_batch_db.curated_transacciones LIMIT 10;
```

Ahora funciona. Lo único que cambió es que existe una entrada en el catálogo.

### Verificar que el ETL hizo su trabajo

```sql
-- Debe devolver 7 filas (las 10 originales menos 3 negativas)
SELECT COUNT(*) AS total FROM curated_transacciones;

-- Debe devolver 0 filas: ya no hay montos negativos
SELECT * FROM curated_transacciones WHERE amount < 0;

-- Ahora sí, el total por cliente es correcto
SELECT customer_id, SUM(amount) AS total_gastado
FROM curated_transacciones
GROUP BY customer_id
ORDER BY total_gastado DESC;
```

Compara este último resultado con el mismo cálculo sobre la tabla raw del Lab 01. **C001 pasa de 138.20 a 150.50.** Ese es el valor concreto de la capa curated.

### Comprobar el particionado

```sql
-- Fíjate en "Data scanned" al pie del resultado de cada consulta
SELECT * FROM curated_transacciones WHERE transaction_date = DATE '2025-01-05';
SELECT * FROM curated_transacciones;
```

La primera escanea **una sola partición**; la segunda, todas. Con 10 filas la diferencia es irrelevante, pero Athena cobra **por byte escaneado**: con terabytes, esa diferencia es la factura del mes.

### Ver el esquema corregido

Glue → Tables → `curated_transacciones` → Schema.

`transaction_date` ahora aparece como **partition key** de tipo `date`, no como `string`. El problema que detectamos en el Lab 01 quedó resuelto.

---

## Checkpoint

- [ ] El Job `job-transacciones-etl` terminó en `Succeeded`
- [ ] En S3 hay 5 carpetas `transaction_date=...` con archivos `.parquet`
- [ ] Viste la consulta **fallar** antes del segundo crawler y **funcionar** después
- [ ] `SELECT COUNT(*) FROM curated_transacciones` devuelve **7**
- [ ] `SELECT * FROM curated_transacciones WHERE amount < 0` devuelve **0 filas**
- [ ] La tabla se llama `curated_transacciones` (con prefijo) y la de raw sigue intacta

---

## Lo importante de este lab

Acabas de construir un pipeline batch completo: **ingesta → catalogación → transformación → catalogación → consulta**. Es el patrón que usa la mayoría de los data lakes en producción.

Pero fíjate en el costo de este diseño: para tener la tabla disponible necesitaste **dos pasos y dos esperas** — el Job, y después el Crawler. En producción eso significa orquestar dos piezas y sumar varios minutos de latencia cada vez.

**Existe una alternativa.** El propio Job puede registrar la tabla en el catálogo mientras escribe, usando `enableUpdateCatalog=True`. Eso elimina el segundo crawler por completo: cuando el Job termina, la tabla ya está lista.

La regla práctica que conviene recordar:

> **Cataloga con crawler lo que no controlas. Cataloga desde el job lo que sí controlas.**

La zona `raw` recibe datos de fuera, no sabes cuándo ni cómo llegan → crawler.
La zona `curated` la escribe tu propio pipeline, tú eres el único dueño → el job la cataloga.

---

## Problemas frecuentes

| Síntoma | Causa | Solución |
|---|---|---|
| `Failed to delete key: curated/transacciones` en la **2ª ejecución** | Falta `s3:DeleteObject` en el rol. La 1ª vez funciona porque no había nada que borrar | Agrega el permiso (Lab 00, paso 3) |
| La 2ª ejecución termina OK pero no escribe nada | Job bookmark activado | Job details → Job bookmark → **Disable** |
| `Table not found` **después** del segundo crawler | Te falta el prefijo, o el crawler no terminó | Verifica el nombre exacto en Data Catalog → Tables |
| La tabla de raw ahora apunta a `curated/` | Corriste el 2º crawler sin prefijo | Ver la advertencia del Paso 6 |
| `AnalysisException: cannot resolve 'amount'` | El Job está leyendo la tabla equivocada | Mismo problema de colisión de nombres |
| Los montos aparecen duplicados | Ejecutaste el Job varias veces sin overwrite efectivo | Vacía `curated/transacciones/` y vuelve a correr |
| El error real no aparece en los logs | Estás mirando el log de *Output* | Mira **CloudWatch → `/aws-glue/jobs/error`** |
| No veo los mensajes `[ETL]` por ningún lado | El logger de Glue escribe en el grupo `/error`, no en `/output` | Abre **Error logs** y filtra por `[ETL]` |

---

## Retos opcionales

Si terminaste antes, prueba estos:

1. **Agrega una columna calculada.** Clasifica cada transacción en `alto` / `medio` / `bajo` según el monto, usando `when().otherwise()` de PySpark.
2. **Cambia el particionado.** Particiona por `customer_id` en vez de por fecha. ¿Cuántas carpetas se crean? ¿En qué caso convendría cada opción?
3. **Rompe el pipeline a propósito.** Quita `s3:DeleteObject` del rol y ejecuta el job dos veces. Confirma que la primera pasa y la segunda falla — y que ahora sabes exactamente por qué.
4. **Investiga `enableUpdateCatalog`.** Modifica el script para que el Job registre la tabla él mismo y elimine la necesidad del segundo crawler.

---

## Limpieza — no te saltes esto

Los recursos de este lab siguen costando dinero si los dejas. Al terminar:

**Desde la consola:**
1. **S3** → tu bucket → **Empty** → luego **Delete**
2. **Glue → Crawlers** → borra `crawler-transacciones-raw` y `crawler-transacciones-curated`
3. **Glue → ETL jobs** → borra `job-transacciones-etl`
4. **Glue → Databases** → borra `demo_batch_db`
5. **IAM → Roles** → borra `AWSGlueServiceRole-Lab`

**O con AWS CLI:**

```bash
aws s3 rm s3://bsg-glue-lab-TUSINICIALES --recursive
aws s3 rb s3://bsg-glue-lab-TUSINICIALES
aws glue delete-crawler --name crawler-transacciones-raw
aws glue delete-crawler --name crawler-transacciones-curated
aws glue delete-job --job-name job-transacciones-etl
aws glue delete-database --name demo_batch_db
```
