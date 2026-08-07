# Lab 01 — Crawler y Data Catalog

**Duración:** ~25 minutos
**Requisito:** haber completado el [Lab 00](lab00_preparacion_entorno.md)

## Qué vas a construir

```
  s3://TU-BUCKET/raw/transacciones/
        transacciones.csv                 (archivo suelto, nadie sabe qué contiene)
                │
                │  Crawler: lee una muestra, deduce columnas y tipos
                ▼
     Glue Data Catalog
        demo_batch_db.transacciones       (ahora es una TABLA)
                │
                ▼
             Athena
        SELECT * FROM transacciones       (SQL sobre un CSV en S3)
```

## Qué vas a aprender

- Qué es el **Glue Data Catalog** y por qué es la pieza central de un data lake
- Qué hace exactamente un **Crawler** (y qué *no* hace)
- Cómo consultar con SQL un archivo que sigue siendo un CSV en S3

---

## El concepto, en una frase

Un archivo en S3 es solo bytes. Nadie sabe si es un CSV, cuántas columnas tiene ni de qué tipo son.

El **Data Catalog** es el índice que responde esas preguntas: *"en esta ruta hay una tabla llamada `transacciones`, con 4 columnas, en formato CSV"*. Athena, Redshift Spectrum, EMR y Glue leen ese índice para saber cómo interpretar los archivos.

El **Crawler** es el proceso automático que rellena ese índice: mira una muestra de los archivos, deduce el esquema y lo registra.

> **La analogía:** S3 es la bodega llena de cajas. El Data Catalog es el inventario que dice qué hay en cada caja. El Crawler es el empleado que recorre la bodega y escribe el inventario.

---

## Paso 1 — Crear el Crawler

1. Consola → **AWS Glue** → menú izquierdo **Crawlers** → **Create crawler**

**Step 1 — Set crawler properties**
- **Name:** `crawler-transacciones-raw`
- **Next**

**Step 2 — Choose data sources**
- **Is your data already mapped to Glue tables?** → *Not yet*
- **Add a data source:**
  - Data source: **S3**
  - S3 path: `s3://bsg-glue-lab-TUSINICIALES/raw/transacciones/`
  - Subsequent crawler runs: *Crawl all sub-folders*
- **Add an S3 data source** → **Next**

> Cuidado aquí: apunta a la **carpeta**, no al archivo. Si pones `.../transacciones.csv`, el crawler creará una tabla por archivo en vez de una tabla para el conjunto — y cuando mañana lleguen 300 archivos tendrás 300 tablas.

**Step 3 — Configure security settings**
- **Existing IAM role:** `AWSGlueServiceRole-Lab`
- **Next**

**Step 4 — Set output and scheduling**
- **Target database** → **Add database** (se abre una pestaña nueva)
  - Name: `demo_batch_db` → **Create database**
  - Vuelve a la pestaña del crawler y refresca el desplegable → selecciona `demo_batch_db`
- **Table name prefix:** *déjalo vacío*
- **Crawler schedule / Frequency:** *On demand*
- **Next**

**Step 5 — Review** → **Create crawler**

---

## Paso 2 — Ejecutar el Crawler

1. En la lista de crawlers, selecciona `crawler-transacciones-raw` → **Run crawler**
2. El estado pasa por `Starting` → `Running` → `Stopping` → `Ready` (**tarda 1-2 minutos**, es normal)
3. Cuando termine, la columna **Table changes from last run** debe decir **1 created**

> Ese minuto y medio para leer un archivo de 300 bytes no es lentitud: es el tiempo de arrancar la infraestructura serverless por detrás. Con 10 GB tardaría prácticamente lo mismo. Vale la pena tenerlo presente al diseñar pipelines: **el costo fijo de arranque domina cuando los datos son pequeños.**

---

## Paso 3 — Inspeccionar la tabla creada

Glue → **Data Catalog** → **Tables** → clic en **`transacciones`**

Revisa y responde:

| Campo | Qué deberías ver |
|---|---|
| **Location** | `s3://TU-BUCKET/raw/transacciones/` |
| **Classification** | `csv` |
| **Record count / Size** | valores estimados por el crawler |

Y baja hasta el **Schema**:

| Columna | Tipo inferido |
|---|---|
| `transaction_id` | `bigint` |
| `customer_id` | `string` |
| `amount` | `double` |
| `transaction_date` | `string` (fíjate en esta) |

### Pregunta para pensar

**¿Por qué `transaction_date` quedó como `string` y no como `date`?**

Porque en un CSV **todo es texto**. El crawler aplica heurísticas: vio que `transaction_id` eran solo dígitos → `bigint`; que `amount` tenía punto decimal → `double`. Pero `2025-01-05` es ambiguo — podría ser una fecha, un código o un texto cualquiera. Ante la duda, el crawler elige lo seguro: `string`.

**Esto tiene consecuencias reales.** Con `transaction_date` como texto no puedes hacer `WHERE transaction_date > current_date - 7`, ni ordenar cronológicamente de forma fiable, ni particionar bien. Corregirlo va a ser una de las tareas del Lab 02.

Lección de fondo: **el crawler adivina, no sabe.** Es cómodo, pero un esquema inferido no es lo mismo que un esquema definido por ti.

---

## Paso 4 — Consultar con Athena

1. Consola → **Athena** → **Query editor**
2. **Database:** `demo_batch_db` (panel izquierdo)
3. Ejecuta:

```sql
SELECT * FROM transacciones;
```

Deberías ver **las 10 filas**, incluidas las de monto negativo.

### Consultas para explorar

```sql
-- ¿Cuántas transacciones hay?
SELECT COUNT(*) AS total FROM transacciones;

-- Los datos sucios que vamos a limpiar en el Lab 02
SELECT * FROM transacciones WHERE amount < 0;

-- Total gastado por cliente (¡ojo con el resultado!)
SELECT customer_id, SUM(amount) AS total
FROM transacciones
GROUP BY customer_id
ORDER BY total DESC;
```

### Pregunta para pensar

Mira el resultado de la última consulta. **Los montos negativos están restando del total de cada cliente.** C001 debería tener 150.50 pero muestra 138.20.

Ese es exactamente el tipo de error silencioso que un pipeline de datos debe evitar: nadie ve un mensaje de error, simplemente **el número del reporte está mal**. Por eso existe la capa `curated`.

---

## Checkpoint

- [ ] Crawler `crawler-transacciones-raw` en estado `Ready` con **1 tabla creada**
- [ ] Tabla `transacciones` visible en `demo_batch_db` con 4 columnas
- [ ] `SELECT * FROM transacciones` devuelve 10 filas en Athena
- [ ] Entiendes por qué `transaction_date` quedó como `string`

---

## Lo importante de este lab

**No moviste ni un byte.** El CSV sigue exactamente donde estaba, sin modificar. Lo único que creaste fue *metadatos*: una entrada en un índice que dice cómo interpretar ese archivo.

Y con solo eso ya puedes hacer SQL sobre él. Eso es lo que significa **"schema-on-read"** y es la diferencia fundamental entre un data lake y una base de datos tradicional:

| | Base de datos (schema-on-write) | Data Lake (schema-on-read) |
|---|---|---|
| ¿Cuándo se define el esquema? | Al cargar los datos | Al leerlos |
| ¿Se puede cargar dato "raro"? | No, lo rechaza | Sí, se guarda igual |
| ¿Cuánto cuesta cambiar el esquema? | Migración de toda la tabla | Actualizar metadatos |

---

## Problemas frecuentes

| Síntoma | Causa | Solución |
|---|---|---|
| Crawler termina con **0 tables created** | La ruta S3 está mal o vacía | Verifica que el CSV esté dentro de `raw/transacciones/` |
| `AccessDeniedException` | La política inline no cubre tu bucket | Revisa el nombre del bucket en el JSON del Lab 00 |
| El rol no aparece en el desplegable | El nombre no empieza con `AWSGlueServiceRole` | Renómbralo — Glue filtra por ese prefijo |
| Athena: *"No output location provided"* | Falta configurar el resultado | Athena → Settings → Manage (Lab 00, paso 4) |
| La tabla se llama `raw` o `transacciones_csv` | Apuntaste al archivo, no a la carpeta | Borra la tabla, corrige la ruta y vuelve a correr |

---

**Siguiente:** [Lab 02 — ETL con Glue Job y catalogación de curated](lab02_glue_job_y_crawler_curated.md)
