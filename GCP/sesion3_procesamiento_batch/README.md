# Sesión 3 — Demo: Procesamiento Batch con Cloud Dataflow

Llevar un CSV desde Cloud Storage hasta una tabla consultable en BigQuery, de dos formas distintas.

```
   ventas_sucursales.csv          Dataflow                  BigQuery
          |                          |                          |
          v                          v                          v
   Cloud Storage      ---->    procesa el CSV     ---->    tabla consultable
   gs://tu-bucket/raw/         linea por linea            con SQL
```

| Camino | Qué es | Cuándo se usa |
|---|---|---|
| **A. Plantilla** | Un pipeline que Google ya escribió; tú solo rellenas parámetros | Cargas simples, sin lógica de negocio |
| **B. Pipeline propio** | Código Apache Beam en Python que tú escribes | En cuanto hay que validar, derivar o deduplicar |

Hazlos en ese orden. El camino B se entiende mucho mejor después de topar con los límites del A.

---

## Contenido de esta carpeta

```
sesion3_procesamiento_batch/
├── README.md
├── requirements.txt
├── consultas.sql                  verificación y análisis en BigQuery
├── datos/
│   └── ventas_sucursales.csv      412 filas con problemas de calidad
├── plantilla/                     archivos que necesita el camino A
│   ├── esquema_bq.json
│   └── udf_transform.js
└── pipeline_batch_beam.py         camino B
```

## El dataset

412 filas de ventas de cuatro sucursales, entre marzo y abril de 2025. Trae problemas deliberados, para que haya algo real que limpiar:

- Montos negativos (devoluciones registradas como venta)
- Categorías vacías
- Cantidades en cero
- Algunas fechas en `dd/mm/yyyy` en vez de `yyyy-mm-dd`
- 12 filas duplicadas exactas

---

## Si es tu primera vez en Google Cloud

Esta es la primera sesión del capítulo de GCP. Si vienes de las sesiones 1 y 2, ya conoces todos los conceptos: solo cambian los nombres.

| En AWS lo llamabas | En GCP se llama | Qué es |
|---|---|---|
| S3 | **Cloud Storage** | Almacenamiento de archivos. Un *bucket* es lo mismo en ambas |
| Glue Job | **Dataflow** | El motor que procesa los datos |
| Athena | **BigQuery** | Donde consultas con SQL |
| Base de datos del Data Catalog | **Dataset de BigQuery** | La carpeta que agrupa tablas |
| CloudWatch Logs | **Cloud Logging** | Donde ves qué pasó |

**Todo cuelga de un proyecto.** En GCP el proyecto es la unidad de facturación, de permisos y de aislamiento. Lo ves y lo cambias en el selector de la barra superior de la consola, al lado del logo. Cuando en este documento aparezca `MI-PROYECTO`, se refiere al **ID** de tu proyecto (no al nombre): lo encuentras en ese mismo selector, o en la página de inicio de la consola.

### Cómo moverse por la consola

Casi todo se hace desde el **menú de navegación**: el icono de tres rayas (☰) arriba a la izquierda. Desde ahí llegas a Cloud Storage, BigQuery, Dataflow y todo lo demás.

### Cómo está escrito este documento

**Todo lo que se puede hacer desde el portal está explicado desde el portal**, clic a clic. Los comandos de terminal aparecen siempre después, dentro de un bloque como este:

> **Alternativa por terminal:** `gcloud ...`

Son opcionales. Si nunca has usado la consola de GCP, ignóralos y sigue solo los pasos del portal.

La única parte que **sí requiere terminal** es el Camino B, porque consiste en ejecutar un programa de Python. No hay forma de hacer eso desde el portal.

Para esos casos, lo más cómodo es **Cloud Shell**: el icono `>_` arriba a la derecha de la consola. Abre una terminal dentro del navegador, ya autenticada y con todo instalado, así no configuras nada en tu máquina.

---

## Paso 0 — Preparar el entorno

### 0.0 Crear el proyecto y activar la facturación

Si ya tienes un proyecto de GCP con facturación activa, salta al paso 0.1.

**Crear el proyecto:**

1. Entra a **console.cloud.google.com** con tu cuenta de Google
2. En la barra superior, junto al logo de Google Cloud, haz clic en el **selector de proyecto**
3. Arriba a la derecha de la ventana que se abre: **Proyecto nuevo**
4. **Nombre del proyecto:** por ejemplo `curso-arquitectura-datos`
5. Mira el **ID del proyecto** que se genera debajo del nombre. Si quieres cambiarlo, hazlo ahora: **el ID no se puede modificar después**. Es único a nivel mundial, así que Google puede añadirle números si el que quieres ya existe.
6. **Ubicación:** deja *Sin organización* si es una cuenta personal
7. **Crear**, y cuando termine, selecciona el proyecto nuevo en el selector

**Activar la facturación:**

Esto no es opcional: **Dataflow no funciona sin una cuenta de facturación vinculada**, ni siquiera dentro del nivel gratuito.

1. Menú ☰ → **Facturación**
2. Si no tienes ninguna cuenta: **Administrar cuentas de facturación** → **Crear cuenta**
3. Si ya la tienes: **Vincular una cuenta de facturación** y selecciónala

La primera vez, Google ofrece la **prueba gratuita: 300 USD de crédito por 90 días**. Pide una tarjeta para verificar identidad, pero no cobra nada ni pasa a modo de pago automáticamente: cuando se agota el crédito o el plazo, los servicios se detienen y hay que aprobar el cambio a mano.

**Cuánto cuesta esta demo:** céntimos. Pero conviene saber qué es gratis y qué no:

| Servicio | Nivel gratuito mensual |
|---|---|
| BigQuery | 1 TiB de consultas + 10 GiB de almacenamiento |
| Cloud Storage | 5 GiB en regiones de EE. UU. |
| **Dataflow** | **Ninguno** — cobra desde el primer minuto |

Por eso el Camino B empieza por `DirectRunner`, que se ejecuta en tu máquina o en Cloud Shell sin costo de cómputo.

### 0.1 Habilitar las APIs

En GCP los servicios vienen **apagados** y hay que encenderlos por proyecto. Esto sorprende a quien viene de AWS, donde todo está disponible desde el inicio. Si te lo saltas, verás errores de permisos que parecen otra cosa.

1. Menú ☰ → **APIs y servicios** → **Biblioteca**
2. Busca **Dataflow API** → clic → **Habilitar**
3. Repite con **Cloud Storage API** y **BigQuery API**

Puede tardar un minuto cada una.

> Atajo por terminal, si prefieres: abre Cloud Shell y ejecuta
> `gcloud services enable dataflow.googleapis.com storage.googleapis.com bigquery.googleapis.com`

### 0.2 Crear el bucket

1. Menú ☰ → **Cloud Storage** → **Buckets**
2. **Crear**
3. **Nombre:** `bsg-demo-gcp-TUSINICIALES`
   Los nombres son **únicos a nivel mundial**, igual que en S3, así que pon tus iniciales o un número.
4. **Tipo de ubicación:** *Región* → **us-central1 (Iowa)**
5. Deja el resto por defecto → **Crear**

Si aparece una ventana sobre acceso público, deja marcada la protección.

Anota tu bucket, lo vas a escribir varias veces:

```
Mi bucket: gs://________________________________
```

### 0.3 Subir los tres archivos

Dentro del bucket que acabas de crear:

1. **Crear carpeta** → nombre `raw` → **Crear**
2. Vuelve al bucket → **Crear carpeta** → nombre `config` → **Crear**
3. Entra a `raw/` → **Subir archivos** → selecciona `datos/ventas_sucursales.csv`
4. Vuelve a `config/` → **Subir archivos** → selecciona **los dos** archivos de la carpeta `plantilla/`: `esquema_bq.json` y `udf_transform.js`

Debe quedar así:

```
gs://bsg-demo-gcp-TUSINICIALES/
    raw/ventas_sucursales.csv
    config/esquema_bq.json
    config/udf_transform.js
```

> Por terminal sería `gsutil cp archivo gs://tu-bucket/carpeta/`, que es el equivalente exacto de `aws s3 cp`.

### 0.4 Crear el dataset de BigQuery

Un *dataset* es la carpeta que agrupa tablas. Es lo mismo que una base de datos del Data Catalog en AWS.

1. Menú ☰ → **BigQuery**. Se abre el espacio de trabajo SQL.
2. En el panel **Explorador** de la izquierda, busca tu proyecto
3. Clic en los **tres puntos verticales** a la derecha del nombre del proyecto → **Crear conjunto de datos**
4. **ID del conjunto de datos:** `ventas_demo`
5. **Tipo de ubicación:** *Región* → **us-central1**, la misma que el bucket
6. **Crear conjunto de datos**

> La región del dataset debe coincidir con la del bucket. Si no, Dataflow falla con un error de ubicación que no dice claramente cuál es el problema.

### Checkpoint del paso 0

- [ ] Las tres APIs habilitadas
- [ ] Bucket creado en `us-central1`
- [ ] Los tres archivos subidos, en `raw/` y `config/`
- [ ] Dataset `ventas_demo` creado en la misma región

---

## Camino A — La plantilla de Google

Corresponde a la actividad de la diapositiva 48.

### Qué es una plantilla de Dataflow

Una **plantilla** es un pipeline que Google ya escribió, probó y publicó. No programas nada: rellenas unos parámetros en un formulario y lo ejecutas.

Comparado con lo que ya conoces: es como si AWS te diera un Glue Job ya hecho, y tú solo indicaras de dónde leer y a dónde escribir.

La que usamos, **Text Files on Cloud Storage to BigQuery**, hace tres cosas:

```
   Archivos de texto            Tu funcion               Tabla de
   en Cloud Storage   ---->     JavaScript      ---->    BigQuery
   (nuestro CSV)                (linea a linea)
```

1. Lee los archivos de texto que le indiques, línea por línea
2. Pasa cada línea por una función JavaScript **que tú escribes**
3. Guarda lo que esa función devuelva en una tabla de BigQuery

### Por qué pide dos archivos extra

Este es el punto que conviene entender antes de ejecutar nada.

La plantilla es **genérica**: sirve para cualquier archivo de texto, así que no tiene forma de saber qué columnas tiene tu CSV ni de qué tipo son. Hay que dárselo hecho:

| Archivo | Qué le dice a la plantilla |
|---|---|
| `esquema_bq.json` | Qué columnas y tipos tendrá la tabla de destino |
| `udf_transform.js` | Cómo convertir cada línea de texto en un objeto con esos campos |

**Este es el contraste con la Sesión 1.** El Crawler de AWS leía una muestra del archivo y deducía el esquema solo. Aquí no hay nadie adivinando: o lo declaras tú, o la plantilla no sabe qué hacer con tus datos.

Ninguno de los dos enfoques es mejor. El crawler es más cómodo y a veces se equivoca —como cuando dejó `transaction_date` en `string`—. Declararlo cuesta más trabajo y siempre es exacto.

### Ejecutar el job

1. Menú ☰ → **Dataflow** → **Jobs**
2. **Crear trabajo a partir de plantilla**
3. **Nombre del trabajo:** `carga-ventas-plantilla`
4. **Extremo regional:** `us-central1`
5. **Plantilla de Dataflow:** despliega la lista y busca **Text Files on Cloud Storage to BigQuery**

   Al seleccionarla, el formulario cambia y aparecen sus parámetros.

6. Rellena los seis campos, sustituyendo tu bucket y tu proyecto:

| Campo del formulario | Valor |
|---|---|
| JavaScript UDF path in Cloud Storage | `gs://bsg-demo-gcp-TUSINICIALES/config/udf_transform.js` |
| JavaScript UDF name | `transform` |
| JSON path | `gs://bsg-demo-gcp-TUSINICIALES/config/esquema_bq.json` |
| Cloud Storage Input File(s) | `gs://bsg-demo-gcp-TUSINICIALES/raw/ventas_sucursales.csv` |
| BigQuery output table | `MI-PROYECTO:ventas_demo.ventas_plantilla` |
| Temporary directory for BigQuery loading | `gs://bsg-demo-gcp-TUSINICIALES/temp` |

7. **Ejecutar trabajo**

> **JavaScript UDF name** debe ser exactamente `transform`, porque así se llama la función dentro de `udf_transform.js`.
>
> **BigQuery output table** usa dos puntos entre el proyecto y el dataset, y un punto entre el dataset y la tabla: `proyecto:dataset.tabla`. Es un formato peculiar y es causa frecuente de error.
>
> La carpeta `temp/` no hace falta crearla: Dataflow la crea sola.

### Qué observar mientras corre

Al ejecutar, la consola te lleva a la página del job. Quédate en la pestaña del **gráfico de ejecución**.

Verás las cajas del pipeline iluminarse a medida que avanza, con el número de elementos que entró y salió de cada paso. Un cuello de botella se detecta a simple vista: una caja acumula elementos a la entrada y produce pocos a la salida.

El job tarda **3 a 5 minutos**, la mayor parte en levantar las máquinas. Es normal.

En el panel derecho, la sección de **Autoescalado** muestra cómo Dataflow añade y quita workers solo.

### Ver el resultado

Cuando el job aparezca como **Succeeded**:

1. Menú ☰ → **BigQuery**
2. En el **Explorador**, despliega tu proyecto → `ventas_demo`
3. Deberías ver la tabla **`ventas_plantilla`**. Haz clic en ella.
4. Pestaña **Vista previa** para ver las filas, o **Esquema** para ver los tipos
5. Para consultarla, botón **Consulta** → escribe:

```sql
SELECT * FROM `MI-PROYECTO.ventas_demo.ventas_plantilla` LIMIT 20;
```

6. **Ejecutar**

### El punto pedagógico

Abre `plantilla/udf_transform.js` y mira estas líneas:

```javascript
if (valores[0] === 'venta_id') {
  return null;      // descarta la cabecera
}
```

Sin ese `return null`, la fila de encabezados entraría a BigQuery como un registro más, con la palabra "venta_id" dentro de la columna `venta_id`. Es el error número uno con esta plantilla.

Compruébalo tú: ejecuta esta consulta y confirma que devuelve cero filas.

```sql
SELECT * FROM `MI-PROYECTO.ventas_demo.ventas_plantilla`
WHERE venta_id = 'venta_id';
```

### La limitación

Mira el esquema de la tabla que se creó: `fecha_venta` quedó como **STRING**, no como DATE. Y ejecuta esto:

```sql
SELECT COUNT(*) AS montos_negativos
FROM `MI-PROYECTO.ventas_demo.ventas_plantilla`
WHERE monto_total < 0;
```

Hay montos negativos. Y categorías vacías. Y duplicados.

La plantilla cargó el CSV **tal cual**. No filtra, no normaliza, no deduplica y no convierte tipos. Para eso hace falta escribir el pipeline: el camino B.

---

## Camino B — Pipeline propio en Beam

### Instalar

Desde Cloud Shell, o desde tu terminal en esta carpeta:

```bash
pip install -r requirements.txt
```

Si trabajas fuera de Cloud Shell, además hay que autenticarse:

```bash
gcloud auth application-default login
```

### Ejecutar en local

`DirectRunner` es el runner que ejecuta el pipeline **en tu propia máquina**. Sirve para desarrollar y depurar con pocos datos, sin costo de cómputo:

```bash
python pipeline_batch_beam.py \
  --input datos/ventas_sucursales.csv \
  --project MI-PROYECTO \
  --dataset ventas_demo \
  --temp_location gs://bsg-demo-gcp-TUSINICIALES/temp
```

Escribe en BigQuery de verdad —por eso necesita credenciales y un `temp_location`—, pero el procesamiento ocurre en tu laptop: no se levanta ninguna máquina en GCP.

Tarda unos segundos. Al terminar, vuelve a BigQuery y refresca el dataset: aparecerán **dos tablas nuevas**, `ventas_curadas` y `ventas_rechazadas`.

### Ejecutar en Dataflow

El mismo archivo, cambiando el runner y apuntando al CSV que ya está en el bucket:

```bash
python pipeline_batch_beam.py \
  --input gs://bsg-demo-gcp-TUSINICIALES/raw/ventas_sucursales.csv \
  --project MI-PROYECTO \
  --dataset ventas_demo \
  --runner DataflowRunner \
  --region us-central1 \
  --temp_location gs://bsg-demo-gcp-TUSINICIALES/temp \
  --staging_location gs://bsg-demo-gcp-TUSINICIALES/staging
```

Ve a **Dataflow → Jobs** y verás el job corriendo, con su gráfico de ejecución, igual que el de la plantilla.

**Ese es el argumento central de Apache Beam**, y vale la pena decirlo en voz alta: el código no cambió ni una línea. Escribes el pipeline una vez y decides después dónde corre. Con Spark quedas atado a Spark; con Beam el mismo código puede ejecutarse en Dataflow, en Flink o en tu máquina.

### Qué hace el pipeline

1. Lee el CSV saltando la cabecera (`skip_header_lines=1`)
2. Convierte tipos: texto a fecha, aceptando los dos formatos presentes
3. Rechaza montos no positivos, cantidades en cero y categorías vacías
4. Normaliza las categorías
5. Deriva la columna `anio_mes`
6. Deduplica por `venta_id`
7. Escribe en `ventas_curadas`, particionada por mes

### Lo que no descarta en silencio

Las filas rechazadas **no se pierden**: salen por una salida secundaria y terminan en `ventas_rechazadas`, con el motivo.

```python
yield beam.pvalue.TaggedOutput(self.RECHAZOS, {...})
```

Es el equivalente en Beam de una Dead Letter Queue. Ese desglose es información real sobre la calidad del origen; si solo filtraras, se perdería.

---

## Verificar el resultado

1. Menú ☰ → **BigQuery**
2. Botón **Consulta** para abrir un editor
3. Abre el archivo `consultas.sql` de esta carpeta, copia las consultas una a una y sustituye `MI-PROYECTO`

Las cuatro que más vale la pena mirar:

| Consulta | Qué demuestra |
|---|---|
| 1 | Curadas más rechazadas suman el total del CSV: no se perdió nada |
| 2 | Por qué se rechazó cada fila |
| 4 | Cuatro categorías, no ocho variantes: la normalización funcionó |
| 6 | Bytes procesados con y sin filtro de partición |

Sobre la consulta 6: antes de ejecutar, BigQuery muestra arriba a la derecha cuántos bytes va a procesar. Compara ese número entre las dos versiones. **Es el mismo concepto que *Data scanned* en Athena, y la misma factura.**

---

## Comparación de los dos caminos

| | Plantilla | Pipeline propio |
|---|---|---|
| Tiempo de puesta en marcha | Minutos | Horas |
| Requiere programar | Solo una función JS | Sí, Python con Beam |
| Validación y reglas de negocio | No | Sí |
| Deduplicación | No | Sí |
| Tipos correctos en destino | No, casi todo texto | Sí |
| Rastro de lo rechazado | No | Sí |

**La regla práctica:** usa la plantilla si solo tienes que mover el dato. En cuanto tengas que cambiarlo, escribe el pipeline.

---

## Limpieza

Los recursos siguen costando si los dejas.

**Desde la consola:**

1. Menú ☰ → **BigQuery** → tres puntos junto a `ventas_demo` → **Borrar**
2. Menú ☰ → **Cloud Storage** → marca tu bucket → **Borrar**
3. Menú ☰ → **Dataflow** → comprueba que ningún job siga en ejecución

**Por terminal:**

```bash
bq rm -r -f --dataset MI-PROYECTO:ventas_demo
gsutil -m rm -r gs://bsg-demo-gcp-TUSINICIALES
```

Los jobs batch terminan solos, así que no queda nada facturando. Aun así, revisa la lista.

---

## Problemas frecuentes

| Síntoma | Causa | Solución |
|---|---|---|
| `API has not been used in project` | Falta habilitar el servicio | Paso 0.1 |
| La cabecera aparece como una fila más | Falta el `return null` en la UDF | Ver `udf_transform.js` |
| `Dataset not found` | El dataset no existe o está en otra región | Paso 0.4; bucket y dataset en la misma región |
| `Cannot read and write in different locations` | Bucket y dataset en regiones distintas | Recrea uno de los dos en `us-central1` |
| Error en el nombre de la tabla de salida | Formato incorrecto | Debe ser `proyecto:dataset.tabla`, con dos puntos y punto |
| El job se queda en *Queued* mucho rato | Sin cuota de CPUs en la región | Prueba otra región, o usa `DirectRunner` |
| `403 Forbidden` al escribir en BigQuery | Faltan permisos | Tu cuenta necesita el rol *BigQuery Data Editor* |
| `ModuleNotFoundError: apache_beam` | Falta instalar | `pip install -r requirements.txt` |
| El pipeline no encuentra el CSV local | Ruta relativa desde otro directorio | Ejecuta desde esta carpeta, o usa ruta absoluta |
