# Sesión 4 — Demo: Procesamiento en Streaming con Pub/Sub

Un script de Python publica telemetría de transformadores eléctricos y los datos aparecen en BigQuery en segundos.

```
   generador_transformadores.py
              |
              |  publica mensajes JSON
              v
        Pub/Sub  (tema: telemetria-red)
              |
      +-------+--------+
      |                |
      v                v
  Camino A         Camino B
  suscripcion      pipeline de
  directa          Dataflow (Beam)
      |                |
      v                v
         BigQuery
```

| Camino | Qué es | Cuándo se usa |
|---|---|---|
| **A. Suscripción a BigQuery** | Pub/Sub escribe directo en la tabla, sin procesador | El dato ya viene bien formado |
| **B. Pipeline de Beam** | Dataflow en el medio | Hay que validar, derivar o agregar |

Hazlos en ese orden.

---

## Contenido de esta carpeta

```
sesion4_procesamiento_streaming/
├── README.md
├── requirements.txt
├── consultas_monitoreo.sql
├── esquema_transformadores.avsc     contrato de datos del tema
├── generador_transformadores.py     publica telemetría a Pub/Sub
└── pipeline_streaming_beam.py       camino B
```

## El escenario

Cinco transformadores (`TR-101` a `TR-105`) reportan voltaje y corriente. El rango normal de tensión es 200-240 V. Alrededor del **8% de las lecturas** salen fuera de ese rango a propósito, simulando subtensiones y sobretensiones, para que haya anomalías reales que detectar.

---

## Si es tu primera vez con Pub/Sub

Tres conceptos y ya puedes seguir todo lo demás:

| Concepto | Qué es | Analogía |
|---|---|---|
| **Tema** (topic) | El canal donde se publican los mensajes | Un buzón al que cualquiera echa cartas |
| **Suscripción** | La conexión de un consumidor con ese tema | Cada persona con copia de la llave del buzón |
| **Mensaje** | El dato que viaja, aquí un JSON | La carta |

Lo que más confunde al principio: **cada suscripción recibe una copia completa de todos los mensajes**. No se reparten el trabajo. Si creas dos suscripciones, cada mensaje llega a las dos. Lo vas a comprobar en vivo al final.

Si vienes de las sesiones de AWS:

| En AWS | En GCP |
|---|---|
| Kinesis Data Streams | **Pub/Sub** |
| Lambda | Cloud Functions / Cloud Run |
| DynamoDB Streams | Suscripciones de Pub/Sub |
| Glue Job | **Dataflow** |
| Athena | **BigQuery** |
| S3 | Cloud Storage |

Y una diferencia de diseño que conviene notar: en la Sesión 2, API Gateway invocaba a Lambda **directamente**. Si Lambda fallaba, el evento se perdía. Pub/Sub es precisamente el amortiguador que allí no pusimos: guarda los mensajes hasta siete días y los entrega cuando el consumidor pueda recibirlos.

### Cómo moverse

Casi todo se hace desde el **menú de navegación**: el icono ☰ arriba a la izquierda de la consola.

### Cómo está escrito este documento

**Todo lo que se puede hacer desde el portal está explicado desde el portal**, clic a clic. Los comandos de terminal aparecen siempre después, dentro de un bloque como este:

> **Alternativa por terminal:** `gcloud ...`

Son opcionales. Si nunca has usado la consola de GCP, ignóralos y sigue solo los pasos del portal.

Hay dos cosas que **sí requieren terminal**, porque consisten en ejecutar programas de Python y no existe forma de hacerlo desde el portal:

- El **generador** que publica los mensajes (pasos A.4 y B.4)
- El **pipeline de Beam** del Camino B

Para esos casos, usa **Cloud Shell**: el icono `>_` arriba a la derecha de la consola. Abre una terminal dentro del navegador, ya autenticada y con `gcloud`, `bq` y Python instalados, así no configuras nada en tu máquina.

---

## Paso 0 — Preparar el entorno

### 0.0 Tener un proyecto con facturación activa

Si vienes de la Sesión 3, reutiliza el mismo proyecto y salta al paso 0.1.

Si es tu primer contacto con GCP, necesitas crear un proyecto y vincularle una cuenta de facturación antes de nada. Está explicado paso a paso en el [Paso 0.0 de la Sesión 3](../sesion3_procesamiento_batch/README.md#00-crear-el-proyecto-y-activar-la-facturación).

En resumen: `console.cloud.google.com` → selector de proyecto en la barra superior → **Proyecto nuevo**, y después menú ☰ → **Facturación** → vincular una cuenta. La prueba gratuita da 300 USD por 90 días.

**Dataflow no funciona sin facturación activa.** Pub/Sub y BigQuery sí tienen nivel gratuito (10 GiB de mensajes y 1 TiB de consultas al mes), así que el Camino A se puede hacer casi sin costo. El Camino B con `DirectRunner` también, porque el cómputo ocurre en Cloud Shell y no en Dataflow.

### 0.1 Habilitar las APIs

1. Menú ☰ → **APIs y servicios** → **Biblioteca**
2. Busca y habilita, una por una: **Cloud Pub/Sub API**, **BigQuery API** y **Dataflow API**

> Atajo desde Cloud Shell:
> `gcloud services enable pubsub.googleapis.com bigquery.googleapis.com dataflow.googleapis.com`

### 0.2 Anotar el ID y el número de tu proyecto

Los vas a necesitar más adelante, y **son dos cosas distintas**:

1. Menú ☰ → **IAM y administración** → **Configuración**
2. Ahí ves **Nombre del proyecto**, **ID del proyecto** y **Número del proyecto**

```
ID del proyecto:     ____________________   (texto, ej. mi-curso-datos-2025)
Numero del proyecto: ____________________   (solo digitos, ej. 483920174552)
```

El **ID** es el que se usa en las consultas SQL y en los comandos. El **número** solo hace falta para construir la dirección de la cuenta de servicio en el paso A.2.

> Si además vas a usar la terminal, abre Cloud Shell (`>_`) y fija el proyecto una vez, para no repetirlo en cada comando:
> ```bash
> export PROYECTO=MI-PROYECTO
> gcloud config set project $PROYECTO
> ```
> Si cierras Cloud Shell, hay que volver a ejecutar el `export`.

### 0.3 Crear el esquema del tema

El esquema Avro es un **contrato de datos**: Pub/Sub valida cada mensaje al publicarlo y rechaza el que no cumple.

1. Menú ☰ → **Pub/Sub** → **Esquemas**
2. **Crear esquema**
3. **ID del esquema:** `esquema-transformadores`
4. **Tipo de esquema:** **Avro**
5. En **Definición del esquema**, pega el contenido del archivo `esquema_transformadores.avsc` de esta carpeta
6. **Crear**

> Desde Cloud Shell sería:
> ```bash
> gcloud pubsub schemas create esquema-transformadores \
>   --type=AVRO --definition-file=esquema_transformadores.avsc
> ```

**Este es el contraste con la Sesión 1.** El crawler adivinaba el esquema **después** de que el dato llegara. Aquí el esquema se declara **antes** y se impone en la puerta de entrada: un mensaje mal formado ni siquiera entra al sistema.

### 0.4 Crear el tema

1. Menú ☰ → **Pub/Sub** → **Temas**
2. **Crear tema**
3. **ID del tema:** `telemetria-red`
4. Marca **Usar un esquema**
5. **Selecciona un esquema de Pub/Sub:** `esquema-transformadores`
6. **Codificación del mensaje:** **JSON**
7. Deja marcado *Agregar una suscripción predeterminada* si aparece, no molesta
8. **Crear**

### 0.5 Crear el dataset de BigQuery

1. Menú ☰ → **BigQuery**
2. En el panel **Explorador**, clic en los **tres puntos** junto al nombre de tu proyecto → **Crear conjunto de datos**
3. **ID:** `red_electrica`
4. **Ubicación:** *Región* → **us-central1**
5. **Crear conjunto de datos**

### Checkpoint del paso 0

- [ ] Tres APIs habilitadas
- [ ] Esquema `esquema-transformadores` creado como Avro
- [ ] Tema `telemetria-red` creado y asociado a ese esquema
- [ ] Dataset `red_electrica` creado

---

## Camino A — Suscripción directa a BigQuery

Sin Dataflow y sin escribir código de procesamiento. Es la actividad expositiva de la diapositiva 60.

### A.1 Crear la tabla de destino

La suscripción no crea la tabla: tiene que existir antes, y con las columnas exactas.

1. Menú ☰ → **BigQuery**
2. En el Explorador, tres puntos junto a `red_electrica` → **Crear tabla**
3. **Crear tabla desde:** *Tabla vacía*
4. **Tabla:** `telemetria_directa`
5. En **Esquema**, activa **Editar como texto** y pega:

```
transformador_id:STRING,voltaje:FLOAT,corriente:FLOAT,timestamp:TIMESTAMP
```

6. **Crear tabla**

> Desde Cloud Shell:
> ```bash
> bq mk --table $PROYECTO:red_electrica.telemetria_directa \
>   transformador_id:STRING,voltaje:FLOAT,corriente:FLOAT,timestamp:TIMESTAMP
> ```

### A.2 Dar permisos al service account de Pub/Sub

**Este es el paso que hace fallar la demo en silencio.** Si lo omites, la suscripción se crea sin ningún error y los mensajes simplemente desaparecen: no llegan a BigQuery y no hay mensaje de fallo en ningún sitio.

Pub/Sub no escribe con tu usuario, sino con una **cuenta de servicio propia** que Google crea para tu proyecto. Esa cuenta, por defecto, no tiene permiso sobre tus tablas.

**Paso 1: arma la dirección de la cuenta de servicio.**

Toma el **número de proyecto** que anotaste en el paso 0.2 y sustitúyelo aquí:

```
service-NUMERO_DE_PROYECTO@gcp-sa-pubsub.iam.gserviceaccount.com
```

Por ejemplo, si tu número es `483920174552`, queda:

```
service-483920174552@gcp-sa-pubsub.iam.gserviceaccount.com
```

Ojo: es el **número**, no el ID. Si pones el ID de texto, el permiso se otorga a una cuenta que no existe y la demo falla igual.

**Paso 2: otórgale el permiso.**

1. Menú ☰ → **IAM y administración** → **IAM**
2. Botón **Otorgar acceso** (arriba)
3. **Principales nuevas:** pega la dirección que armaste
4. **Asignar roles** → busca y selecciona **Editor de datos de BigQuery**
5. **Guardar**

**Paso 3: comprueba que quedó.**

En la lista de IAM, activa la casilla **Incluir asignaciones de roles proporcionadas por Google** (arriba a la derecha). Sin marcarla, las cuentas de servicio de Google no aparecen y parece que no se guardó nada.

> **Alternativa por terminal**, que además obtiene el número solo:
> ```bash
> NUMERO=$(gcloud projects describe $PROYECTO --format="value(projectNumber)")
> SA="service-${NUMERO}@gcp-sa-pubsub.iam.gserviceaccount.com"
> echo "Cuenta de servicio: $SA"
>
> gcloud projects add-iam-policy-binding $PROYECTO \
>   --member="serviceAccount:${SA}" \
>   --role="roles/bigquery.dataEditor"
> ```

### A.3 Crear la suscripción

1. Menú ☰ → **Pub/Sub** → **Suscripciones** → **Crear suscripción**
2. **ID de suscripción:** `telemetria-a-bq`
3. **Tema de Cloud Pub/Sub:** selecciona `telemetria-red`
4. **Tipo de entrega:** **Escribir en BigQuery**
5. **Proyecto:** el tuyo · **Conjunto de datos:** `red_electrica` · **Tabla:** `telemetria_directa`
6. Marca **Usar el esquema del tema**
7. **Crear**

Si el botón de crear se queda deshabilitado o sale un error de permisos, vuelve al paso A.2.

### A.4 Publicar datos

Desde Cloud Shell, en esta carpeta:

```bash
pip install -r requirements.txt
python generador_transformadores.py --project $PROYECTO --topic telemetria-red --mensajes 50
```

Verás en pantalla cada lectura publicada, con las anomalías marcadas.

### A.5 Ver los datos llegando

**Antes** de arrancar el generador, o en otra pestaña:

1. Menú ☰ → **BigQuery** → botón **Consulta**
2. Ejecuta:

```sql
SELECT COUNT(*) AS lecturas FROM `MI-PROYECTO.red_electrica.telemetria_directa`;
```

**El momento a provocar en clase:** ejecuta esa consulta antes de arrancar el generador y muestra que devuelve **0**. Arranca el generador, espera unos segundos y ejecuta **la misma consulta** otra vez. Repítelo tres veces.

Ver el número subir sin que nadie toque nada es lo que hace tangible el streaming.

Después, mira las filas:

```sql
SELECT * FROM `MI-PROYECTO.red_electrica.telemetria_directa`
ORDER BY timestamp DESC LIMIT 20;
```

### Lo que hay que señalar

No hay procesador en el medio. No hay Dataflow, no hay Cloud Functions, no hay código de ingesta. Pub/Sub escribe directo en BigQuery.

Menos piezas que orquestar, menos latencia y menos costo. Es la misma lección del Escenario B de la Sesión 1: **la mejor pieza de arquitectura suele ser la que consigues no poner**.

---

## Camino B — Pipeline de Beam

Necesario en cuanto haya que cambiar el dato antes de guardarlo.

### B.1 Crear una segunda suscripción

1. Menú ☰ → **Pub/Sub** → **Suscripciones** → **Crear suscripción**
2. **ID:** `telemetria-beam`
3. **Tema:** `telemetria-red`
4. **Tipo de entrega:** **Extracción** (Pull)
5. **Crear**

> Ahora hay dos suscripciones sobre el mismo tema. **Cada una va a recibir todos los mensajes.** No se reparten el trabajo. Es el malentendido más común de Pub/Sub y conviene demostrarlo en vivo comparando los conteos de las dos tablas al final.

### B.2 Crear un bucket para archivos temporales

Beam necesita un espacio en Cloud Storage para dejar archivos intermedios. Si ya hiciste la Sesión 3, reutiliza el mismo bucket y salta este paso.

1. Menú ☰ → **Cloud Storage** → **Buckets**
2. **Crear**
3. **Nombre:** `bsg-demo-gcp-TUSINICIALES`
   Los nombres son únicos a nivel mundial, así que pon tus iniciales.
4. **Tipo de ubicación:** *Región* → **us-central1 (Iowa)**, la misma del dataset
5. **Crear**

No hace falta crear la carpeta `temp/`: Beam la crea sola.

> **Alternativa por terminal:** `gsutil mb -l us-central1 gs://bsg-demo-gcp-TUSINICIALES`

### B.3 Ejecutar el pipeline en local

En una terminal de Cloud Shell, dentro de esta carpeta:

```bash
python pipeline_streaming_beam.py \
  --project $PROYECTO \
  --subscription projects/$PROYECTO/subscriptions/telemetria-beam \
  --dataset red_electrica \
  --temp_location gs://bsg-demo-gcp-TUSINICIALES/temp \
  --streaming
```

El pipeline queda **corriendo y esperando mensajes**. No termina solo: esa terminal se queda ocupada.

### B.4 Publicar en continuo

Abre **otra** terminal de Cloud Shell (el botón `+` de la barra de pestañas) y lanza el generador sin límite:

```bash
python generador_transformadores.py --project $PROYECTO --topic telemetria-red --mensajes 0
```

Con `--mensajes 0` publica hasta que lo cortes con Ctrl+C. Es lo cómodo para una demo en vivo.

### B.5 Ver el resultado

En BigQuery aparecerán **dos tablas nuevas** dentro de `red_electrica`:

- `lecturas` — cada lectura, ya validada, con `potencia_kw` y `estado` calculados
- `promedios_ventana` — el voltaje promedio por transformador cada 60 segundos

```sql
SELECT * FROM `MI-PROYECTO.red_electrica.lecturas`
ORDER BY timestamp DESC LIMIT 20;
```

### B.6 Ejecutar en Dataflow

El mismo archivo, cambiando el runner:

```bash
python pipeline_streaming_beam.py \
  --project $PROYECTO \
  --subscription projects/$PROYECTO/subscriptions/telemetria-beam \
  --dataset red_electrica \
  --runner DataflowRunner \
  --region us-central1 \
  --temp_location gs://bsg-demo-gcp-TUSINICIALES/temp \
  --staging_location gs://bsg-demo-gcp-TUSINICIALES/staging \
  --streaming
```

Ve a menú ☰ → **Dataflow** → **Jobs** para ver el gráfico de ejecución en vivo.

> **Un pipeline de streaming no termina solo.** Hay que detenerlo a mano: Ctrl+C en local, o el botón **Detener** en la página del job en Dataflow. Es la diferencia más visible con el pipeline batch de la Sesión 3, que acaba cuando se termina el archivo. Y es la forma más fácil de dejar un job facturando por olvido.

### Qué hace que el camino A no puede hacer

1. **Valida.** Los mensajes ilegibles o con lecturas imposibles no rompen el pipeline: salen por una salida secundaria.
2. **Deriva una columna.** `potencia_kw` no viene en el mensaje, se calcula como voltaje por corriente. Esa es, en una línea, la razón de tener un procesador en el medio.
3. **Clasifica.** Añade el campo `estado` con `NORMAL`, `SUBTENSION` o `SOBRETENSION`.
4. **Agrega por ventanas.** Calcula el voltaje promedio por transformador cada 60 segundos.

### Sobre las ventanas

```python
| "Ventana" >> beam.WindowInto(window.FixedWindows(60))
```

Una ventana fija (*tumbling*) agrupa los eventos en bloques que no se solapan: 00:00-00:01, 00:01-00:02, y así.

Es el concepto que no existe en batch, y conviene explicar por qué: en batch tienes el conjunto completo y puedes calcular el promedio de todo. En streaming el flujo no termina nunca, así que "el promedio" no significa nada hasta que defines **de qué intervalo**. La ventana es esa definición.

---

## Verificar

1. Menú ☰ → **BigQuery** → **Consulta**
2. Abre `consultas_monitoreo.sql`, copia las consultas y sustituye `MI-PROYECTO`

Las dos que más valen en clase:

**Consulta 4** — calcula cuántos segundos pasaron entre la lectura y ahora. Convierte el "tiempo real" en un número concreto que se puede mostrar.

**Consulta 6** — compara el promedio precalculado por ventana con el promedio recalculado sobre todo el histórico. El primero está listo al instante; el segundo obliga a escanear la tabla completa cada vez que alguien pregunta.

---

## Observabilidad

Con el generador corriendo:

1. Menú ☰ → **Pub/Sub** → **Suscripciones** → clic en `telemetria-beam`
2. Pestaña **Métricas**
3. Busca **Oldest unacknowledged message age**

Esa es la métrica que hay que vigilar. Si crece de forma sostenida, el consumidor no da abasto.

**El experimento que mejor enseña el desacoplamiento:** detén el pipeline de Beam con Ctrl+C y deja el generador publicando. La métrica empieza a subir y los mensajes sin confirmar se acumulan. Vuelve a arrancar el pipeline y verás cómo procesa el atraso y la métrica cae.

El productor nunca se enteró de que el consumidor estaba caído. Eso es el desacoplamiento, y verlo vale más que explicarlo.

---

## Limpieza

Un pipeline de streaming olvidado factura las 24 horas. No te saltes esto.

**Desde la consola:**

1. Menú ☰ → **Dataflow** → **Jobs** → detén cualquier job en ejecución
2. Menú ☰ → **Pub/Sub** → **Suscripciones** → borra `telemetria-a-bq` y `telemetria-beam`
3. **Pub/Sub** → **Temas** → borra `telemetria-red`
4. **Pub/Sub** → **Esquemas** → borra `esquema-transformadores`
5. Menú ☰ → **BigQuery** → tres puntos junto a `red_electrica` → **Borrar**

**Por terminal:**

```bash
gcloud pubsub subscriptions delete telemetria-a-bq telemetria-beam
gcloud pubsub topics delete telemetria-red
gcloud pubsub schemas delete esquema-transformadores
bq rm -r -f --dataset $PROYECTO:red_electrica
```

---

## Problemas frecuentes

| Síntoma | Causa | Solución |
|---|---|---|
| La suscripción existe pero no llega nada a BigQuery | Falta el permiso del service account | Paso A.2 |
| `API has not been used in project` | Falta habilitar el servicio | Paso 0.1 |
| `INVALID_ARGUMENT: Message failed schema validation` | El JSON no cumple el Avro | Los tipos deben coincidir: `voltaje` es `double`, no texto |
| No puedo crear la suscripción a BigQuery | La tabla no existe o el esquema no coincide | Pasos A.1 y A.2 |
| El pipeline arranca y no procesa nada | Nombre de suscripción incorrecto | Debe ser la ruta completa `projects/.../subscriptions/...` |
| `Workflow failed` en Dataflow por cuota | Sin CPUs disponibles en la región | Otra región, o ejecuta en local sin `--runner` |
| Los mensajes llegan duplicados | La entrega es *at least once* | Es esperable: el consumidor debe ser idempotente |
| El job sigue facturando tras la clase | Streaming no termina solo | **Detener** en la consola de Dataflow |
| `$PROYECTO` está vacío | Cerraste Cloud Shell | Vuelve a ejecutar el `export` del paso 0.2 |
