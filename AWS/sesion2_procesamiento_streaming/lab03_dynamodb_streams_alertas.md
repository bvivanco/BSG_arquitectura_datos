# Lab 03 — DynamoDB Streams y alertas

**Duración:** ~30 minutos
**Requisito:** haber completado el [Lab 02](lab02_api_gateway.md)

## Qué vas a construir

```
   POST /pulsaciones
        |
        v
   Lambda RegistrarPulso  --->  DynamoDB SignosVitales
                                       |
                                       |  DynamoDB Stream
                                       |  (INSERT / UPDATE / DELETE)
                                       v
                              Lambda DetectarAlertas
                                       |
                                       v
                          DynamoDB AlertasSignosVitales
```

Hasta ahora el pipeline era una línea recta: entra un dato, se guarda, alguien lo consulta. Este lab añade la pieza que lo convierte en **arquitectura orientada a eventos**: un proceso que reacciona solo, sin que nadie lo llame.

## Qué vas a aprender

- Activar DynamoDB Streams y conectarlo a una Lambda
- Por qué conviene separar la ingesta del análisis
- El formato DynamoDB JSON y cómo deserializarlo
- Cómo se crea (y cómo se evita) un bucle infinito que factura

---

## Paso 1 — Crear la tabla de alertas

1. DynamoDB → **Create table**
2. **Table name:** `AlertasSignosVitales`
3. **Partition key:** `PacienteId` — String
4. **Sort key:** `Timestamp` — String
5. **Capacity mode:** On-demand
6. **Create table**

### Por qué una tabla distinta y no una columna en la misma

Esta es la decisión más importante del lab.

Si la Lambda de alertas escribiera en `SignosVitales`, esa escritura generaría **un nuevo evento en el stream**, que volvería a invocar a la misma Lambda, que escribiría otra vez, y así indefinidamente. Un bucle infinito que no falla ni da error: simplemente corre y factura hasta que alguien mira la cuenta.

La regla, y conviene anotarla: **una Lambda disparada por un stream nunca escribe en la tabla que la dispara.** O escribes en otra tabla, o filtras por tipo de evento al inicio de la función para cortar la realimentación.

---

## Paso 2 — Activar el stream

1. DynamoDB → **Tables** → `SignosVitales` → pestaña **Exports and streams**
2. En **DynamoDB stream details** → **Turn on**
3. **View type:** **New image**
4. **Turn on stream**

### Qué significa el view type

| Opción | Qué entrega | Cuándo usarla |
|---|---|---|
| Keys only | Solo la clave del ítem modificado | Cuando basta saber que algo cambió |
| **New image** | El ítem completo después del cambio | **La nuestra**: necesitamos el pulso |
| Old image | El ítem completo antes del cambio | Auditoría de qué se borró |
| New and old images | Ambas versiones | Para saber qué cambió exactamente |

El stream conserva los cambios durante **24 horas**. Si tu consumidor está caído más de un día, esos eventos se pierden. No es un almacén: es una cinta transportadora.

---

## Paso 3 — Ampliar el rol IAM

Leer de un stream requiere permisos que el rol todavía no tiene.

1. IAM → **Roles** → `LambdaSignosVitalesRole`
2. **Add permissions** → **Attach policies**
3. Busca y marca **`AWSLambdaDynamoDBExecutionRole`** → **Add permissions**

Esa política gestionada concede `GetRecords`, `GetShardIterator`, `DescribeStream` y `ListStreams`, que son los permisos de lectura del stream.

Los permisos de escritura sobre `AlertasSignosVitales` ya los añadiste en el Lab 00, cuando incluiste esa tabla en la política inline.

---

## Paso 4 — Crear la Lambda de alertas

1. Lambda → **Create function** → `DetectarAlertas`, Python 3.12, rol `LambdaSignosVitalesRole`
2. Pega el código de [`scripts/lambda_detectar_alertas.py`](scripts/lambda_detectar_alertas.py) → **Deploy**
3. **Configuration** → **General configuration** → **Edit** → **Timeout:** 30 segundos → **Save**

El timeout más alto importa porque esta función procesa **lotes** de registros, no uno solo.

### Conectar el stream

1. En la misma función → **Add trigger**
2. **Source:** **DynamoDB**
3. **DynamoDB table:** `SignosVitales`
4. **Batch size:** 10
5. **Starting position:** **Latest**
6. **Add**

El trigger tarda un minuto en quedar activo. Espera a que aparezca como *Enabled*.

---

## Paso 5 — Entender el código

### Llegan lotes, no eventos sueltos

```python
registros = event.get("Records", [])
for registro in registros:
```

Con invocación stream-based, Lambda hace *polling* sobre el stream y entrega hasta 10 registros por invocación (el batch size que configuraste). Tu código tiene que recorrerlos.

Esto es más eficiente: una sola invocación procesa varios eventos. Pero tiene una consecuencia importante: **si un registro del lote hace fallar la función, el lote completo se reintenta**, incluidos los registros que ya se habían procesado bien. Por eso el código ignora en silencio los registros que no puede procesar en lugar de lanzar una excepción.

### El formato DynamoDB JSON

```python
imagen = registro["dynamodb"].get("NewImage")
item = {k: deserializador.deserialize(v) for k, v in imagen.items()}
```

Los registros del stream no llegan como JSON normal. Cada valor viene envuelto en su tipo:

```json
{
  "PacienteId": { "S": "P001" },
  "pulso":      { "N": "145" }
}
```

`S` es String, `N` es Number, `M` es Map, `L` es List. El `TypeDeserializer` de boto3 lo convierte a un diccionario de Python normal. Intentar leer `imagen["pulso"]` directamente devuelve `{"N": "145"}`, no `145`.

### Solo interesan las inserciones

```python
if registro.get("eventName") != "INSERT":
    continue
```

El stream entrega también UPDATE y DELETE. Aquí solo nos importan las lecturas nuevas.

---

## Paso 6 — Probar el flujo completo

Envía tráfico con el script del Lab 02:

```bash
./scripts/enviar_pulsos.sh https://TU-API.execute-api.us-east-1.amazonaws.com/dev/pulsaciones 30
```

Espera unos segundos y revisa:

1. **DynamoDB → `SignosVitales`** → deben estar las 30 lecturas.
2. **DynamoDB → `AlertasSignosVitales`** → solo las que quedaron fuera del rango 50-120, marcadas como `TAQUICARDIA` o `BRADICARDIA`.
3. **CloudWatch → `/aws/lambda/DetectarAlertas`** → busca las líneas `[ALERTA]` y `[ALERTAS]`.

En los logs verás algo así:

```
[ALERTAS] lote de 7 registros, 2 alertas generadas
```

Fíjate en que los lotes no son de tamaño fijo: Lambda entrega lo que hay disponible en el stream en ese momento, hasta el máximo configurado.

---

## Paso 7 — El punto de la arquitectura

Compara lo que hace cada función:

| | `RegistrarPulso` | `DetectarAlertas` |
|---|---|---|
| Quién la invoca | API Gateway, síncrono | El stream, automático |
| Alguien espera respuesta | Sí, el cliente HTTP | No |
| Si falla | El cliente recibe un error | Se reintenta sola |
| Qué pasa si se vuelve lenta | El cliente espera | Nadie se entera |

**Esa separación es el objetivo del lab.** La ingesta hace lo mínimo —validar y guardar— para responder rápido. Todo el análisis ocurre después, en paralelo, sin bloquear a nadie.

Si mañana quieres añadir un envío de correo, un cálculo de promedio móvil o una notificación por SNS, se añade como otro consumidor del mismo stream. **La función de ingesta no se toca.**

Es el mismo principio que separaba `raw` de `curated` en la Sesión 1: no mezcles el proceso que recibe con el proceso que interpreta. La diferencia es que allí pasaban horas entre uno y otro, y aquí pasan milisegundos.

---

## Checkpoint

- [ ] Tabla `AlertasSignosVitales` creada
- [ ] Stream activo en `SignosVitales` con view type **New image**
- [ ] El trigger de `DetectarAlertas` aparece como **Enabled**
- [ ] Enviaste tráfico y aparecen alertas solo para pulsos fuera de 50-120
- [ ] Sabes explicar por qué la alerta se escribe en otra tabla

---

## Reto opcional

Añade una notificación real: crea un tema en **SNS**, suscribe tu correo, y haz que `DetectarAlertas` publique un mensaje cuando detecte `TAQUICARDIA`. Necesitarás añadir `sns:Publish` al rol.

Pista sobre lo que vas a descubrir: si envías 30 lecturas y 6 son alertas, recibirás 6 correos. En un sistema real eso es inaceptable. La solución se llama *agregación de ventanas* y es la diapositiva 30 de la sesión.

---

## Problemas frecuentes

| Síntoma | Causa | Solución |
|---|---|---|
| El trigger no se activa | Faltan permisos de stream | Adjunta `AWSLambdaDynamoDBExecutionRole` (paso 3) |
| No se genera ninguna alerta | Todos los pulsos están en rango | El script genera ~20% fuera; envía más lecturas |
| `KeyError: 'NewImage'` | View type mal elegido | Debe ser **New image**, no *Keys only* |
| El pulso llega como `{'N': '145'}` | Falta deserializar | Es lo que hace `TypeDeserializer`; revisa el código |
| La misma alerta aparece repetida | El lote se reintentó tras un fallo | Es esperable: la entrega es *at least once*, el código debe ser idempotente |
| La factura sube sola | Bucle infinito de stream | Comprueba que escribes en `AlertasSignosVitales`, no en `SignosVitales` |

---

## Limpieza — no te saltes esto

```bash
aws dynamodb delete-table --table-name SignosVitales
aws dynamodb delete-table --table-name AlertasSignosVitales
aws lambda delete-function --function-name RegistrarPulso
aws lambda delete-function --function-name ConsultarPaciente
aws lambda delete-function --function-name DetectarAlertas
```

Y desde la consola: borra la API `ApiSignosVitales` en API Gateway y el rol `LambdaSignosVitalesRole` en IAM.

Presta atención especial a **desactivar el stream y borrar el trigger** antes de irte. Una Lambda conectada a un stream sigue haciendo polling aunque no lleguen datos.
