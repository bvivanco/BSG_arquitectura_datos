# Lab 02 — API Gateway

**Duración:** ~35 minutos
**Requisito:** haber completado el [Lab 01](lab01_lambda_y_dynamodb.md)

## Qué vas a construir

```
   curl / Postman
        |
        |  POST /pulsaciones
        v                            GET /pacientes/{PacienteId}
   API Gateway  ----------------------------------------+
        |                                               |
        v                                               v
   Lambda RegistrarPulso                    Lambda ConsultarPaciente
        |                                               |
        v                                               |
   DynamoDB  SignosVitales  <---------------------------+
```

## Qué vas a aprender

- Exponer una Lambda por HTTP con Lambda Proxy Integration
- Por qué el cuerpo llega como texto y qué error produce olvidarlo
- La diferencia entre `Query` y `Scan`, y por qué la decide el diseño de la clave

---

## Paso 1 — Crear la API

1. Consola → **API Gateway** → **Create API**
2. Busca **REST API** (no *REST API Private*, no *HTTP API*) → **Build**
3. **API name:** `ApiSignosVitales`
4. **API endpoint type:** **Regional**
5. **Create API**

> **REST API frente a HTTP API.** HTTP API es más barata y rápida, pero tiene menos funciones. Usamos REST API porque su consola muestra cada etapa del flujo por separado, y eso hace visible lo que estamos montando. En producción, para un caso simple como este, HTTP API sería una elección razonable.

---

## Paso 2 — Endpoint de ingesta (POST)

### Crear el recurso

1. **Resources** → **Create resource**
2. **Resource name:** `pulsaciones`
3. **Create resource**

### Crear el método

1. Con `/pulsaciones` seleccionado → **Create method**
2. **Method type:** **POST**
3. **Integration type:** **Lambda function**
4. Activa **Lambda proxy integration**
5. **Lambda function:** `RegistrarPulso`
6. **Create method**

Cuando pregunte si puede añadir permisos para invocar la función, acepta.

### Qué es Lambda Proxy Integration

Con esta opción activada, API Gateway le pasa a Lambda **el evento HTTP completo**: método, ruta, cabeceras, parámetros de query y cuerpo. A cambio, exige que la respuesta tenga exactamente esta forma:

```json
{
  "statusCode": 201,
  "headers": { "Content-Type": "application/json" },
  "body": "{\"mensaje\":\"lectura registrada\"}"
}
```

**`body` tiene que ser una cadena de texto**, no un diccionario. Si devuelves un diccionario, API Gateway responde `502 Bad Gateway` y en los logs aparece `Internal server error`. Por eso el código usa `json.dumps()` en la función `respuesta()`.

Sin proxy integration tendrías que escribir plantillas de mapeo en VTL para traducir la petición y la respuesta. Es más control y bastante más trabajo.

---

## Paso 3 — Desplegar

Una API creada no está publicada. Hay que desplegarla a una etapa.

1. **Deploy API**
2. **Stage:** *New stage* → **Stage name:** `dev`
3. **Deploy**

Copia la **Invoke URL** que aparece arriba. Tiene esta forma:

```
https://a1b2c3d4e5.execute-api.us-east-1.amazonaws.com/dev
```

> Recuerda desplegar **cada vez** que cambies algo en la API. Es el equivalente al botón Deploy de Lambda, y olvidarlo es una fuente clásica de "pero si ya lo arreglé".

---

## Paso 4 — Probar el pipeline completo

Desde tu terminal, sustituyendo la URL por la tuya:

```bash
curl -i -X POST \
  https://TU-API.execute-api.us-east-1.amazonaws.com/dev/pulsaciones \
  -H 'Content-Type: application/json' \
  -d '{"PacienteId":"P001","pulso":82,"origen":"reloj-demo"}'
```

Deberías recibir `HTTP/2 201` y el registro en la respuesta.

Abre **DynamoDB → SignosVitales → Explore table items**: el dato ya está ahí.

**Ese es el momento clave de la sesión.** Un dato entró por HTTP desde tu laptop, se validó, se enriqueció y quedó guardado en una base de datos, en menos de un segundo. No creaste ningún servidor, no configuraste ningún balanceador y no instalaste nada.

### Prueba también que la validación sigue viva

```bash
curl -i -X POST \
  https://TU-API.execute-api.us-east-1.amazonaws.com/dev/pulsaciones \
  -H 'Content-Type: application/json' \
  -d '{"PacienteId":"P001","pulso":900}'
```

Debe devolver `400`. La misma lógica del Lab 01, ahora accesible desde internet.

### Generar tráfico

Para tener suficientes datos para el Lab 03, usa el script incluido:

```bash
chmod +x scripts/enviar_pulsos.sh
./scripts/enviar_pulsos.sh https://TU-API.execute-api.us-east-1.amazonaws.com/dev/pulsaciones 20
```

Envía 20 lecturas repartidas entre tres pacientes, de las cuales alrededor del 20% quedan fuera del rango normal a propósito.

---

## Paso 5 — Endpoint de consulta (GET)

Ya sabemos escribir. Ahora hay que poder leer.

### Crear la segunda Lambda

1. Lambda → **Create function** → `ConsultarPaciente`, Python 3.12, rol `LambdaSignosVitalesRole`
2. Pega el código de [`scripts/lambda_consultar_paciente.py`](scripts/lambda_consultar_paciente.py) → **Deploy**

### Crear el recurso con parámetro de ruta

1. API Gateway → `ApiSignosVitales` → **Resources** → selecciona `/` → **Create resource**
2. **Resource name:** `pacientes` → **Create resource**
3. Con `/pacientes` seleccionado → **Create resource**
4. **Resource name:** `{PacienteId}` — con las llaves incluidas
5. **Create resource**

Las llaves indican que es un parámetro variable. Lo que el cliente ponga ahí llegará a la función en `event["pathParameters"]["PacienteId"]`.

### Crear el método GET

1. Con `/{PacienteId}` seleccionado → **Create method**
2. **Method type:** **GET**
3. **Integration type:** Lambda function, con **Lambda proxy integration** activado
4. **Lambda function:** `ConsultarPaciente`
5. **Create method**

### Desplegar y probar

**Deploy API** → etapa `dev`.

```bash
curl -s https://TU-API.execute-api.us-east-1.amazonaws.com/dev/pacientes/P001?limite=5 | python3 -m json.tool
```

Deberías ver las 5 lecturas más recientes de ese paciente, de la más nueva a la más vieja.

---

## Paso 6 — Query frente a Scan

Mira estas dos líneas del código de consulta:

```python
resultado = tabla.query(
    KeyConditionExpression=Key("PacienteId").eq(paciente),
    ScanIndexForward=False,
    Limit=limite,
)
```

**`query`** lee únicamente la partición de ese paciente. Cuesta lo mismo tener 100 lecturas que 100 millones en la tabla, porque nunca toca las demás particiones.

**`scan`** recorrería la tabla entera y descartaría después lo que no coincide. Con 20 registros no notarías la diferencia; con 20 millones, la consulta tardaría minutos y costaría dinero real.

Y aquí está el punto que conviene fijar: **solo puedes usar `query` porque `PacienteId` es la partition key**. Si en el Lab 00 hubiéramos elegido un id aleatorio de lectura como clave, esta consulta sería imposible de hacer eficientemente y no habría forma de arreglarlo sin recrear la tabla.

`ScanIndexForward=False` invierte el orden de la sort key, así que devuelve las lecturas más recientes primero. Sin ese parámetro tendrías las más antiguas.

> Compáralo con la Sesión 1: allí, particionar por fecha permitía a Athena leer solo las carpetas necesarias. Aquí, la partition key permite a DynamoDB leer solo una partición. Es la misma idea —no escanees lo que no necesitas— aplicada en dos tecnologías distintas.

---

## Checkpoint

- [ ] `POST /pulsaciones` devuelve **201** y el dato aparece en DynamoDB
- [ ] Un pulso de 900 devuelve **400** a través de la API
- [ ] Enviaste al menos 20 lecturas con el script
- [ ] `GET /pacientes/P001?limite=5` devuelve las lecturas más recientes primero
- [ ] Sabes explicar por qué se usa `query` y no `scan`

---

## Problemas frecuentes

| Síntoma | Causa | Solución |
|---|---|---|
| `502 Bad Gateway` | La Lambda devolvió `body` como diccionario | `body` debe ser cadena: `json.dumps(...)` |
| `403 Forbidden` / `Missing Authentication Token` | La ruta no existe o no desplegaste | Revisa la URL y vuelve a **Deploy API** |
| `500` al enviar JSON válido | Falta `parse_float=Decimal` | Ver Lab 01, paso 2 |
| El GET devuelve `{}` o `404` | Ese paciente no tiene lecturas | Envía primero un POST con ese `PacienteId` |
| Cambié la Lambda y la API responde igual | Falta **Deploy** en Lambda, o en API Gateway | Hay que desplegar en los dos sitios |
| `Internal server error` sin más detalle | El error real está en los logs | CloudWatch → `/aws/lambda/RegistrarPulso` |

---

**Siguiente:** [Lab 03 — DynamoDB Streams y alertas](lab03_dynamodb_streams_alertas.md)
