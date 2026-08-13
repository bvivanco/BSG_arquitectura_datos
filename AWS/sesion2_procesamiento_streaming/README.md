# Sesión 2 — Procesamiento en Streaming con AWS Lambda

Laboratorios prácticos de la Sesión 2 del curso **Arquitectura de Datos en la Nube con Python**.

Vas a construir un pipeline de streaming completo: un dato entra por HTTP, se valida, se guarda y dispara un análisis automático, todo en menos de un segundo y sin crear un solo servidor.

El escenario es un **sistema de monitoreo de signos vitales**: un reloj inteligente que envía el pulso de un paciente.

---

## El pipeline que vas a construir

```
   curl / Postman
        |
        |  POST /pulsaciones
        v
   API Gateway  ------------------------------+
        |                                     |  GET /pacientes/{id}
        v                                     v
   Lambda RegistrarPulso            Lambda ConsultarPaciente
   valida y enriquece                        |
        |                                    |
        v                                    |
   DynamoDB SignosVitales  <-----------------+
        |
        |  DynamoDB Stream
        v
   Lambda DetectarAlertas
        |
        v
   DynamoDB AlertasSignosVitales
```

---

## Los laboratorios

| # | Lab | Duración | Qué construyes |
|---|---|---|---|
| 00 | [Preparación del entorno](lab00_preparacion_entorno.md) | ~15 min | Tabla DynamoDB y rol IAM |
| 01 | [Lambda y DynamoDB](lab01_lambda_y_dynamodb.md) | ~30 min | Función de ingesta con validación |
| 02 | [API Gateway](lab02_api_gateway.md) | ~35 min | Endpoints POST y GET, prueba end-to-end |
| 03 | [DynamoDB Streams y alertas](lab03_dynamodb_streams_alertas.md) | ~30 min | Análisis automático orientado a eventos |

**Hazlos en orden.** Construimos de dentro hacia fuera: primero la función, después la puerta HTTP, y al final la reacción automática. Si algo falla en un lab, ya sabes que las capas anteriores funcionaban.

---

## Contenido de esta carpeta

```
sesion2_procesamiento_streaming/
├── README.md
├── lab00_preparacion_entorno.md
├── lab01_lambda_y_dynamodb.md
├── lab02_api_gateway.md
├── lab03_dynamodb_streams_alertas.md
└── scripts/
    ├── lambda_registrar_pulso.py       ingesta con validación
    ├── lambda_consultar_paciente.py    consulta por rango
    ├── lambda_detectar_alertas.py      disparada por el stream
    └── enviar_pulsos.sh                genera tráfico de prueba
```

---

## Servicios de AWS que vas a usar

| Servicio | Para qué |
|---|---|
| **AWS Lambda** | Ejecutar código sin servidores, en respuesta a eventos |
| **DynamoDB** | Guardar los datos con latencia de milisegundos |
| **DynamoDB Streams** | Capturar cada cambio y disparar un análisis |
| **API Gateway** | Exponer el pipeline por HTTP |
| **CloudWatch Logs** | Ver qué pasó dentro de cada función |
| **IAM** | Dar a cada Lambda exactamente los permisos que necesita |

---

## Antes de empezar

- **Región:** trabaja siempre en **`us-east-1`** y no la cambies durante la sesión.
- **Terminal con `curl`:** viene instalado en macOS y Linux. Postman también sirve.
- **Costo:** céntimos de dólar si completas los cuatro labs y haces la limpieza. DynamoDB On-demand y Lambda cobran por uso, y este volumen es mínimo.
- **Limpieza:** al terminar, sigue la sección final del [Lab 03](lab03_dynamodb_streams_alertas.md#limpieza--no-te-saltes-esto). Presta atención a desactivar el stream: una Lambda conectada sigue haciendo polling aunque no lleguen datos.

---

## Las cuatro ideas que debes llevarte

**1. En streaming, si descartas un dato, desapareció.**
En batch el archivo original sigue en `raw/` y puedes reprocesar. Aquí no hay segunda oportunidad. Por eso los eventos rechazados van a una Dead Letter Queue en lugar de tirarse en silencio.

**2. La clave de DynamoDB se diseña a partir de las preguntas, no de los datos.**
Puedes usar `query` en vez de `scan` únicamente porque `PacienteId` es la partition key. Si eliges mal la clave, no hay forma de arreglarlo sin recrear la tabla.

**3. Separa el proceso que recibe del proceso que interpreta.**
La ingesta valida y guarda; el análisis ocurre después, disparado por el stream. Añadir un consumidor nuevo no obliga a tocar la ingesta.

**4. Un stream que se realimenta es un bucle infinito que factura.**
Una Lambda disparada por un stream nunca escribe en la tabla que la dispara.

---

## Si algo falla

Cada lab tiene su tabla de **Problemas frecuentes**. Los tres errores que más aparecen:

| Error | Dónde está la solución |
|---|---|
| `Float types are not supported. Use Decimal types instead.` | [Lab 01, paso 2](lab01_lambda_y_dynamodb.md#dynamodb-no-acepta-float) — falta `parse_float=Decimal` |
| `502 Bad Gateway` desde API Gateway | [Lab 02, paso 2](lab02_api_gateway.md#qué-es-lambda-proxy-integration) — `body` debe ser cadena, no diccionario |
| El trigger del stream no se activa | [Lab 03, paso 3](lab03_dynamodb_streams_alertas.md#paso-3--ampliar-el-rol-iam) — falta `AWSLambdaDynamoDBExecutionRole` |

Y una regla general: cuando la consola diga `Internal server error` sin más detalle, el error real está en **CloudWatch → `/aws/lambda/<nombre-de-la-función>`**.
