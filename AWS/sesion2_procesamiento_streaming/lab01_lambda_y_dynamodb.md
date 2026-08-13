# Lab 01 — Lambda y DynamoDB

**Duración:** ~30 minutos
**Requisito:** haber completado el [Lab 00](lab00_preparacion_entorno.md)

## Qué vas a construir

```
   Evento de prueba (consola de Lambda)
              |
              v
   Lambda  RegistrarPulso
       valida el pulso
       enriquece con timestamp
              |
              v
   DynamoDB  SignosVitales
```

Todavía no hay API. Construimos de dentro hacia fuera: primero la función que hace el trabajo, y solo cuando esté probada le ponemos la puerta HTTP. Así, si algo falla en el Lab 02, ya sabes que la capa de abajo funciona.

## Qué vas a aprender

- Crear y probar una función Lambda desde la consola
- Validar datos en streaming, con un solo evento y sin contexto
- Por qué DynamoDB rechaza los números decimales de Python
- Leer la duración de una ejecución y ver el efecto del cold start

---

## Paso 1 — Crear la función

1. Consola → **Lambda** → **Create function**
2. **Author from scratch**
3. **Function name:** `RegistrarPulso`
4. **Runtime:** **Python 3.12**
5. **Architecture:** x86_64
6. Despliega **Change default execution role** → **Use an existing role** → `LambdaSignosVitalesRole`
7. **Create function**

En la pestaña **Code**, borra el contenido de `lambda_function.py` y pega el código de [`scripts/lambda_registrar_pulso.py`](scripts/lambda_registrar_pulso.py).

Pulsa **Deploy**. El botón no es opcional: mientras no despliegues, sigues ejecutando la versión anterior.

---

## Paso 2 — Entender el código antes de ejecutarlo

Tres detalles que explican casi todos los errores de este lab.

### El cuerpo puede llegar de dos formas

```python
cuerpo = event.get("body")
if isinstance(cuerpo, str):
    datos = json.loads(cuerpo, parse_float=Decimal)
else:
    datos = cuerpo if isinstance(cuerpo, dict) else event
```

Cuando invocas desde la consola, el evento llega como diccionario. Cuando lo invoca API Gateway, llega como una **cadena de texto** dentro de `event["body"]`. La función soporta ambos casos para que puedas probarla ahora y conectarla al API después sin tocar el código.

### DynamoDB no acepta `float`

```python
datos = json.loads(cuerpo, parse_float=Decimal)
```

Ese `parse_float=Decimal` es imprescindible. Python convierte `72.5` en un `float`, y `put_item` responde:

```
Float types are not supported. Use Decimal types instead.
```

Es el error número uno con DynamoDB en Python. La razón es que `float` es binario y no puede representar exactamente valores decimales; DynamoDB exige precisión, así que solo acepta `Decimal`.

### El timestamp lo pone el servidor

```python
"Timestamp": datetime.now(timezone.utc).isoformat(),
```

No se toma del cliente. Un reloj mal configurado, o alguien enviando datos a propósito con fecha falsa, podría ensuciar el orden completo de la tabla. En un sistema de ingesta, la hora de llegada la decide quien recibe.

---

## Paso 3 — Probar con un evento válido

1. Pestaña **Test** → **Create new test event**
2. **Event name:** `pulso_valido`
3. Reemplaza el JSON por:

```json
{
  "PacienteId": "P001",
  "pulso": 78,
  "origen": "reloj-demo"
}
```

4. **Save** → **Test**

Deberías ver `statusCode: 201` y el registro devuelto.

Ve a **DynamoDB → Tables → SignosVitales → Explore table items**. Ahí está tu lectura, con el timestamp que generó la función.

---

## Paso 4 — Probar que la validación funciona

Crea tres eventos de prueba más y ejecútalos. Anota qué código devuelve cada uno:

| Evento | JSON | Esperado |
|---|---|---|
| `pulso_fuera_rango` | `{"PacienteId": "P001", "pulso": 900}` | 400 — fuera de rango fisiológico |
| `falta_campo` | `{"pulso": 78}` | 400 — falta `PacienteId` |
| `pulso_texto` | `{"PacienteId": "P001", "pulso": "rapido"}` | 400 — no es un número |

Comprueba en DynamoDB que **ninguno de los tres se guardó**. La tabla debe seguir teniendo un solo ítem.

### La pregunta importante

En la Sesión 1 filtramos montos negativos dentro de un Glue Job, con el dataset completo delante. Podíamos comparar, contar, calcular la media.

Aquí la función decide con **un solo evento y sin contexto**. No sabe cuál fue la lectura anterior de ese paciente ni cuál será la siguiente. Por eso la validación en streaming se limita a reglas que se pueden evaluar de forma aislada: rangos, tipos, campos obligatorios.

Y hay una diferencia de consecuencias: en batch, si descartas una fila, el archivo original sigue en `raw/` y puedes reprocesarlo. Aquí, **si descartas el evento, desapareció para siempre**. Por eso en producción los eventos rechazados no se tiran: se mandan a una Dead Letter Queue para revisarlos después.

---

## Paso 5 — Observar la ejecución

Abre la pestaña **Monitor** → **View CloudWatch logs**, y entra al stream más reciente.

Busca la línea `REPORT` al final de cada invocación:

```
REPORT RequestId: ...  Duration: 412.55 ms  Billed Duration: 413 ms
Memory Size: 128 MB  Max Memory Used: 78 MB  Init Duration: 289.11 ms
```

Fíjate en dos cosas:

**`Init Duration` aparece solo en la primera invocación.** Es el cold start: el tiempo que AWS tardó en crear el entorno de ejecución, descargar tu código e inicializarlo. Vuelve a ejecutar el test inmediatamente y compara: esa línea ya no está, y la duración cae a decenas de milisegundos.

**`Max Memory Used` frente a `Memory Size`.** Si usas 78 MB de 128 asignados, vas bien. Si estuvieras rozando el límite, la función se volvería lenta o fallaría. Y recuerda que en Lambda la CPU escala con la memoria: subir la memoria a veces reduce el costo total, porque la función termina antes.

---

## Checkpoint

- [ ] La función `RegistrarPulso` existe y está desplegada
- [ ] El evento válido devuelve **201** y el ítem aparece en DynamoDB
- [ ] Los tres eventos inválidos devuelven **400** y no escriben nada
- [ ] Encontraste `Init Duration` en la primera ejecución y comprobaste que desaparece en la segunda

---

## Lo importante de este lab

Escribiste una función que se ejecuta sin que exista ningún servidor, valida datos y escribe en una base capaz de responder en milisegundos a cualquier escala. Y no configuraste una sola máquina.

Pero fíjate en lo que todavía falta: **nadie de fuera puede invocarla**. Solo tú, desde la consola. Eso es lo que resuelve el Lab 02.

---

## Problemas frecuentes

| Síntoma | Causa | Solución |
|---|---|---|
| `Float types are not supported` | Falta `parse_float=Decimal` | Está en el código; comprueba que pegaste la versión completa |
| `AccessDeniedException` en `PutItem` | La política inline no cubre la tabla | Revisa el número de cuenta en el JSON del Lab 00 |
| `ResourceNotFoundException` | El nombre de la tabla no coincide, o estás en otra región | Debe ser exactamente `SignosVitales` en `us-east-1` |
| Los cambios del código no surten efecto | No pulsaste **Deploy** | Deploy después de cada edición |
| `Task timed out after 3.00 seconds` | El timeout por defecto es de 3 s | Configuration → General configuration → Timeout: 10 s |

---

**Siguiente:** [Lab 02 — API Gateway](lab02_api_gateway.md)
