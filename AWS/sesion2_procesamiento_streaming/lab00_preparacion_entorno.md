# Lab 00 — Preparación del entorno

**Duración:** ~15 minutos
**Objetivo:** crear la tabla de DynamoDB y el rol IAM que usarán las funciones Lambda.

Igual que en la Sesión 1, este lab es plomería. Pero si el rol queda mal, los errores que verás después son confusos: la función parece ejecutarse bien y el dato simplemente no aparece.

---

## Antes de empezar

- Cuenta de AWS con permisos sobre **Lambda, DynamoDB, API Gateway, IAM y CloudWatch**.
- Trabaja en **`us-east-1`** y no cambies de región durante toda la sesión.
- Ten a mano una terminal con `curl` (viene instalado en macOS y Linux). Postman también sirve.

---

## Paso 1 — Crear la tabla de DynamoDB

1. Consola → **DynamoDB** → **Tables** → **Create table**
2. **Table name:** `SignosVitales`
3. **Partition key:** `PacienteId` — tipo **String**
4. **Sort key:** `Timestamp` — tipo **String**
5. **Table settings:** *Customize settings*
6. **Capacity mode:** **On-demand**
7. **Create table**

### Por qué esta clave y no otra

Esta es la decisión más importante del lab, y conviene entenderla antes de seguir.

DynamoDB reparte los datos en particiones según el hash de la **partition key**. Con `PacienteId` como partition key, todas las lecturas de un mismo paciente caen juntas. La **sort key** las ordena dentro de esa partición.

Esa combinación es la que permite preguntar *"dame las últimas 10 lecturas del paciente P001"* leyendo una sola partición. Si hubiéramos usado un id de lectura aleatorio como partition key, esa consulta obligaría a recorrer la tabla completa con un `Scan`: lento y caro.

La regla general: en DynamoDB **la clave se diseña a partir de las preguntas que vas a hacer**, no a partir de la estructura del dato. Es lo contrario de una base relacional.

### Sobre On-demand

Con On-demand pagas por petición y no configuras capacidad. Para un laboratorio con decenas de escrituras el costo es de céntimos. El modo Provisioned obliga a estimar cuántas lecturas y escrituras por segundo necesitas, y cobra esa capacidad reservada aunque nadie la use.

---

## Paso 2 — Crear el rol IAM para Lambda

1. Consola → **IAM** → **Roles** → **Create role**
2. **Trusted entity type:** AWS service
3. **Use case:** **Lambda** → **Next**
4. **Permissions:** busca y marca **`AWSLambdaBasicExecutionRole`** → **Next**
5. **Role name:** `LambdaSignosVitalesRole` → **Create role**

`AWSLambdaBasicExecutionRole` solo da permiso para escribir logs en CloudWatch. Todavía no puede tocar DynamoDB.

### Agregar los permisos sobre la tabla

1. Entra al rol recién creado → **Add permissions** → **Create inline policy**
2. Pestaña **JSON** → pega esto, **cambiando `TU-CUENTA`** por tu número de cuenta (lo ves arriba a la derecha en la consola):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:PutItem",
        "dynamodb:Query",
        "dynamodb:GetItem"
      ],
      "Resource": [
        "arn:aws:dynamodb:us-east-1:TU-CUENTA:table/SignosVitales",
        "arn:aws:dynamodb:us-east-1:TU-CUENTA:table/AlertasSignosVitales"
      ]
    }
  ]
}
```

3. **Next** → **Policy name:** `AccesoTablasSignosVitales` → **Create policy**

La tabla `AlertasSignosVitales` todavía no existe: la crearás en el Lab 03. Incluirla ahora en la política no causa ningún problema y te ahorra volver aquí.

### Sobre el principio de mínimo privilegio

Fíjate en que el rol **no** tiene `dynamodb:*` ni `Resource: "*"`. Solo puede insertar, consultar y leer, y únicamente sobre dos tablas concretas. Si la función tuviera un fallo o alguien lograra ejecutarla con datos maliciosos, no podría borrar otras tablas.

Es el mismo criterio del rol de Glue de la Sesión 1. Y recuerda la lección de aquel lab: los permisos de más son un riesgo, pero los de menos rompen el pipeline en la segunda ejecución. Concede exactamente lo que la función necesita hacer.

---

## Checkpoint

- [ ] Tabla `SignosVitales` en estado **Active**, con partition key `PacienteId` y sort key `Timestamp`
- [ ] Modo de capacidad **On-demand**
- [ ] Rol `LambdaSignosVitalesRole` con **dos** políticas: `AWSLambdaBasicExecutionRole` y `AccesoTablasSignosVitales`
- [ ] En la política inline aparece tu número de cuenta real, no el texto `TU-CUENTA`

Si todo está marcado, sigue con [Lab 01 — Lambda y DynamoDB](lab01_lambda_y_dynamodb.md).

---

## Problemas frecuentes

| Síntoma | Causa | Solución |
|---|---|---|
| No encuentro el use case "Lambda" en IAM | La lista es larga | Escribe "lambda" en el buscador de casos de uso |
| No sé cuál es mi número de cuenta | Está oculto en el menú superior | Clic en tu nombre, arriba a la derecha: aparece como *Account ID* |
| La tabla se queda en *Creating* | Es normal | Tarda unos segundos; refresca la lista |
| Puse mal la sort key | No se puede cambiar después | Borra la tabla y créala de nuevo: las claves son inmutables |
