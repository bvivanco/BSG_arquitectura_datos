# Lab 00 — Preparación del entorno

**Duración:** ~20 minutos
**Objetivo:** dejar listos el bucket S3, el rol IAM y Athena para poder hacer los labs 01 y 02.

Este lab no produce ningún dato interesante. Es plomería. Pero si algo falla aquí, todo lo demás falla después con errores confusos, así que vale la pena hacerlo con calma.

---

## Antes de empezar

- Cuenta de AWS con permisos para crear recursos de **S3, Glue, Athena, IAM y CloudWatch**.
- Trabaja siempre en la región **`us-east-1` (N. Virginia)**. Verifica el selector de región arriba a la derecha de la consola **antes de cada paso**. Un recurso creado en otra región simplemente "no existe" para los demás.

> **El error #1 del curso:** crear el bucket en una región y el crawler en otra. Si algo no aparece donde debería, lo primero que hay que revisar es la región.

---

## Paso 1 — Crear el bucket S3

Los nombres de bucket en S3 son **únicos a nivel mundial**, así que no puedes usar el mismo que tu compañero. Usa tus iniciales o un número al final.

1. Consola de AWS → **S3** → **Create bucket**
2. **Bucket name:** `bsg-glue-lab-TUSINICIALES` (ejemplo: `bsg-glue-lab-bv01`)
3. **AWS Region:** `us-east-1`
4. Deja todo lo demás por defecto (Block all public access **activado**)
5. **Create bucket**

Anota el nombre de tu bucket. Lo vas a escribir muchas veces:

```
Mi bucket: s3://________________________________
```

### Crear la estructura de carpetas

Entra al bucket y crea estas cuatro carpetas con **Create folder**:

| Carpeta | Para qué sirve |
|---|---|
| `raw/` | datos crudos, tal como llegan de origen |
| `curated/` | datos limpios y transformados, listos para consumir |
| `scripts/` | donde Glue guarda el código de los Jobs |
| `athena-results/` | donde Athena deja el resultado de cada consulta |

Dentro de `raw/`, crea una subcarpeta `transacciones/`.

> En S3 las "carpetas" no existen de verdad: son un prefijo en el nombre del objeto. Pero la consola te deja crearlas y ayuda mucho a organizarse.

---

## Paso 2 — Subir el dataset

Descarga [`data/transacciones.csv`](data/transacciones.csv) de este repositorio y súbelo:

1. S3 → tu bucket → `raw/` → `transacciones/`
2. **Upload** → **Add files** → selecciona `transacciones.csv` → **Upload**

Debe quedar exactamente en:

```
s3://bsg-glue-lab-TUSINICIALES/raw/transacciones/transacciones.csv
```

### Mira el dataset antes de seguir

```csv
transaction_id,customer_id,amount,transaction_date
1001,C001,150.50,2025-01-05
1002,C002,-45.00,2025-01-05     <- monto negativo
1003,C003,320.75,2025-01-06
1004,C001,-12.30,2025-01-06     <- monto negativo
...
```

Son **10 transacciones**, de las cuales **3 tienen montos negativos**. Están puestos a propósito: son el "dato sucio" que vamos a limpiar en el Lab 02.

Fíjate también en que `transaction_date` es texto. En un CSV **todo** es texto — no hay tipos. Convertirlo a fecha real será parte del trabajo del ETL.

---

## Paso 3 — Crear el rol IAM para Glue

Glue no puede leer tu bucket por sí solo. Necesita un rol que le dé permiso.

1. Consola → **IAM** → **Roles** → **Create role**
2. **Trusted entity type:** AWS service
3. **Use case:** busca y selecciona **Glue** → **Next**
4. **Permissions:** busca y marca **`AWSGlueServiceRole`** → **Next**
5. **Role name:** `AWSGlueServiceRole-Lab` → **Create role**

### Agregar los permisos sobre tu bucket

El rol ya puede usar Glue, pero todavía no puede tocar *tu* bucket. Hay que decírselo:

1. Entra al rol recién creado → pestaña **Permissions**
2. **Add permissions** → **Create inline policy**
3. Pestaña **JSON** → borra lo que haya y pega esto (**cambia el nombre del bucket en las dos líneas**):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::bsg-glue-lab-TUSINICIALES",
        "arn:aws:s3:::bsg-glue-lab-TUSINICIALES/*"
      ]
    }
  ]
}
```

4. **Next** → **Policy name:** `S3AccesoBucketLab` → **Create policy**

> **`s3:DeleteObject` no es opcional.** En el Lab 02 vas a escribir con `mode("overwrite")`, y "overwrite" en Spark significa *borrar todo y volver a escribir*. Sin este permiso la **primera** ejecución funciona (la carpeta está vacía, no hay nada que borrar) y **todas las siguientes fallan** con `Failed to delete key: curated/transacciones`. Es un error real de producción y cuesta mucho encontrarlo si no sabes que existe.

**¿Por qué las dos líneas en `Resource`?** La primera (`...:::bucket`) es el bucket como contenedor — la necesita `ListBucket`. La segunda (`...:::bucket/*`) son los objetos dentro — las necesitan `GetObject`, `PutObject` y `DeleteObject`. Si pones solo una, la mitad de las operaciones falla.

---

## Paso 4 — Configurar Athena

Athena necesita saber dónde dejar los resultados de las consultas. Si no se lo dices, la primera consulta falla y parece un error del pipeline.

1. Consola → **Athena** → **Query editor**
2. Pestaña **Settings** → **Manage**
3. **Location of query result:** `s3://bsg-glue-lab-TUSINICIALES/athena-results/`
4. **Save**

---

## Checkpoint — antes de pasar al Lab 01

Verifica que tienes las cuatro cosas:

- [ ] Bucket creado en `us-east-1` con las carpetas `raw/transacciones/`, `curated/`, `scripts/`, `athena-results/`
- [ ] `transacciones.csv` visible en `s3://TU-BUCKET/raw/transacciones/`
- [ ] Rol `AWSGlueServiceRole-Lab` con **dos** políticas: `AWSGlueServiceRole` (managed) + `S3AccesoBucketLab` (inline)
- [ ] Athena con el *query result location* configurado

Si tienes los cuatro marcados, sigue con [Lab 01 — Crawler y Data Catalog](lab01_crawler_y_data_catalog.md).

---

## Problemas frecuentes

| Síntoma | Causa | Solución |
|---|---|---|
| "Bucket name already exists" | Los nombres son globales | Agrega números o tus iniciales |
| No veo mi bucket en Glue | Estás en otra región | Cambia a `us-east-1` arriba a la derecha |
| No encuentro el use case "Glue" en IAM | La lista es larga | Escribe "glue" en el buscador de casos de uso |
| El rol no aparece luego al crear el crawler | El nombre no empieza con `AWSGlueServiceRole` | Glue filtra por ese prefijo — renómbralo |
