# Sesión 1 — Procesamiento Batch en AWS con Glue

Laboratorios prácticos de la Sesión 1 del curso **Arquitectura de Datos en la Nube con Python**.

Vas a construir un pipeline ETL batch completo sobre AWS, desde un CSV crudo hasta una tabla consultable con SQL.

---

## El pipeline que vas a construir

```
  transacciones.csv         Crawler RAW          Glue Job (PySpark)        Crawler CURATED
        │                        │                       │                        │
        ▼                        ▼                       ▼                        ▼
  s3://…/raw/  ──────►  demo_batch_db.      ──────►  s3://…/curated/  ──────►  demo_batch_db.
                        transacciones                Parquet +                curated_
                        (Data Catalog)              particiones               transacciones
                                                                                   │
                                                                                   ▼
                                                                                Athena
                                                                              (consultas SQL)
```

---

## Los laboratorios

| # | Lab | Duración | Qué construyes |
|---|---|---|---|
| 00 | [Preparación del entorno](lab00_preparacion_entorno.md) | ~20 min | Bucket S3, rol IAM, Athena configurado |
| 01 | [Crawler y Data Catalog](lab01_crawler_y_data_catalog.md) | ~25 min | Catalogar el CSV crudo y consultarlo con SQL |
| 02 | [Glue Job + segundo Crawler](lab02_glue_job_y_crawler_curated.md) | ~35 min | ETL en PySpark, Parquet particionado, capa curated |

**Hazlos en orden.** Cada uno usa lo que construyó el anterior.

---

## Contenido de esta carpeta

```
sesion1_procesamiento_batch/
├── Sesion1_Detalles.md                              este archivo
├── lab00_preparacion_entorno.md
├── lab01_crawler_y_data_catalog.md
├── lab02_glue_job_y_crawler_curated.md
├── data/
│   └── transacciones.csv                  dataset de entrada (10 filas)
└── scripts/
    └── etl_transacciones.py               código PySpark del Lab 02
```

---

## Servicios de AWS que vas a usar

| Servicio | Para qué |
|---|---|
| **S3** | Almacenamiento de los datos (zonas `raw` y `curated`) |
| **Glue Crawler** | Detectar el esquema y registrarlo en el catálogo |
| **Glue Data Catalog** | El "índice" que hace consultables los archivos de S3 |
| **Glue Job (PySpark)** | Limpiar y transformar los datos |
| **Athena** | Consultar con SQL directamente sobre S3 |
| **CloudWatch Logs** | Ver qué pasó dentro del Job |
| **IAM** | Dar permisos a Glue para leer y escribir tu bucket |

---

## Antes de empezar

- **Región:** trabaja siempre en **`us-east-1` (N. Virginia)**. Verifica el selector arriba a la derecha de la consola antes de cada paso.
- **Costo:** unos pocos centavos de dólar si completas los tres labs y haces la limpieza al final. Glue cobra por tiempo de ejecución y Athena por bytes escaneados; con este dataset ambos son mínimos.
- **Limpieza:** al terminar, sigue la sección de limpieza al final del [Lab 02](lab02_glue_job_y_crawler_curated.md#limpieza--no-te-saltes-esto). Los recursos olvidados generan cobros.

---

## Las tres ideas que debes llevarte

**1. Escribir archivos ≠ crear una tabla.**
Puedes tener Parquet perfectamente escrito en S3 y que Athena diga que la tabla no existe. Los datos y los metadatos son dos cosas separadas. El Lab 02 te lo hace ver en vivo.

**2. El crawler adivina, no sabe.**
Infiere el esquema con heurísticas. Por eso una fecha en un CSV termina como `string`: ante la duda, elige lo seguro. Un esquema definido por tu código siempre será más preciso.

**3. Cataloga con crawler lo que no controlas; cataloga desde el job lo que sí controlas.**
La zona `raw` recibe datos de fuera → crawler. La zona `curated` la escribe tu propio pipeline → el job puede catalogarla solo, con `enableUpdateCatalog`, y te ahorras un paso entero.

---

## Si algo falla

Cada lab tiene su tabla de **Problemas frecuentes** al final. Los dos errores que más aparecen:

| Error | Dónde está la solución |
|---|---|
| `Failed to delete key: curated/transacciones` (falla solo desde la 2ª ejecución) | [Lab 00, paso 3](lab00_preparacion_entorno.md#paso-3--crear-el-rol-iam-para-glue) — falta `s3:DeleteObject` |
| La tabla de raw apunta a `curated/` y el Job se rompe | [Lab 02, paso 6](lab02_glue_job_y_crawler_curated.md#paso-6--crear-el-segundo-crawler) — falta el prefijo `curated_` |
