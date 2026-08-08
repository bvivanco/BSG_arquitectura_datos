# Datasets de la Tarea 01

Elige **uno solo** de los tres archivos. Cada uno cubre un dominio distinto y plantea retos de limpieza diferentes, así que no hay una opción "más fácil": hay una que se ajusta mejor a lo que quieras practicar.

Los tres cumplen los requisitos del enunciado: más de 1.000 filas, al menos una columna de fecha y al menos una columna categórica que sirve como criterio de particionado.

| Archivo | Formato | Filas | Dominio |
|---|---|---|---|
| `ventas_retail.csv` | CSV | 2.040 | Ventas de una cadena retail, abril–junio 2025 |
| `sensores_iot.json` | JSON Lines | 2.535 | Lecturas de sensores industriales |
| `viajes_taxi.parquet` | Parquet | 3.000 | Viajes de taxi en Lima |

**Los tres contienen problemas de calidad deliberados.** Encontrarlos es parte de la tarea: antes de escribir una sola transformación, explora el dataset en Athena y documenta qué está mal.

---

## ventas_retail.csv

Ventas de cinco tiendas entre el 1 de abril y el 30 de junio de 2025.

```
venta_id,fecha_venta,tienda,categoria,producto,cantidad,precio_unitario,monto_total,metodo_pago
V000067,2025-06-16,Lima Centro,hogar,Organizador,1,229.23,229.23,Plin
V000204,2025-05-05,Trujillo, Electronica ,Monitor 24,2,403.85,807.70,Yape
```

| Columna | Descripción |
|---|---|
| `venta_id` | identificador de la venta |
| `fecha_venta` | fecha en formato `YYYY-MM-DD` |
| `tienda` | una de cinco tiendas |
| `categoria` | categoría del producto |
| `producto` | nombre del producto |
| `cantidad` | unidades vendidas |
| `precio_unitario` | precio por unidad |
| `monto_total` | total de la venta |
| `metodo_pago` | Efectivo, Tarjeta, Yape, Plin o Transferencia |

**Pistas de dónde mirar:** revisa los valores distintos de `categoria` con un `SELECT DISTINCT`; cuenta cuántos `venta_id` aparecen más de una vez; y fíjate en el rango de `monto_total` y `cantidad`.

---

## sensores_iot.json

Lecturas de sensores en tres plantas industriales. El archivo está en formato **JSON Lines**: un objeto JSON por línea, sin comas ni corchetes envolviendo el conjunto. Es el formato estándar en un data lake, y es el que Spark y el crawler de Glue leen sin configuración adicional.

```json
{"evento_id": "E000891", "timestamp_lectura": "2025-06-28T23:16:29", "sensor_id": "S-037", "ubicacion": {"planta": "Planta Callao", "linea": "L1"}, "tipo_sensor": "humedad", "valor": 91.21, "unidad": "pct", "bateria_pct": 44}
```

| Campo | Descripción |
|---|---|
| `evento_id` | identificador de la lectura |
| `timestamp_lectura` | fecha y hora en formato ISO 8601 |
| `sensor_id` | identificador del sensor físico |
| `ubicacion` | **objeto anidado** con `planta` y `linea` |
| `tipo_sensor` | temperatura, presion, humedad o vibracion |
| `valor` | la lectura |
| `unidad` | unidad de medida |
| `bateria_pct` | carga de batería del sensor |

**Lo que hace especial a este dataset:** `ubicacion` es un objeto anidado. Para particionar por planta primero tendrás que aplanarlo (`ubicacion.planta` se convierte en una columna propia). Es una transformación que solo aparece con JSON y vale la pena practicarla.

**Pistas:** mira los valores distintos de `unidad` para un mismo `tipo_sensor`; busca lecturas fuera de todo rango físico razonable; y revisa el rango de `bateria_pct`.

---

## viajes_taxi.parquet

Viajes de taxi en ocho distritos de Lima.

| Columna | Tipo en el archivo | Descripción |
|---|---|---|
| `viaje_id` | string | identificador del viaje |
| `fecha_hora_inicio` | **string** | inicio, en formato ISO |
| `fecha_hora_fin` | **string** | fin, en formato ISO |
| `zona_origen` | string | distrito de origen |
| `zona_destino` | string | distrito de destino |
| `distancia_km` | double | distancia recorrida |
| `duracion_min` | int64 | duración en minutos |
| `tarifa` | double | tarifa cobrada |
| `propina` | double | propina |
| `pasajeros` | int64 | número de pasajeros |
| `metodo_pago` | string | Efectivo, Tarjeta o App |

**Ojo con los tipos.** Parquet sí guarda tipos, a diferencia de CSV y JSON, así que el crawler no tiene que adivinar casi nada. Pero fíjate en que las dos columnas de fecha vienen como `string`: así se exportó desde el sistema de origen. Es un caso muy común en la práctica, y convertirlas es trabajo tuyo.

**Pistas:** busca viajes con distancia cero, duraciones que no puedan existir, y compara `fecha_hora_inicio` con `fecha_hora_fin`.

---

## Cómo subirlos a S3

Desde la consola de S3, entra a tu bucket, a la carpeta `raw/`, crea una subcarpeta con el nombre del dataset y sube el archivo dentro:

```
s3://<tu-bucket>/raw/ventas/ventas_retail.csv
```

Recuerda apuntar el crawler a la **carpeta** (`raw/ventas/`), nunca al archivo.

---

## Sobre el particionado

Los tres datasets abarcan 91 días. Si particionas por la fecha exacta te van a salir **91 carpetas** con unas pocas decenas de filas cada una, lo que en un data lake real se conoce como el *small files problem*: muchos archivos diminutos que hacen las consultas más lentas, no más rápidas.

Tienes alternativas: el mes, la tienda, la planta, la zona de origen, la categoría. **La decisión es tuya y se califica la justificación, no la opción.** Piensa en qué consultas vas a hacer después y elige la columna por la que realmente filtrarías.
