"""
Sesión 3 - Demo: pipeline batch con Apache Beam
================================================
Lee un CSV de ventas, aplica control de calidad y escribe el resultado en
BigQuery. Las filas que no pasan la validación no se descartan: se desvían
a una salida secundaria y terminan en una tabla de rechazos.

Ejecutar en local (DirectRunner), sin costo ni cluster:

    python pipeline_batch_beam.py \
        --input datos/ventas_sucursales.csv \
        --project MI-PROYECTO \
        --dataset ventas_demo \
        --temp_location gs://MI-BUCKET/temp

Ejecutar en Dataflow:

    python pipeline_batch_beam.py \
        --input gs://MI-BUCKET/raw/ventas_sucursales.csv \
        --project MI-PROYECTO \
        --dataset ventas_demo \
        --runner DataflowRunner \
        --region us-central1 \
        --temp_location gs://MI-BUCKET/temp \
        --staging_location gs://MI-BUCKET/staging

El mismo código sirve para los dos casos. Esa es la promesa de Beam: el
pipeline no sabe ni le importa quién lo ejecuta.
"""

import argparse
import csv
import datetime as dt
import logging

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

# ---------------------------------------------------------------------------
# Esquemas de las tablas de destino
# ---------------------------------------------------------------------------
ESQUEMA_CURADO = {
    "fields": [
        {"name": "venta_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "fecha_venta", "type": "DATE", "mode": "REQUIRED"},
        {"name": "sucursal", "type": "STRING", "mode": "REQUIRED"},
        {"name": "categoria", "type": "STRING", "mode": "REQUIRED"},
        {"name": "cantidad", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "monto_total", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "anio_mes", "type": "STRING", "mode": "REQUIRED"},
    ]
}

ESQUEMA_RECHAZOS = {
    "fields": [
        {"name": "linea_original", "type": "STRING", "mode": "NULLABLE"},
        {"name": "motivo", "type": "STRING", "mode": "NULLABLE"},
    ]
}

# Formatos de fecha que aceptamos. El CSV trae la mayoría en ISO, pero
# algunas filas vienen en formato peruano: hay que contemplarlo.
FORMATOS_FECHA = ("%Y-%m-%d", "%d/%m/%Y")


class ParsearYValidar(beam.DoFn):
    """Convierte una línea de CSV en un diccionario, o la rechaza.

    Las filas inválidas salen por una salida secundaria (side output) en
    lugar de descartarse. Es el equivalente en Beam a una Dead Letter Queue:
    nunca se descarta en silencio.
    """

    RECHAZOS = "rechazos"

    def __init__(self):
        self.leidas = Metrics.counter(self.__class__, "filas_leidas")
        self.validas = Metrics.counter(self.__class__, "filas_validas")
        self.rechazadas = Metrics.counter(self.__class__, "filas_rechazadas")

    def _rechazo(self, linea, motivo):
        self.rechazadas.inc()
        return beam.pvalue.TaggedOutput(
            self.RECHAZOS, {"linea_original": linea, "motivo": motivo}
        )

    def process(self, linea):
        self.leidas.inc()

        try:
            campos = next(csv.reader([linea]))
        except Exception:
            yield self._rechazo(linea, "linea ilegible")
            return

        if len(campos) != 6:
            yield self._rechazo(linea, "numero de columnas incorrecto")
            return

        venta_id, fecha_txt, sucursal, categoria, cantidad_txt, monto_txt = campos

        # --- conversión de tipos: en un CSV todo llega como texto ---
        fecha = None
        for formato in FORMATOS_FECHA:
            try:
                fecha = dt.datetime.strptime(fecha_txt.strip(), formato).date()
                break
            except ValueError:
                continue
        if fecha is None:
            yield self._rechazo(linea, "fecha con formato desconocido")
            return

        try:
            cantidad = int(cantidad_txt)
            monto = float(monto_txt)
        except ValueError:
            yield self._rechazo(linea, "cantidad o monto no numericos")
            return

        # --- reglas de negocio ---
        if monto <= 0:
            yield self._rechazo(linea, "monto no positivo")
            return
        if cantidad <= 0:
            yield self._rechazo(linea, "cantidad no positiva")
            return
        if not categoria.strip():
            yield self._rechazo(linea, "categoria vacia")
            return

        self.validas.inc()
        yield {
            "venta_id": venta_id.strip(),
            "fecha_venta": fecha.isoformat(),
            "sucursal": sucursal.strip(),
            # normalización: "  electronica " y "ELECTRONICA" son la misma cosa
            "categoria": categoria.strip().title(),
            "cantidad": cantidad,
            "monto_total": round(monto, 2),
            # columna derivada: sirve para particionar y para agrupar
            "anio_mes": fecha.strftime("%Y-%m"),
        }


def clave_de_deduplicacion(venta):
    """El venta_id identifica de forma única una venta."""
    return venta["venta_id"], venta


def ejecutar(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True,
                        help="Ruta del CSV, local o gs://")
    parser.add_argument("--dataset", default="ventas_demo",
                        help="Dataset de BigQuery de destino")
    parser.add_argument("--tabla_curada", default="ventas_curadas")
    parser.add_argument("--tabla_rechazos", default="ventas_rechazadas")
    conocidos, resto = parser.parse_known_args(argv)

    opciones = PipelineOptions(resto)
    opciones.view_as(SetupOptions).save_main_session = True
    proyecto = opciones.get_all_options().get("project")
    if not proyecto:
        raise SystemExit("Falta --project MI-PROYECTO")

    destino_curado = "%s:%s.%s" % (proyecto, conocidos.dataset, conocidos.tabla_curada)
    destino_rechazos = "%s:%s.%s" % (proyecto, conocidos.dataset, conocidos.tabla_rechazos)

    with beam.Pipeline(options=opciones) as p:
        # skip_header_lines evita que la cabecera entre como un registro más.
        # Es el error clásico al procesar CSV en Beam.
        lineas = p | "LeerCSV" >> beam.io.ReadFromText(
            conocidos.input, skip_header_lines=1
        )

        resultado = lineas | "ParsearYValidar" >> beam.ParDo(
            ParsearYValidar()
        ).with_outputs(ParsearYValidar.RECHAZOS, main="validas")

        validas = resultado.validas
        rechazos = resultado[ParsearYValidar.RECHAZOS]

        # Deduplicación: el CSV trae filas repetidas. En batch es sencillo
        # porque tenemos el conjunto completo delante; en streaming no lo
        # sería, porque los duplicados pueden llegar con horas de diferencia.
        deduplicadas = (
            validas
            | "ClaveVentaId" >> beam.Map(clave_de_deduplicacion)
            | "AgruparPorId" >> beam.GroupByKey()
            | "TomarPrimera" >> beam.Map(lambda kv: next(iter(kv[1])))
        )

        (
            deduplicadas
            | "EscribirCuradas" >> beam.io.WriteToBigQuery(
                destino_curado,
                schema=ESQUEMA_CURADO,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                additional_bq_parameters={
                    # Particionado por mes: menos bytes escaneados en las
                    # consultas que filtran por periodo.
                    "timePartitioning": {"type": "MONTH", "field": "fecha_venta"},
                    "clustering": {"fields": ["sucursal", "categoria"]},
                },
            )
        )

        (
            rechazos
            | "EscribirRechazos" >> beam.io.WriteToBigQuery(
                destino_rechazos,
                schema=ESQUEMA_RECHAZOS,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    ejecutar()
