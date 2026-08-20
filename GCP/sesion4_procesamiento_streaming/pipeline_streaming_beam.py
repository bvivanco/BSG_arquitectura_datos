"""
Sesión 4 - Demo: pipeline de streaming con Apache Beam
=======================================================
Lee la telemetría de Pub/Sub, la valida, calcula la potencia y escribe en
BigQuery. En paralelo agrega el voltaje promedio por transformador en
ventanas de 60 segundos y lo deja en una segunda tabla.

Este pipeline es la alternativa a la suscripción directa a BigQuery. La
suscripción directa es más simple y más barata, pero solo mueve el dato tal
como llega. En cuanto hay que transformar, validar o agregar, hace falta
un procesador en el medio: eso es lo que muestra este archivo.

Ejecutar en local (DirectRunner):

    python pipeline_streaming_beam.py \
        --project MI-PROYECTO \
        --subscription projects/MI-PROYECTO/subscriptions/telemetria-beam \
        --dataset red_electrica \
        --temp_location gs://MI-BUCKET/temp \
        --streaming

Ejecutar en Dataflow:

    python pipeline_streaming_beam.py \
        --project MI-PROYECTO \
        --subscription projects/MI-PROYECTO/subscriptions/telemetria-beam \
        --dataset red_electrica \
        --runner DataflowRunner \
        --region us-central1 \
        --temp_location gs://MI-BUCKET/temp \
        --staging_location gs://MI-BUCKET/staging \
        --streaming

Un pipeline de streaming NO termina solo. Hay que detenerlo a mano: Ctrl+C
en local, o "Stop job" en la consola de Dataflow. Es la diferencia más
visible frente al pipeline batch de la Sesión 3.
"""

import argparse
import json
import logging

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.transforms import window

VOLTAJE_MIN, VOLTAJE_MAX = 200.0, 240.0

ESQUEMA_LECTURAS = {
    "fields": [
        {"name": "transformador_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "voltaje", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "corriente", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "potencia_kw", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "estado", "type": "STRING", "mode": "REQUIRED"},
        {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

ESQUEMA_AGREGADOS = {
    "fields": [
        {"name": "transformador_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "voltaje_promedio", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "lecturas", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "ventana_inicio", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "ventana_fin", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}


class ParsearLectura(beam.DoFn):
    """Convierte los bytes del mensaje en un diccionario validado.

    Los mensajes que no se pueden interpretar salen por una salida
    secundaria en lugar de romper el pipeline. En streaming esto importa
    más que en batch: una excepción no controlada detiene el flujo entero.
    """

    RECHAZOS = "rechazos"

    def __init__(self):
        self.recibidos = Metrics.counter(self.__class__, "mensajes_recibidos")
        self.validos = Metrics.counter(self.__class__, "mensajes_validos")
        self.rechazados = Metrics.counter(self.__class__, "mensajes_rechazados")
        self.anomalias = Metrics.counter(self.__class__, "anomalias_detectadas")

    def _rechazo(self, crudo, motivo):
        self.rechazados.inc()
        return beam.pvalue.TaggedOutput(
            self.RECHAZOS, json.dumps({"mensaje": crudo, "motivo": motivo})
        )

    def process(self, mensaje):
        self.recibidos.inc()
        crudo = mensaje.decode("utf-8", errors="replace")

        try:
            datos = json.loads(crudo)
        except json.JSONDecodeError:
            yield self._rechazo(crudo, "no es JSON valido")
            return

        faltantes = [c for c in ("transformador_id", "voltaje", "corriente", "timestamp")
                     if c not in datos]
        if faltantes:
            yield self._rechazo(crudo, "faltan campos: %s" % ",".join(faltantes))
            return

        try:
            voltaje = float(datos["voltaje"])
            corriente = float(datos["corriente"])
        except (TypeError, ValueError):
            yield self._rechazo(crudo, "voltaje o corriente no numericos")
            return

        if voltaje <= 0 or corriente < 0:
            yield self._rechazo(crudo, "lectura fisicamente imposible")
            return

        # Columna derivada: la potencia no viene en el mensaje, se calcula.
        # Esta es exactamente la razón de tener un procesador en el medio.
        potencia_kw = round(voltaje * corriente / 1000.0, 3)

        if voltaje < VOLTAJE_MIN:
            estado = "SUBTENSION"
            self.anomalias.inc()
        elif voltaje > VOLTAJE_MAX:
            estado = "SOBRETENSION"
            self.anomalias.inc()
        else:
            estado = "NORMAL"

        self.validos.inc()
        yield {
            "transformador_id": str(datos["transformador_id"]),
            "voltaje": voltaje,
            "corriente": corriente,
            "potencia_kw": potencia_kw,
            "estado": estado,
            "timestamp": datos["timestamp"],
        }


class FormatearAgregado(beam.DoFn):
    """Añade los límites de la ventana al resultado agregado.

    El objeto window llega como parámetro con DoFn.WindowParam: es la forma
    de saber a qué intervalo de tiempo corresponde cada agregación.
    """

    def process(self, elemento, ventana=beam.DoFn.WindowParam):
        transformador, estadisticas = elemento
        yield {
            "transformador_id": transformador,
            "voltaje_promedio": round(estadisticas["suma"] / estadisticas["conteo"], 2),
            "lecturas": estadisticas["conteo"],
            "ventana_inicio": ventana.start.to_utc_datetime().isoformat(),
            "ventana_fin": ventana.end.to_utc_datetime().isoformat(),
        }


def acumular(lecturas):
    suma = 0.0
    conteo = 0
    for lectura in lecturas:
        suma += lectura["voltaje"]
        conteo += 1
    return {"suma": suma, "conteo": conteo}


def ejecutar(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--subscription", required=True,
                        help="projects/MI-PROYECTO/subscriptions/NOMBRE")
    parser.add_argument("--dataset", default="red_electrica")
    parser.add_argument("--tabla_lecturas", default="lecturas")
    parser.add_argument("--tabla_agregados", default="promedios_ventana")
    parser.add_argument("--ventana_segundos", type=int, default=60)
    conocidos, resto = parser.parse_known_args(argv)

    opciones = PipelineOptions(resto)
    opciones.view_as(SetupOptions).save_main_session = True
    proyecto = opciones.get_all_options().get("project")
    if not proyecto:
        raise SystemExit("Falta --project MI-PROYECTO")

    destino_lecturas = "%s:%s.%s" % (proyecto, conocidos.dataset, conocidos.tabla_lecturas)
    destino_agregados = "%s:%s.%s" % (proyecto, conocidos.dataset, conocidos.tabla_agregados)

    with beam.Pipeline(options=opciones) as p:
        mensajes = p | "LeerPubSub" >> beam.io.ReadFromPubSub(
            subscription=conocidos.subscription
        )

        resultado = mensajes | "Parsear" >> beam.ParDo(
            ParsearLectura()
        ).with_outputs(ParsearLectura.RECHAZOS, main="validas")

        lecturas = resultado.validas
        rechazos = resultado[ParsearLectura.RECHAZOS]

        # --- Rama 1: cada lectura individual a BigQuery ---
        (
            lecturas
            | "EscribirLecturas" >> beam.io.WriteToBigQuery(
                destino_lecturas,
                schema=ESQUEMA_LECTURAS,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

        # --- Rama 2: promedio por transformador en ventanas fijas ---
        # Una ventana fija (tumbling) agrupa los eventos en bloques de tiempo
        # que no se solapan: 00:00-00:01, 00:01-00:02, y así.
        (
            lecturas
            | "Ventana" >> beam.WindowInto(
                window.FixedWindows(conocidos.ventana_segundos)
            )
            | "ClavePorTransformador" >> beam.Map(
                lambda l: (l["transformador_id"], l)
            )
            | "AgruparPorTransformador" >> beam.GroupByKey()
            | "Acumular" >> beam.Map(lambda kv: (kv[0], acumular(kv[1])))
            | "Formatear" >> beam.ParDo(FormatearAgregado())
            | "EscribirAgregados" >> beam.io.WriteToBigQuery(
                destino_agregados,
                schema=ESQUEMA_AGREGADOS,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

        # --- Rama 3: los mensajes que no se pudieron interpretar ---
        # En una demo basta con verlos en el log. En producción irían a un
        # dead letter topic o a una tabla de rechazos.
        (
            rechazos
            | "LogRechazos" >> beam.Map(
                lambda r: logging.warning("Mensaje rechazado: %s", r)
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    ejecutar()
