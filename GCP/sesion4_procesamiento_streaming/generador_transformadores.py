"""
Sesión 4 - Generador de telemetría para Pub/Sub
================================================
Simula transformadores de una red eléctrica publicando lecturas de voltaje
y corriente. Cada mensaje es un JSON codificado en UTF-8.

Uso:

    python generador_transformadores.py \
        --project MI-PROYECTO \
        --topic telemetria-red \
        --mensajes 50 \
        --intervalo 2

Con --mensajes 0 publica indefinidamente hasta que lo cortes con Ctrl+C,
que es lo cómodo para una demo en vivo.

En Cloud Shell las credenciales ya están resueltas. En tu máquina hace falta:
    gcloud auth application-default login
    pip install google-cloud-pubsub
"""

import argparse
import json
import random
import signal
import sys
import time
from datetime import datetime, timezone

from google.cloud import pubsub_v1

TRANSFORMADORES = ["TR-101", "TR-102", "TR-103", "TR-104", "TR-105"]

# Rangos de operación normal
VOLTAJE_NOMINAL = 220.0
CORRIENTE_MIN, CORRIENTE_MAX = 5.0, 60.0

_detener = False


def _manejar_señal(signum, frame):
    global _detener
    _detener = True
    print("\nDeteniendo el generador...")


def generar_lectura():
    """Devuelve una lectura. Un 8% de las veces sale fuera de rango, para
    que la demo tenga anomalías reales que detectar."""
    transformador = random.choice(TRANSFORMADORES)

    if random.random() < 0.08:
        # Anomalía: caída o pico de tensión
        voltaje = random.choice([
            round(random.uniform(150.0, 190.0), 2),   # subtensión
            round(random.uniform(250.0, 285.0), 2),   # sobretensión
        ])
        corriente = round(random.uniform(70.0, 95.0), 2)
    else:
        voltaje = round(random.gauss(VOLTAJE_NOMINAL, 3.5), 2)
        corriente = round(random.uniform(CORRIENTE_MIN, CORRIENTE_MAX), 2)

    return {
        "transformador_id": transformador,
        "voltaje": voltaje,
        "corriente": corriente,
        # timezone.utc explícito: datetime.utcnow() está obsoleto
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True, help="ID del proyecto de GCP")
    parser.add_argument("--topic", default="telemetria-red", help="Nombre del tema")
    parser.add_argument("--mensajes", type=int, default=50,
                        help="Cuántos publicar. 0 = sin límite")
    parser.add_argument("--intervalo", type=float, default=2.0,
                        help="Segundos entre mensajes")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, _manejar_señal)

    publicador = pubsub_v1.PublisherClient()
    ruta_tema = publicador.topic_path(args.project, args.topic)

    print("Publicando en %s" % ruta_tema)
    print("Ctrl+C para detener\n")

    enviados = 0
    pendientes = []
    try:
        while not _detener:
            if args.mensajes and enviados >= args.mensajes:
                break

            lectura = generar_lectura()
            datos = json.dumps(lectura).encode("utf-8")

            # publish() es asíncrono: devuelve un future, no espera al servidor
            futuro = publicador.publish(ruta_tema, datos)
            pendientes.append(futuro)
            enviados += 1

            potencia = lectura["voltaje"] * lectura["corriente"] / 1000
            marca = "  <-- fuera de rango" if not 200 <= lectura["voltaje"] <= 240 else ""
            print("%4d  %s  %6.2f V  %5.2f A  %6.2f kW%s"
                  % (enviados, lectura["transformador_id"], lectura["voltaje"],
                     lectura["corriente"], potencia, marca))

            time.sleep(args.intervalo)
    finally:
        # Esperar a los mensajes en vuelo antes de salir. Sin esto, un script
        # que termina de golpe puede perder los últimos publicados.
        for futuro in pendientes:
            futuro.result()
        print("\nPublicados %d mensajes." % enviados)


if __name__ == "__main__":
    sys.exit(main())
