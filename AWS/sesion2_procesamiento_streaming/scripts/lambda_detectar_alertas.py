"""
Sesión 2 - Lab 03: Lambda de alertas (disparada por DynamoDB Streams)
----------------------------------------------------------------------
Se activa con cada cambio en la tabla SignosVitales y, si detecta un pulso
fuera del rango normal, escribe una alerta en una tabla DISTINTA.

ATENCIÓN - por qué escribe en otra tabla:
Si esta función escribiera de vuelta en SignosVitales, su propia escritura
generaría un nuevo evento en el stream, que volvería a invocarla, y así
indefinidamente. Es un bucle infinito que factura hasta que alguien lo note.
La regla: una Lambda disparada por un stream NUNCA escribe en la tabla que
la dispara. Ver diapositiva 29.

Los registros del stream llegan en formato DynamoDB JSON, donde cada valor
viene envuelto en su tipo: {"pulso": {"N": "145"}}. Por eso hace falta
deserializarlos antes de usarlos.
"""

import json
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from boto3.dynamodb.types import TypeDeserializer

TABLA_ALERTAS = "AlertasSignosVitales"

# Rango considerado normal en reposo. Fuera de esto, se genera alerta.
PULSO_NORMAL_MIN = 50
PULSO_NORMAL_MAX = 120

dynamodb = boto3.resource("dynamodb")
tabla_alertas = dynamodb.Table(TABLA_ALERTAS)
deserializador = TypeDeserializer()


def a_dict(imagen):
    """Convierte DynamoDB JSON a un dict de Python normal."""
    return {k: deserializador.deserialize(v) for k, v in imagen.items()}


def clasificar(pulso):
    if pulso > PULSO_NORMAL_MAX:
        return "TAQUICARDIA"
    if pulso < PULSO_NORMAL_MIN:
        return "BRADICARDIA"
    return None


def lambda_handler(event, context):
    # Lambda recibe un LOTE de registros, no uno solo. Ver diapositiva 25.
    registros = event.get("Records", [])
    alertas = 0

    for registro in registros:
        # Solo interesan las inserciones nuevas
        if registro.get("eventName") != "INSERT":
            continue

        imagen = registro["dynamodb"].get("NewImage")
        if not imagen:
            continue

        item = a_dict(imagen)
        pulso = item.get("pulso")
        if pulso is None:
            continue

        estado = clasificar(Decimal(str(pulso)))
        if estado is None:
            continue

        tabla_alertas.put_item(Item={
            "PacienteId": item["PacienteId"],
            "Timestamp": item["Timestamp"],
            "pulso": Decimal(str(pulso)),
            "estado": estado,
            "detectada_en": datetime.now(timezone.utc).isoformat(),
        })
        alertas += 1
        print("[ALERTA] %s %s pulso=%s" % (item["PacienteId"], estado, pulso))

    print("[ALERTAS] lote de %d registros, %d alertas generadas"
          % (len(registros), alertas))
    # El valor de retorno no se usa: nadie está esperando esta respuesta.
    return {"procesados": len(registros), "alertas": alertas}
