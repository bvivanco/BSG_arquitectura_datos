"""
Sesión 2 - Lab 01: Lambda de ingesta
-------------------------------------
Recibe una lectura de pulso, la valida y la guarda en DynamoDB.

Funciona en dos escenarios:
  - Invocada desde la consola de Lambda con un evento de prueba (dict directo).
  - Invocada por API Gateway con Lambda Proxy Integration (el cuerpo llega
    como CADENA de texto dentro de event["body"]).

Runtime: Python 3.12 (boto3 ya viene incluido, no hay que empaquetar nada).
"""

import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import boto3

TABLA = "SignosVitales"

# Rango fisiológicamente posible. Fuera de esto, el dato es basura de sensor.
PULSO_MIN = 20
PULSO_MAX = 250

dynamodb = boto3.resource("dynamodb")
tabla = dynamodb.Table(TABLA)


def respuesta(codigo, cuerpo):
    """Formato que exige API Gateway con Lambda Proxy Integration.

    Los tres campos son obligatorios y 'body' DEBE ser una cadena.
    Devolver un diccionario en 'body' produce un error 502.
    """
    return {
        "statusCode": codigo,
        "headers": {"Content-Type": "application/json"},
        # default=str porque Decimal no es serializable por json
        "body": json.dumps(cuerpo, default=str, ensure_ascii=False),
    }


def lambda_handler(event, context):
    # ------------------------------------------------------------------
    # 1. Obtener el cuerpo, venga de donde venga
    # ------------------------------------------------------------------
    cuerpo = event.get("body")
    if isinstance(cuerpo, str):
        try:
            # parse_float=Decimal es imprescindible: DynamoDB NO acepta
            # los float de Python. Sin esto, put_item falla con
            # "Float types are not supported. Use Decimal types instead."
            datos = json.loads(cuerpo, parse_float=Decimal)
        except json.JSONDecodeError:
            return respuesta(400, {"error": "el cuerpo no es JSON valido"})
    else:
        # invocación directa desde la consola de Lambda
        datos = cuerpo if isinstance(cuerpo, dict) else event

    # ------------------------------------------------------------------
    # 2. Validar (Bloque 2 de la sesión)
    # ------------------------------------------------------------------
    faltantes = [c for c in ("PacienteId", "pulso") if c not in datos]
    if faltantes:
        return respuesta(400, {"error": "faltan campos obligatorios",
                               "campos": faltantes})

    try:
        pulso = Decimal(str(datos["pulso"]))
    except (InvalidOperation, TypeError):
        return respuesta(400, {"error": "pulso no es un numero",
                               "valor_recibido": datos["pulso"]})

    if not PULSO_MIN <= pulso <= PULSO_MAX:
        return respuesta(400, {"error": "pulso fuera de rango fisiologico",
                               "valor": pulso,
                               "rango_valido": "%d-%d" % (PULSO_MIN, PULSO_MAX)})

    # ------------------------------------------------------------------
    # 3. Enriquecer y guardar
    # ------------------------------------------------------------------
    item = {
        "PacienteId": str(datos["PacienteId"]),
        # El timestamp lo pone el servidor, no el cliente: un reloj mal
        # configurado no debe poder ensuciar el orden de los datos.
        "Timestamp": datetime.now(timezone.utc).isoformat(),
        "pulso": pulso,
        "origen": str(datos.get("origen", "desconocido")),
    }
    tabla.put_item(Item=item)

    print("[INGESTA] guardado %s pulso=%s" % (item["PacienteId"], pulso))
    return respuesta(201, {"mensaje": "lectura registrada", "registro": item})
