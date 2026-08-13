"""
Sesión 2 - Lab 02: Lambda de consulta
--------------------------------------
Devuelve las últimas lecturas de un paciente.

Se invoca desde API Gateway con:  GET /pacientes/{PacienteId}?limite=10

El punto importante de este script es que usa Query y no Scan. Query lee
solo la partición del paciente pedido; Scan recorrería la tabla entera.
Eso solo es posible porque PacienteId es la partition key: si la clave
estuviera mal elegida, no habría forma de hacer esta consulta de forma
eficiente. Ver diapositiva 28.
"""

import json

import boto3
from boto3.dynamodb.conditions import Key

TABLA = "SignosVitales"
LIMITE_POR_DEFECTO = 10
LIMITE_MAXIMO = 100

dynamodb = boto3.resource("dynamodb")
tabla = dynamodb.Table(TABLA)


def respuesta(codigo, cuerpo):
    return {
        "statusCode": codigo,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(cuerpo, default=str, ensure_ascii=False),
    }


def lambda_handler(event, context):
    # El valor de {PacienteId} de la ruta llega en pathParameters
    parametros_ruta = event.get("pathParameters") or {}
    paciente = parametros_ruta.get("PacienteId")
    if not paciente:
        return respuesta(400, {"error": "falta PacienteId en la ruta"})

    parametros_query = event.get("queryStringParameters") or {}
    try:
        limite = int(parametros_query.get("limite", LIMITE_POR_DEFECTO))
    except ValueError:
        return respuesta(400, {"error": "el parametro limite no es un numero"})
    limite = max(1, min(limite, LIMITE_MAXIMO))

    resultado = tabla.query(
        KeyConditionExpression=Key("PacienteId").eq(paciente),
        # False = orden descendente por sort key, o sea las más recientes primero
        ScanIndexForward=False,
        Limit=limite,
    )

    items = resultado.get("Items", [])
    print("[CONSULTA] paciente=%s devueltos=%d" % (paciente, len(items)))

    if not items:
        return respuesta(404, {"mensaje": "sin lecturas para ese paciente",
                               "PacienteId": paciente})

    return respuesta(200, {"PacienteId": paciente,
                           "total": len(items),
                           "lecturas": items})
