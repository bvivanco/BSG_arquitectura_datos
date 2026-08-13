#!/usr/bin/env bash
# Envia lecturas de pulso al endpoint de API Gateway para probar el pipeline.
#
#   ./enviar_pulsos.sh https://xxxx.execute-api.us-east-1.amazonaws.com/dev/pulsaciones  20
#
# El segundo argumento (opcional) es cuántas lecturas enviar. Por defecto 10.
# Alrededor de un 20% de las lecturas salen fuera del rango normal a propósito,
# para que el Lab 03 tenga alertas que detectar.

set -euo pipefail

URL="${1:?Falta la URL del endpoint}"
CANTIDAD="${2:-10}"
PACIENTES=("P001" "P002" "P003")

for ((i = 1; i <= CANTIDAD; i++)); do
    paciente="${PACIENTES[$((RANDOM % ${#PACIENTES[@]}))]}"

    if (( RANDOM % 5 == 0 )); then
        pulso=$((130 + RANDOM % 60))     # taquicardia
    else
        pulso=$((60 + RANDOM % 40))      # normal
    fi

    printf '%2d/%d  %s  pulso=%s  -> ' "$i" "$CANTIDAD" "$paciente" "$pulso"
    curl -s -o /dev/null -w '%{http_code}\n' \
        -X POST "$URL" \
        -H 'Content-Type: application/json' \
        -d "{\"PacienteId\":\"$paciente\",\"pulso\":$pulso,\"origen\":\"reloj-demo\"}"

    sleep 0.3
done

echo "Listo. Revisa la tabla SignosVitales en la consola de DynamoDB."
