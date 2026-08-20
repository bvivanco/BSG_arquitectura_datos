-- =====================================================================
-- Sesión 4 - Consultas de monitoreo sobre la telemetría en streaming
-- Reemplaza MI-PROYECTO por el id de tu proyecto.
-- =====================================================================

-- 1. ¿Están llegando datos? Ejecútala dos veces con unos segundos de
--    diferencia mientras el generador corre: el conteo debe subir.
SELECT COUNT(*) AS lecturas,
       MIN(timestamp) AS primera,
       MAX(timestamp) AS ultima
FROM `MI-PROYECTO.red_electrica.lecturas`;

-- 2. Transformadores en estado anómalo, lo más reciente primero.
SELECT transformador_id, voltaje, corriente, potencia_kw, estado, timestamp
FROM `MI-PROYECTO.red_electrica.lecturas`
WHERE estado != 'NORMAL'
ORDER BY timestamp DESC
LIMIT 20;

-- 3. Reparto de estados. Con el generador por defecto, alrededor del 8%
--    de las lecturas deberían salir fuera de rango.
SELECT estado,
       COUNT(*) AS lecturas,
       ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS porcentaje
FROM `MI-PROYECTO.red_electrica.lecturas`
GROUP BY estado
ORDER BY lecturas DESC;

-- 4. Latencia real del pipeline: cuánto tardó cada lectura desde que se
--    generó hasta que quedó consultable. Esto es lo que convierte el
--    "tiempo real" en un número concreto que se puede enseñar.
SELECT transformador_id,
       timestamp AS momento_lectura,
       TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), timestamp, SECOND) AS segundos_desde_lectura
FROM `MI-PROYECTO.red_electrica.lecturas`
ORDER BY timestamp DESC
LIMIT 10;

-- 5. Los promedios por ventana que calculó el pipeline de Beam.
--    Fíjate en que cada transformador tiene una fila por cada ventana
--    de 60 segundos: eso es una agregación tumbling.
SELECT ventana_inicio, transformador_id, voltaje_promedio, lecturas
FROM `MI-PROYECTO.red_electrica.promedios_ventana`
ORDER BY ventana_inicio DESC, transformador_id
LIMIT 30;

-- 6. Comparación útil para cerrar la sesión: el promedio calculado en la
--    ventana por Beam frente al promedio calculado ahora sobre la tabla
--    completa. El primero está disponible al instante; el segundo obliga
--    a escanear todo el histórico cada vez que alguien pregunta.
SELECT transformador_id,
       ROUND(AVG(voltaje), 2) AS voltaje_promedio_historico,
       COUNT(*) AS lecturas_totales
FROM `MI-PROYECTO.red_electrica.lecturas`
GROUP BY transformador_id
ORDER BY transformador_id;
