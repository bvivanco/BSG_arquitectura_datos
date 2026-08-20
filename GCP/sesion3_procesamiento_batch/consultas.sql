-- =====================================================================
-- Sesión 3 - Consultas de verificación sobre las tablas resultantes
-- Reemplaza MI-PROYECTO por el id de tu proyecto.
-- =====================================================================

-- 1. ¿Cuántas filas sobrevivieron y cuántas se rechazaron?
--    Las dos cifras deben sumar el total de líneas del CSV original.
SELECT 'curadas' AS tabla, COUNT(*) AS filas
FROM `MI-PROYECTO.ventas_demo.ventas_curadas`
UNION ALL
SELECT 'rechazadas', COUNT(*)
FROM `MI-PROYECTO.ventas_demo.ventas_rechazadas`;

-- 2. ¿Por qué se rechazaron? Este desglose es el valor de no descartar
--    en silencio: sabes exactamente qué venía mal en el origen.
SELECT motivo, COUNT(*) AS veces
FROM `MI-PROYECTO.ventas_demo.ventas_rechazadas`
GROUP BY motivo
ORDER BY veces DESC;

-- 3. El control de calidad funcionó: esto debe devolver 0 filas.
SELECT COUNT(*) AS montos_no_positivos
FROM `MI-PROYECTO.ventas_demo.ventas_curadas`
WHERE monto_total <= 0;

-- 4. La normalización funcionó: deben aparecer 4 categorías, no 8 variantes.
SELECT categoria, COUNT(*) AS ventas
FROM `MI-PROYECTO.ventas_demo.ventas_curadas`
GROUP BY categoria
ORDER BY categoria;

-- 5. Análisis de negocio: ventas por sucursal y mes.
SELECT sucursal, anio_mes, ROUND(SUM(monto_total), 2) AS total
FROM `MI-PROYECTO.ventas_demo.ventas_curadas`
GROUP BY sucursal, anio_mes
ORDER BY anio_mes, total DESC;

-- =====================================================================
-- 6. El efecto del particionado.
--    Ejecuta las dos y compara los "bytes procesados" que reporta
--    BigQuery arriba a la derecha, ANTES de darle a ejecutar.
-- =====================================================================

-- Sin filtro de partición: lee toda la tabla
SELECT SUM(monto_total) FROM `MI-PROYECTO.ventas_demo.ventas_curadas`;

-- Con filtro de partición: lee solo un mes
SELECT SUM(monto_total)
FROM `MI-PROYECTO.ventas_demo.ventas_curadas`
WHERE fecha_venta BETWEEN '2025-03-01' AND '2025-03-31';
