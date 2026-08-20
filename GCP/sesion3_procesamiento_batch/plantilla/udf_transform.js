/**
 * Sesión 3 - UDF para la plantilla "Text Files on Cloud Storage to BigQuery".
 *
 * La plantilla NO infiere el esquema del CSV: le entrega cada línea a esta
 * función y espera de vuelta una cadena JSON con las claves que coincidan
 * con esquema_bq.json.
 *
 * Devolver null descarta la línea. Así se elimina la fila de cabecera, que
 * si no entraría a BigQuery como un registro más.
 */
function transform(line) {
  var valores = line.split(',');

  // Descarta la cabecera
  if (valores[0] === 'venta_id') {
    return null;
  }

  // Descarta líneas mal formadas
  if (valores.length !== 6) {
    return null;
  }

  var obj = {};
  obj.venta_id    = valores[0];
  obj.fecha_venta = valores[1];
  obj.sucursal    = valores[2];
  obj.categoria   = valores[3];
  obj.cantidad    = parseInt(valores[4], 10);
  obj.monto_total = parseFloat(valores[5]);

  return JSON.stringify(obj);
}
