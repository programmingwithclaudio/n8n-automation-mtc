CREATE OR REPLACE VIEW vista_actividad_usuarios AS
WITH
  Calendar AS (
    SELECT
      generate_series(
        '2025-01-01'::date,
        '2025-12-31'::date,
        '1 day'
      )::date AS fecha
  ),

  UsuariosAjustados AS (
    SELECT
      LEFT(u.usuario, 8) AS dni,  -- Usamos la columna 'usuario' que contiene el DNI real
      u.nombre,
      CASE
        WHEN u.zonal LIKE 'LIMA%' THEN 'LIMA'
        ELSE u.zonal
      END AS zonal_principal,
      CASE
        WHEN u.dni IN ('41455870','42047009','46862391','48306579','70258803','72807335','75947227')
          THEN 'GOMEZ PALZA CAROLINA MERCEDES'
        ELSE u.superior
      END AS superior_ajustado,
      CASE
        WHEN u.superior = 'GOMEZ PALZA CAROLINA MERCEDES'
          THEN 'ILO'
        ELSE u.zonal
      END AS zonal_ajustado,
      u.estado,
      regexp_replace(u.rol, '^.* - ', '') AS rol
    FROM usuarios u
    WHERE
      u.rol ILIKE '%vendedor%'
      AND u.estado = 'En campo'
      AND u.fecha_proceso = CURRENT_DATE
      AND u.nombre <> 'BECERRA ACHATA SERGIO RENATO'
  )

SELECT
  c.fecha,
  ua.dni,
  ua.nombre,
  ua.zonal_ajustado AS zonal,
  ua.superior_ajustado AS supervisor,
  ua.estado,
  ua.rol,
  1 AS hc,
  MAX(CASE WHEN a.actividad = 'LOGIN' THEN 1 ELSE 0 END) AS login,
  MAX(CASE WHEN a.actividad = 'PRESENCIA HUELLERO' THEN 1 ELSE 0 END) AS asisth,
  MAX(CASE WHEN a.detalle = 'VENTA FIJA' THEN 1 ELSE 0 END) AS hc_c_vta,
  COUNT(CASE WHEN a.detalle = 'VENTA FIJA' THEN 1 END) AS ventas,
  COALESCE(cu.valor, 0) AS couta
FROM Calendar c
CROSS JOIN UsuariosAjustados ua
LEFT JOIN actividades a
  ON ltrim(ua.dni, '0') = a.dni_vendedor  -- Eliminamos ceros a la izquierda para coincidir
  AND a.fecha::date = c.fecha
LEFT JOIN cuotas cu
  ON cu.supervisor = ua.superior_ajustado
  AND cu.fecha = (
    SELECT MAX(q2.fecha)
    FROM cuotas q2
    WHERE
      q2.supervisor = ua.superior_ajustado
      AND date_trunc('month', q2.fecha) = date_trunc('month', c.fecha)
  )
GROUP BY
  c.fecha,
  ua.dni,
  ua.nombre,
  ua.zonal_ajustado,
  ua.superior_ajustado,
  ua.estado,
  ua.rol,
  cu.valor
ORDER BY
  ua.nombre,
  c.fecha;


 -- VENTAS REPORTE FIJAS POR SUPERVISOR
SELECT 
  vu.zonal, 
  vu.supervisor, 
  SUM(vu.hc) AS total_hc, 
  SUM(vu.login) AS total_login, 
  SUM(vu.asisth) AS total_asisth, 
  SUM(vu.hc_c_vta) AS total_hc_c_vta, 
  SUM(vu.ventas) AS total_ventas, 
  MAX(vu.couta) AS couta,
  ROUND(SUM(vu.ventas) / NULLIF(MAX(vu.couta), 0), 2) AS cobertura
FROM public.vista_actividad_usuarios vu
WHERE 
  vu.fecha = '2025-05-06'
  AND vu.supervisor IS NOT NULL
GROUP BY vu.zonal, vu.supervisor
ORDER BY vu.zonal, vu.supervisor;

 -- VENTAS REPORTE FIJAS POR ZONAL
SELECT 
  sub.zonal,
  SUM(sub.ventas) AS ventas,
  SUM(sub.max_couta) AS couta,
  ROUND(SUM(sub.ventas)/NULLIF(SUM(sub.max_couta),0),2) AS cobertura
FROM (
  SELECT 
    vu.zonal,
    vu.supervisor,
    MAX(vu.couta) AS max_couta,
    SUM(vu.ventas) AS ventas
  FROM public.vista_actividad_usuarios vu
  WHERE vu.fecha = '2025-05-06'
  GROUP BY vu.zonal, vu.supervisor
) sub
GROUP BY sub.zonal
ORDER BY sub.zonal;

"""agente especialista en sqlpostges en consultas avanzadas y subconsultas
curl -X GET "http://localhost:8000/health"

REM Test a simple human query
curl -X POST "http://localhost:8000/human_query" ^
     -H "Content-Type: application/json" ^
     -d "{\"human_query\":\"Reporte por supervisor para un día dado\", \"date\":\"2025-05-06\"}"

REM Test a simple human query sql
curl -X POST "http://localhost:8000/execute_sql" ^
     -H "Content-Type: application/json" ^
     -d "{\"sql_query\":\"SELECT * FROM public.vista_actividad_usuarios LIMIT 10\"}"

	 
REM Test a zonal report
curl -X POST "http://localhost:8000/human_query" ^
     -H "Content-Type: application/json" ^
     -d "{\"human_query\":\"Reporte por zonal sumando para cada supervisor su cuota máxima\", \"date\":\"2025-05-06\"}"

REM Test a more specific query
curl -X POST "http://localhost:8000/human_query" ^
     -H "Content-Type: application/json" ^
     -d "{\"human_query\":\"Muestra los supervisores que cumplieron su cuota al 100% para el día 2025-05-06\"}"

	
"""

"""
# Server configuration
SERVER_URL=http://localhost:8000

# DeepSeek API configuration
DEEPSEEK_API_KEY=
DEEPSEEK_API_URL=https://api.deepseek.com/v1/chat/completions

# Database configuration
DB_HOST=localhost
#DB_NAME=postgres
DB_NAME=testdbauren
DB_USER=postgres
DB_PASSWORD=localpassword
DB_PORT=5432

"""