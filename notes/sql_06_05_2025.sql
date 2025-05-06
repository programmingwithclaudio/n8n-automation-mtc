"""
INFO:     Started server process [16140]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
INFO:     Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)
INFO:__main__:Received query: Reporte por supervisor para un día dado
INFO:__main__:DeepSeek API raw response: {"sql_query":"SELECT \n  vu.zonal, \n  vu.supervisor, \n  SUM(vu.hc) AS total_hc, \n  SUM(vu.login) AS total_login, \n  SUM(vu.asisth) AS total_asisth, \n  SUM(vu.hc_c_vta) AS total_hc_c_vta, \n  SUM(vu.ventas) AS total_ventas, \n  MAX(vu.couta) AS couta,\n  ROUND(SUM(vu.ventas)/NULLIF(MAX(vu.couta),0),2) AS cobertura\nFROM public.vista_actividad_por_usuarios vu\nWHERE vu.fecha = '2025-05-05'\nGROUP BY vu.zonal, vu.supervisor\nORDER BY vu.zonal, vu.supervisor;"}
INFO:__main__:Generated SQL: SELECT 
  vu.zonal,
  vu.supervisor,
  SUM(vu.hc) AS total_hc,
  SUM(vu.login) AS total_login,
  SUM(vu.asisth) AS total_asisth,
  SUM(vu.hc_c_vta) AS total_hc_c_vta,
  SUM(vu.ventas) AS total_ventas,
  MAX(vu.couta) AS couta,
  ROUND(SUM(vu.ventas)/NULLIF(MAX(vu.couta),0),2) AS cobertura
FROM public.vista_actividad_por_usuarios vu
WHERE vu.fecha = '2025-05-05'
GROUP BY vu.zonal, vu.supervisor
ORDER BY vu.zonal, vu.supervisor;
ERROR:__main__:Error executing query: relation "public.vista_actividad_por_usuarios" does not exist
LINE 11: FROM public.vista_actividad_por_usuarios vu
              ^

ERROR:__main__:Error executing SQL: Error executing SQL query: relation "public.vista_actividad_por_usuarios" does not exist       
LINE 11: FROM public.vista_actividad_por_usuarios vu
              ^

INFO:     192.168.2.180:52833 - "POST /human_query?Content-Type=application%2Fjson HTTP/1.1" 200 OK
"""

SELECT * FROM actividades  LIMIT 7;
--
SELECT superior, detalle, COUNT(detalle) 
FROM actividades
WHERE superior ILIKE '%TIRADO DELGADO%'
  AND fecha::date = '2025-05-06'
GROUP BY superior, detalle
ORDER BY superior;

"""
SELECT superior, detalle, COUNT(detalle) 
FROM actividades
WHERE superior ILIKE '%TIRADO DELGADO%'
  AND fecha::date = '2025-05-06'
GROUP BY superior, detalle
ORDER BY superior;

"""
SELECT * FROM usuarios LIMIT 7;
SELECT * 
FROM usuarios 
WHERE superior ILIKE '%TIRADO DELGADO%'
  AND fecha_proceso::date = '2025-05-06'
  AND nombre ILIKE '%ROCIO%'
"""resultado
usuario,"empresa","nombre","dni","rol","poblacion","zonal","email","telefono","region","genero","superior","direccion","pais","codigo_postal","zona_horaria","fecha_ingreso","fecha_cese","estado","fecha_proceso","fecha_carga","fecha_actualizacion","hash_id","estado_carga"
00799820,NULL,"ROCIO MAGALLY CELIS CARDENAS","DESCONOCIDO","VENDEDOR - PLANILLA",NULL,"ILO","rociocelis258@gmail.com","+51956800255","REGION SUR","Mujer","TIRADO DELGADO VANESSA YEIMY","Tren Al Sur MZ.O LT.15 ILO","PerÃº",NULL,"(UTC-05:00) Bogota, Lima, Quito","2025-05-02",NULL,"En campo","2025-05-06","2025-05-06 10:24:29.409759","2025-05-06 10:24:29.409759","e457c1cfa38d0a80cced65198d5dbafe","nuevo"

"""


SELECT * FROM actividades WHERE superior ILIKE '%TIRADO DELGADO%'
  AND fecha::date = '2025-05-06' AND detalle = 'VENTA FIJA';

"""resultado
id,"fecha","nombre_usuario","dni_vendedor","superior","actividad","detalle","motivo","zonas_asignadas","alertas","latitud","longitud","zona","estado_carga","fecha_carga","hash_id","fecha_actualizacion"
29254,"2025-05-06 12:44:55","ROCIO MAGALLY CELIS CARDENAS","799820","TIRADO DELGADO VANESSA YEIMY","GUARDAR FORMULARIO","VENTA FIJA","None","None","None","-17.62943500","-71.34009600","None","nuevo","2025-05-06 14:00:32.565073","483f3ad64f2582ced48928822961e12e","2025-05-06 14:00:32.565073"

"""


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
WHERE vu.fecha BETWEEN '2025-05-05' AND '2025-05-05'
GROUP BY vu.zonal, vu.supervisor
ORDER BY vu.zonal, vu.supervisor;



SELECT COUNT(*) FROM usuarios; --717
SELECT * FROM usuarios LIMIT 7;

SELECT * FROM usuarios WHERE fecha_proceso='2025-05-06' LIMIT 7;

SELECT us.estado, COUNT(us.estado) FROM usuarios us GROUP BY us.estado;
SELECT us.nombre, COUNT(us.nombre) FROM usuarios us GROUP BY us.nombre;

SELECT * FROM vista_actividad_usuarios LIMIT 5;


-- actualizado al 2025-05-06
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


  -------------------------------------------#
SELECT superior, detalle, COUNT(detalle) 
FROM actividades
WHERE superior ILIKE '%TIRADO DELGADO%'
  AND fecha::date = '2025-05-06'
GROUP BY superior, detalle
ORDER BY superior;

SELECT * FROM usuarios LIMIT 7;
SELECT * 
FROM usuarios 
WHERE superior ILIKE '%TIRADO DELGADO%'
  AND fecha_proceso::date = '2025-05-06'
  AND nombre ILIKE '%ROCIO%'
"""resultado
usuario,"empresa","nombre","dni","rol","poblacion","zonal","email","telefono","region","genero","superior","direccion","pais","codigo_postal","zona_horaria","fecha_ingreso","fecha_cese","estado","fecha_proceso","fecha_carga","fecha_actualizacion","hash_id","estado_carga"
00799820,NULL,"ROCIO MAGALLY CELIS CARDENAS","DESCONOCIDO","VENDEDOR - PLANILLA",NULL,"ILO","rociocelis258@gmail.com","+51956800255","REGION SUR","Mujer","TIRADO DELGADO VANESSA YEIMY","Tren Al Sur MZ.O LT.15 ILO","PerÃº",NULL,"(UTC-05:00) Bogota, Lima, Quito","2025-05-02",NULL,"En campo","2025-05-06","2025-05-06 10:24:29.409759","2025-05-06 10:24:29.409759","e457c1cfa38d0a80cced65198d5dbafe","nuevo"

"""


SELECT * FROM actividades WHERE superior ILIKE '%TIRADO DELGADO%'
  AND fecha::date = '2025-05-06' AND detalle = 'VENTA FIJA';

"""resultado
id,"fecha","nombre_usuario","dni_vendedor","superior","actividad","detalle","motivo","zonas_asignadas","alertas","latitud","longitud","zona","estado_carga","fecha_carga","hash_id","fecha_actualizacion"
29254,"2025-05-06 12:44:55","ROCIO MAGALLY CELIS CARDENAS","799820","TIRADO DELGADO VANESSA YEIMY","GUARDAR FORMULARIO","VENTA FIJA","None","None","None","-17.62943500","-71.34009600","None","nuevo","2025-05-06 14:00:32.565073","483f3ad64f2582ced48928822961e12e","2025-05-06 14:00:32.565073"

"""
---------------------------------------------------#
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
