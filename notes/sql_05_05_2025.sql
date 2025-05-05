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


SELECT * FROM public.vista_actividad_por_usuarios
LIMIT 7

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
FROM public.vista_actividad_por_usuarios vu
WHERE vu.fecha BETWEEN '2025-05-05' AND '2025-05-05'
GROUP BY vu.zonal, vu.supervisor
ORDER BY vu.zonal, vu.supervisor;

-- REPORTE FIJAS POR ZONAL
SELECT 
  sub.zonal,
  SUM(sub.ventas) AS Ventas,
  SUM(sub.max_couta) AS couta,
  -- puedes agregar más agregados si quieres, como ventas totales
  ROUND(SUM(sub.ventas) / NULLIF(SUM(sub.max_couta), 0), 2) AS cobertura
FROM (
  SELECT 
    vu.zonal,
    vu.supervisor,
    MAX(vu.couta) AS max_couta,
    SUM(vu.ventas) AS ventas
  FROM public.vista_actividad_por_usuarios vu
  WHERE vu.fecha = '2025-05-05'
  GROUP BY vu.zonal, vu.supervisor
) sub
GROUP BY sub.zonal
ORDER BY sub.zonal;
