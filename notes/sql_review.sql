WITH Calendar AS (
  SELECT
    (generate_series(
       '2025-05-01'::date,
       '2025-05-03'::date,
       '1 day'
     ))::date AS fecha
),
UsuariosAjustados AS (
  SELECT
    LEFT(u.dni, 8) AS dni,
    u.nombre,
    CASE WHEN u.zonal LIKE 'LIMA%' THEN 'LIMA' ELSE u.zonal END AS zonal_principal,
    CASE 
      WHEN u.dni IN ('41455870','42047009','46862391','48306579','70258803','72807335','75947227')
        THEN 'GOMEZ PALZA CAROLINA MERCEDES'
      ELSE u.superior
    END AS superior_ajustado,
    CASE 
      WHEN u.superior = 'GOMEZ PALZA CAROLINA MERCEDES' THEN 'MOQUEHUA'
      ELSE u.zonal
    END AS zonal_ajustado,
    u.estado,
    regexp_replace(u.rol, '^.* - ', '') AS rol
  FROM usuarios u
  WHERE u.rol ILIKE '%vendedor%'
    AND u.estado = 'En campo'
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
  COALESCE(cu.valor, 0) AS valor
FROM Calendar c
CROSS JOIN UsuariosAjustados ua
LEFT JOIN actividades a
  ON ua.dni = a.dni_vendedor
  AND a.fecha::date = c.fecha  -- Corregido aquí (eliminado to_date)
LEFT JOIN cuotas cu
  ON cu.supervisor = ua.superior_ajustado
  AND cu.fecha = (
    SELECT MAX(q2.fecha)
    FROM cuotas q2
    WHERE q2.supervisor = ua.superior_ajustado
      AND date_trunc('month', q2.fecha) = date_trunc('month', c.fecha)
  )
GROUP BY
  c.fecha, ua.dni, ua.nombre, ua.zonal_ajustado, ua.superior_ajustado, ua.estado, ua.rol, cu.valor
ORDER BY
  ua.nombre, c.fecha;


-- necesitos una tabla general que combine las tres para realizar una nalsisis especializado como expertors pleae
 """
 actividades
id	fecha	nombre_usuario	dni_vendedor	superior	actividad	detalle	motivo	zonas_asignadas	alertas	latitud	longitud	zona	estado_carga	fecha_carga	hash_id	fecha_actualizacion
1	01/05/2025 23:04	TEJADA TACO REYNALDO VIDAL	48125976	HUANCA CONDORI ANGEL DANIEL	LOGIN	NONE	None	SRG494	Login realizado fuera de horario,Login realizado fuera de zona asignada.	-16.426613	-71.51121	A4R019	nuevo	42:56.3	1a1980f3c2b269366a99f14f1941b3f8	42:56.3
2	01/05/2025 22:59	TEJADA TACO REYNALDO VIDAL	48125976	HUANCA CONDORI ANGEL DANIEL	GUARDAR FORMULARIO	VENTA FIJA	None	SRG494	None	-16.426586	-71.511238	A4R019	nuevo	42:56.3	15a5c7368c9d20935be13d6ae17f9e3b	42:56.3
3	01/05/2025 22:57	TEJADA TACO REYNALDO VIDAL	48125976	HUANCA CONDORI ANGEL DANIEL	LOGIN	NONE	None	SRG494	Login realizado fuera de horario,Login realizado fuera de zona asignada.	-16.426589	-71.511167	A4R019	nuevo	42:56.3	71fb37c7cd96ab5392c171801276ed53	42:56.3
4	01/05/2025 22:56	TEJADA TACO REYNALDO VIDAL	48125976	HUANCA CONDORI ANGEL DANIEL	LOGIN	NONE	None	SRG494	Login realizado fuera de horario,Login realizado fuera de zona asignada.	-16.426614	-71.511226	A4R019	nuevo	42:56.3	2fe8c36cc3e9fd4440b86c58fc66400e	42:56.3
5	01/05/2025 22:06	DIEZ FUENTES MIRIAN DEL ROSARIO	43928702	GARCIA ESPINOZA LEONEL WILMER	LOGIN	NONE	None	SRG494	Login realizado fuera de horario,Login realizado fuera de zona asignada.	-18.015864	-70.254121	HXG036	nuevo	42:56.3	f5275053cfb7051ef36935125b557d8c	42:56.3
6	01/05/2025 20:59	RUELAS ATENCIO MARCO ANTONIO	48056150	ALVAREZ VELARDE GABRIEL ANDRE	GUARDAR FORMULARIO	VENTA FIJA	None	SRG494	None	-16.402121	-71.529845	A3R017	nuevo	42:56.3	faa751d1c42f41a7a9c67caebb55dfe0	42:56.3
7	01/05/2025 20:56	RUELAS ATENCIO MARCO ANTONIO	48056150	ALVAREZ VELARDE GABRIEL ANDRE	LOGIN	NONE	None	SRG494	Login realizado fuera de horario,Login realizado fuera de zona asignada.	-16.401965	-71.529826	A3R017	nuevo	42:56.3	28a1ba165c9652381d1c97a3b24a8a56	42:56.3


usuarios
usuario	empresa	nombre	dni	rol	poblacion	zonal	email	telefono	region	genero	superior	direccion	pais	codigo_postal	zona_horaria	fecha_ingreso	fecha_cese	estado	fecha_proceso	fecha_carga	fecha_actualizacion	hash_id	estado_carga
46916032	NULL	VIDAL AGUERO ALBERTO JORGE	469160320	VENDEDOR - PLANILLA	NULL	LIMA NORTE	avidala2340@gmail.com	51940074653	LIMA	Hombre	CATAMO AZOCAR KATIUSKA DAYANA	Jr. Maria JosÃ© De Arce 480, Urb. Maranga, San Miguel	PerÃº	NULL	(UTC-05:00) Bogota, Lima, Quito	03/05/2025	NULL	En campo	04/05/2025	41:29.2	41:29.2	700e23663c8b36c511d7278aba8f2c1d	nuevo
29398271	NULL	ACHAHUANCO MERMA YOLANDA	293982710	VENDEDOR - PLANILLA	Arequipa	AREQUIPA	yolanda.achahunco@gmail.com	51959670343	REGION SUR	Mujer	ZEBALLOS ESCOBAR KAREN YULEISY	San MartÃ­n 1817 Chapi Chico	PerÃº	NULL	(UTC-05:00) Bogota, Lima, Quito	01/03/2024	NULL	Vacaciones	04/05/2025	41:29.2	41:29.2	f6a0818ed90db25997345641bd4afd92	nuevo
44137762	NULL	ACOSTA HERRERA MILAGROS DE MARIA	441377620	VENDEDOR - COMISIONISTA	Ãncash	CHIMBOTE	milagrosacostaherrera@gmail.com	51944316201	REGION CENTRO	Mujer	GONZALES GUTIERREZ ALEXANDER ISMAEL	UB Nicolas garatea mz 118 LT 21	PerÃº	NULL	(UTC-05:00) Bogota, Lima, Quito	12/03/2025	NULL	En campo	04/05/2025	41:29.2	41:29.2	ce3163916fd0b15f7e14a18260cfbe6b	nuevo
3495147	NULL	ACUNA SANCHEZ JACOB	34951470	VENDEDOR - COMISIONISTA	Arequipa	AREQUIPA	vcordero.auren@gmail.com	51926642555	REGION SUR	Hombre	ZARRAGA HENRIQUEZ SIMON ENRIQUE	NULL	PerÃº	NULL	(UTC-05:00) Bogota, Lima, Quito	NULL	NULL	En campo	04/05/2025	41:29.2	41:29.2	b0419530d85cd2b3f54d1a99c242079e	nuevo
43863338	NULL	ACUNA VEGA RONALD ELIOT	438633380	VENDEDOR - COMISIONISTA	Ãncash	HUARAZ	eronald2020@gmail.com	51958123625	REGION CENTRO	Hombre	GIRALDO LOPEZ RIVALDO ANDRES	Jr. Esteban Castromonte S/N Barr. Pedregal Medio	PerÃº	NULL	(UTC-05:00) Bogota, Lima, Quito	18/03/2025	NULL	En campo	04/05/2025	41:29.2	41:29.2	6875228fe9af2c4a9d7a7b666012b44a	nuevo
43002051	NULL	ADCO GOMEZ DUDLEY DANIEL	430020510	BACK OFFICE	Arequipa	AREQUIPA	adudley.auren@gmail.com	51978589177	REGION SUR	Hombre	AREVALO LAIMITO CARLOS ALBERTO	NULL	PerÃº	NULL	(UTC-05:00) Bogota, Lima, Quito	NULL	NULL	Sin especificar	04/05/2025	41:29.2	41:29.2	075c555dba306a62827cb2d3093d4de6	nuevo
75114101	NULL	AGUILAR LAURA RUTH ESTHER	751141010	VENDEDOR - PLANILLA	Arequipa	AREQUIPA	ruthesteraguilarlaura123@gmail.com	51918077535	REGION SUR	Hombre	ALVAREZ VELARDE GABRIEL ANDRE	3 DE OCTUBRE MZ D LT 1	PerÃº	NULL	(UTC-05:00) Bogota, Lima, Quito	NULL	NULL	En campo	04/05/2025	41:29.2	41:29.2	b1192c4d330c112597c3e0099ba9ebe8	nuevo



detallado
id	codigo_fe	usuario	supervisor	region	zonal	dni_vendedor	formulario	cliente	fecha	tipo_operacion	dni_cliente	nombre_cliente	direccion_instalacion	telefono_contacto	telefono_contacto_otro	producto	nro_pedido	usuario_dito	scoring_dito	es_venta_hoy	fecha_carga	actualizado_el
1	FE-1087621054	CHUMIOQUE LOPEZ KAROL ESTEFANY	ORDINOLA MEJIA JOHAN XAVIER	REGION CENTRO	CHIMBOTE	46569388	Venta Fija	Cliente sin asignar	05/01/2025 07:41	CaptaciÃ³n	45276909	BerlÃ­n Omar RodrÃ­guez DomÃ­nguez	JR. Valencia mz. W lt. 11 A. H. Villa espaÃ±a	918116910	NULL	INTERNET NAKED 400 MBPS	1696141659	kachumioquelo	FINANCIADO PILOTO	Si	05:36.9	NULL
2	FE-1087467235	GARCIA CARPENA FIORELLA VALERY	TORRES ARIAS RAQUEL NATALIA	REGION SUR	TACNA	42904627	Venta Fija	Cliente sin asignar	05/01/2025 08:23	CaptaciÃ³n	48203000	Julie Estefan Mamani Mamani	Cl San Camilo 900	950632147	NULL	INTERNET NAKED 200 MBPS	1695344992	acamachope	FINANCIADO	No	05:36.9	NULL
3	FE-1087023242	VILLAR GONZALES JOSE ALEJANDRO	CHAVEZ VIZALOTE MARIA DEL PILAR	REGION CENTRO	CHIMBOTE	47643450	Venta Fija	Cliente sin asignar	05/01/2025 10:32	CaptaciÃ³n	32124240	Rocio Miriam More Camacho	Calle Gonzales  Prada   Mz  D Lote 5	NULL	908738834	INTERNET NAKED 400 MBPS	1692964299	glugoa	FINANCIADO	No	05:36.9	NULL
4	FE-1087624931	HURTADO ROQUE ANTONIO ALEJANDRO	CHAVEZ VIZALOTE MARIA DEL PILAR	REGION CENTRO	CHIMBOTE	43376281	Venta Fija	Cliente sin asignar	05/01/2025 11:05	CaptaciÃ³n	43233381	Karin Paola Cueva Vasquez	Ah esperanza alta mz t lote 10	923777650	NULL	INTERNET NAKED 400 MBPS	1696169031	jramoneshe	FINANCIADO PILOTO	Si	05:36.9	NULL
5	FE-1087627296	FERNANDEZ GUTIERREZ FABRIZZIO MARCELO	GARCIA ESPINOZA LEONEL WILMER	REGION SUR	TACNA	62347601	Venta Fija	Cliente sin asignar	05/01/2025 13:04	CaptaciÃ³n	47401438	MarÃ­a Elizabeth Ticona Flores	Cl Almirante Miguel Grau 280	924098563	NULL	INTERNET NAKED 200 MBPS	1696196343	dbarrowdia	FINANCIADO	Si	05:37.0	NULL
6	FE-1087629956	VELASQUEZ VEGA MARILYN ELINA	DEYVIS ADRIAN VALDIVIEZO LEON	REGION CENTRO	CHIMBOTE	47440592	Venta Fija	Cliente sin asignar	05/01/2025 13:17	CaptaciÃ³n	33568681	Vidal Almagro Altamirano Pizarro	Ur laderas del norte mz-K lote 4	939368759	918317029	COMPLETA TV	1696197508	Mavelasquezve	FINANCIADO	Si	05:37.0	NULL
7	FE-1087624801	ZEGARRA GONZALES ALBERTO	GUTIERREZ BENITES OSCAR PATRICIO	REGION NORTE	TRUJILLO	18196652	Venta Fija	Cliente sin asignar	05/01/2025 16:14	CaptaciÃ³n	74422909	Sheyla Isela Bentura Quiroz	Mz c lt 17 el milagro etapa 8	932979329	NULL	DUO INTERNET 400 MBPS ESTANDAR DIGITAL TV HD	1696171119	lcaceresme	FINANCIADO	Si	05:37.0	NULL

 
 """