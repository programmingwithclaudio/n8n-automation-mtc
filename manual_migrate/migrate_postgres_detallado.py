import pandas as pd
import os
import logging
import yaml
from datetime import datetime
from sqlalchemy import create_engine, text, inspect
import numpy as np
import sys

# Función para cargar la configuración desde YAML
def cargar_configuracion(archivo_config=r"manual_migrate\config_detallado.yml"):
    """Cargar configuración desde archivo YAML"""
    try:
        if not os.path.exists(archivo_config):
            raise FileNotFoundError(f"No se encontró el archivo de configuración: {archivo_config}")
        
        with open(archivo_config, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)
        
        return config
    except Exception as e:
        print(f"Error al cargar la configuración: {e}")
        sys.exit(1)

# Cargar configuración
config = cargar_configuracion()

# Configuración de logging
log_dir = os.path.dirname(config.get('log_file', 'carga_detallado.log'))
if log_dir and not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(config.get('log_file', 'carga_detallado.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

# Configuración de la conexión a la base de datos desde el archivo YAML
DB_CONFIG = config.get('db', {
    "servidor": "localhost",
    "db": "testdbauren",
    "user": "postgres",
    "password": "localpassword"
})

# Nombre de la tabla destino desde configuración
TABLA_DESTINO = config.get('tabla_destino', 'detallado')

# Configuración de ruta del archivo desde configuración
ARCHIVO_EXCEL = config.get('archivo_excel', r"C:\Users\oak\Downloads\Detallado01.xlsx")

# Claves únicas para identificar registros
CLAVE_PRIMARIA = config.get('clave_unica', 'codigo_fe')
CLAVE_SECUNDARIA = config.get('clave_secundaria', 'dni_cliente')

# Mapeo de columnas
MAPEO_COLUMNAS = config.get('mapeo_columnas', {})

def crear_conexion():
    """Crear conexión a PostgreSQL"""
    try:
        connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['servidor']}/{DB_CONFIG['db']}"
        engine = create_engine(connection_string)
        # Verificar la conexión
        with engine.connect() as conn:
            pass
        logger.info(f"Conexión exitosa a la base de datos {DB_CONFIG['db']}")
        return engine
    except Exception as e:
        logger.error(f"Error al conectar a la base de datos: {e}")
        raise

def verificar_tabla_existe(engine, tabla):
    """Verificar si la tabla existe en la base de datos"""
    inspector = inspect(engine)
    return tabla in inspector.get_table_names()

def crear_tabla_si_no_existe(engine):
    """Crear la tabla si no existe"""
    if not verificar_tabla_existe(engine, TABLA_DESTINO):
        try:
            # Definición de la tabla con índices para ambas claves
            query = f"""
            CREATE TABLE {TABLA_DESTINO} (
                id SERIAL PRIMARY KEY,
                codigo_fe VARCHAR(20),
                usuario VARCHAR(100),
                supervisor VARCHAR(100),
                region VARCHAR(50),
                zonal VARCHAR(50),
                dni_vendedor VARCHAR(20),
                formulario VARCHAR(50),
                cliente VARCHAR(100),
                fecha TIMESTAMP,
                tipo_operacion VARCHAR(50),
                dni_cliente VARCHAR(20),
                nombre_cliente VARCHAR(200),
                direccion_instalacion TEXT,
                telefono_contacto VARCHAR(50),
                telefono_contacto_otro VARCHAR(50),
                producto VARCHAR(100),
                nro_pedido VARCHAR(20),
                usuario_dito VARCHAR(50),
                scoring_dito VARCHAR(50),
                es_venta_hoy VARCHAR(5),
                fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                actualizado_el TIMESTAMP,
                CONSTRAINT uk_codigo_dni UNIQUE (codigo_fe, dni_cliente)
            );
            
            CREATE INDEX idx_codigo_fe ON {TABLA_DESTINO} (codigo_fe);
            CREATE INDEX idx_dni_cliente ON {TABLA_DESTINO} (dni_cliente);
            """
            with engine.connect() as conn:
                conn.execute(text(query))
                conn.commit()
            logger.info(f"Tabla {TABLA_DESTINO} creada exitosamente")
        except Exception as e:
            logger.error(f"Error al crear la tabla {TABLA_DESTINO}: {e}")
            raise
    else:
        logger.info(f"La tabla {TABLA_DESTINO} ya existe")
        # Verificar si los índices y constraint existen, y agregarlos si no
        try:
            with engine.connect() as conn:
                # Verificar si existe el constraint de unicidad
                check_constraint = f"""
                SELECT COUNT(*) FROM pg_constraint
                WHERE conname = 'uk_codigo_dni'
                AND conrelid = '{TABLA_DESTINO}'::regclass;
                """
                result = conn.execute(text(check_constraint))
                constraint_exists = result.scalar() > 0
                
                # Verificar si existen los índices
                check_index_codigo = f"""
                SELECT COUNT(*) FROM pg_indexes
                WHERE indexname = 'idx_codigo_fe'
                AND tablename = '{TABLA_DESTINO}';
                """
                result = conn.execute(text(check_index_codigo))
                index_codigo_exists = result.scalar() > 0
                
                check_index_dni = f"""
                SELECT COUNT(*) FROM pg_indexes
                WHERE indexname = 'idx_dni_cliente'
                AND tablename = '{TABLA_DESTINO}';
                """
                result = conn.execute(text(check_index_dni))
                index_dni_exists = result.scalar() > 0
                
                # Agregar constraint si no existe
                if not constraint_exists:
                    try:
                        add_constraint = f"""
                        ALTER TABLE {TABLA_DESTINO}
                        ADD CONSTRAINT uk_codigo_dni UNIQUE (codigo_fe, dni_cliente);
                        """
                        conn.execute(text(add_constraint))
                        conn.commit()
                        logger.info("Constraint de unicidad agregado")
                    except Exception as e:
                        logger.warning(f"No se pudo agregar el constraint de unicidad: {e}")
                
                # Agregar índices si no existen
                if not index_codigo_exists:
                    add_index_codigo = f"""
                    CREATE INDEX idx_codigo_fe ON {TABLA_DESTINO} (codigo_fe);
                    """
                    conn.execute(text(add_index_codigo))
                    conn.commit()
                    logger.info("Índice para codigo_fe agregado")
                
                if not index_dni_exists:
                    add_index_dni = f"""
                    CREATE INDEX idx_dni_cliente ON {TABLA_DESTINO} (dni_cliente);
                    """
                    conn.execute(text(add_index_dni))
                    conn.commit()
                    logger.info("Índice para dni_cliente agregado")
        except Exception as e:
            logger.warning(f"Error al verificar o agregar índices: {e}")

def limpiar_datos(df):
    """Limpia y normaliza los datos antes de procesarlos"""
    # Hacer una copia para no modificar el original
    df_limpio = df.copy()
    
    # Normalizar strings: quitar espacios en blanco, convertir a minúsculas donde sea apropiado
    for col in df_limpio.columns:
        if df_limpio[col].dtype == 'object':
            # Reemplazar valores nulos con None
            df_limpio[col] = df_limpio[col].replace({np.nan: None})
            
            # Continuar solo con valores no nulos
            mask = df_limpio[col].notna()
            if mask.any():
                # Eliminar espacios en blanco al inicio y final
                df_limpio.loc[mask, col] = df_limpio.loc[mask, col].astype(str).str.strip()
                
                # Reemplazar valores puntos solitarios o vacíos con None
                df_limpio.loc[mask, col] = df_limpio.loc[mask, col].apply(
                    lambda x: None if x in ['.', '..', '', ' '] else x
                )
    
    # Limpieza específica para usuario_dito (eliminar textos como "Usuario:", espacios extra)
    if 'usuario_dito' in df_limpio.columns:
        mask = df_limpio['usuario_dito'].notna()
        if mask.any():
            df_limpio.loc[mask, 'usuario_dito'] = df_limpio.loc[mask, 'usuario_dito'].apply(
                lambda x: x.replace('Usuario:', '').replace('"', '').strip() if isinstance(x, str) else x
            )
    
    # Normalizar nombres de clientes (capitalizar correctamente)
    if 'nombre_cliente' in df_limpio.columns:
        mask = df_limpio['nombre_cliente'].notna()
        if mask.any():
            df_limpio.loc[mask, 'nombre_cliente'] = df_limpio.loc[mask, 'nombre_cliente'].apply(
                lambda x: ' '.join(word.capitalize() for word in x.lower().split()) if isinstance(x, str) else x
            )
    
    # Normalizar teléfonos (eliminar caracteres no numéricos)
    for col in ['telefono_contacto', 'telefono_contacto_otro']:
        if col in df_limpio.columns:
            mask = df_limpio[col].notna()
            if mask.any():
                df_limpio.loc[mask, col] = df_limpio.loc[mask, col].apply(
                    lambda x: ''.join(c for c in str(x) if c.isdigit()) if isinstance(x, str) else x
                )
    
    return df_limpio

def validar_datos(df):
    """Validar y limpiar los datos antes de cargarlos"""
    # Limpiar datos primero
    df_validado = limpiar_datos(df)
    
    # Validar que la columna de clave primaria no tenga valores nulos
    if df_validado[CLAVE_PRIMARIA].isna().any():
        nulos = df_validado[df_validado[CLAVE_PRIMARIA].isna()]
        logger.warning(f"Se encontraron {len(nulos)} registros con {CLAVE_PRIMARIA} nulo. Estos registros serán omitidos.")
        df_validado = df_validado.dropna(subset=[CLAVE_PRIMARIA])
    
    # Validar que la columna de clave secundaria no tenga valores nulos
    if df_validado[CLAVE_SECUNDARIA].isna().any():
        nulos = df_validado[df_validado[CLAVE_SECUNDARIA].isna()]
        logger.warning(f"Se encontraron {len(nulos)} registros con {CLAVE_SECUNDARIA} nulo. Estos registros serán omitidos.")
        df_validado = df_validado.dropna(subset=[CLAVE_SECUNDARIA])
    
    # Validar duplicados en la combinación de claves dentro del archivo
    df_validado['clave_compuesta'] = df_validado[CLAVE_PRIMARIA].astype(str) + '_' + df_validado[CLAVE_SECUNDARIA].astype(str)
    duplicados = df_validado[df_validado.duplicated('clave_compuesta', keep=False)]
    
    if not duplicados.empty:
        logger.warning(f"Se encontraron {len(duplicados)} registros duplicados por combinación de {CLAVE_PRIMARIA} y {CLAVE_SECUNDARIA}.")
        
        # Ordenar por fecha más reciente para cada combinación de claves
        df_validado = df_validado.sort_values('fecha', ascending=False)
        # Mantener solo la versión más reciente de cada combinación de claves
        df_validado = df_validado.drop_duplicates(subset=['clave_compuesta'], keep='first')
        logger.info(f"Se mantendrá solo la versión más reciente de cada registro duplicado.")
    
    # Eliminar la columna auxiliar
    df_validado = df_validado.drop(columns=['clave_compuesta'])
    
    # Validar formato de fechas
    df_validado['fecha'] = pd.to_datetime(df_validado['fecha'], errors='coerce')
    invalidos_fecha = df_validado[df_validado['fecha'].isna()]
    if not invalidos_fecha.empty:
        logger.warning(f"Se encontraron {len(invalidos_fecha)} registros con fechas inválidas.")
        # Mantener solo los registros con fechas válidas
        df_validado = df_validado.dropna(subset=['fecha'])
    
    # Reemplazar valores NaN con None para SQLAlchemy
    df_validado = df_validado.replace({np.nan: None})
    
    # Validar longitud de campos críticos
    campos_max_longitud = {
        'codigo_fe': 20,
        'dni_cliente': 20,
        'telefono_contacto': 50,
        'telefono_contacto_otro': 50,
        'nombre_cliente': 200,
        'usuario_dito': 50
    }
    
    for campo, max_longitud in campos_max_longitud.items():
        if campo in df_validado.columns:
            # Solo procesar los valores no nulos
            mascara = df_validado[campo].notna()
            if mascara.any():
                # Truncar valores que excedan la longitud máxima
                df_validado.loc[mascara, campo] = df_validado.loc[mascara, campo].astype(str).str.slice(0, max_longitud)
    
    return df_validado

def cargar_datos_excel():
    """Cargar datos desde el archivo Excel"""
    try:
        if not os.path.exists(ARCHIVO_EXCEL):
            raise FileNotFoundError(f"No se encontró el archivo Excel: {ARCHIVO_EXCEL}")
            
        logger.info(f"Cargando datos desde {ARCHIVO_EXCEL}")
        df = pd.read_excel(ARCHIVO_EXCEL)
        
        # Aplicar mapeo de columnas si existe
        if MAPEO_COLUMNAS:
            # Renombrar solo las columnas que existen en el DataFrame
            columnas_a_renombrar = {col: nuevo_nombre for col, nuevo_nombre in MAPEO_COLUMNAS.items() 
                                  if col in df.columns}
            df = df.rename(columns=columnas_a_renombrar)
        else:
            # Si no hay mapeo, asumir que las columnas ya tienen los nombres esperados
            logger.info("No se encontró mapeo de columnas, se usarán los nombres actuales")
        
        # Validar y limpiar datos
        df = validar_datos(df)
        
        logger.info(f"Se cargaron {len(df)} registros válidos del archivo Excel")
        return df
    except Exception as e:
        logger.error(f"Error al cargar datos del Excel: {e}")
        raise

def verificar_columnas_requeridas(df):
    """Verificar que el DataFrame tenga todas las columnas necesarias"""
    columnas_requeridas = [
        'codigo_fe', 'usuario', 'supervisor', 'region', 'zonal', 'dni_vendedor',
        'formulario', 'cliente', 'fecha', 'tipo_operacion', 'dni_cliente',
        'nombre_cliente', 'direccion_instalacion', 'telefono_contacto',
        'telefono_contacto_otro', 'producto', 'nro_pedido', 'usuario_dito',
        'scoring_dito', 'es_venta_hoy'
    ]
    
    columnas_faltantes = [col for col in columnas_requeridas if col not in df.columns]
    
    if columnas_faltantes:
        logger.error(f"Faltan las siguientes columnas en el archivo: {', '.join(columnas_faltantes)}")
        raise ValueError(f"Faltan columnas requeridas: {', '.join(columnas_faltantes)}")
    
    return True

def obtener_registros_existentes(engine, df):
    """Obtener registros que ya existen en la base de datos basados en la combinación de claves"""
    if df.empty:
        return {}
    
    try:
        # Crear tuplas de (código_fe, dni_cliente) para buscar
        pares_claves = list(zip(df[CLAVE_PRIMARIA].tolist(), df[CLAVE_SECUNDARIA].tolist()))
        registros_existentes = {}
        
        # Procesar en lotes para evitar consultas demasiado grandes
        tamaño_lote = 1000
        for i in range(0, len(pares_claves), tamaño_lote):
            lote_actual = pares_claves[i:i+tamaño_lote]
            
            # Construir condiciones para cada par de claves
            condiciones = []
            for codigo_fe, dni_cliente in lote_actual:
                if codigo_fe is not None and dni_cliente is not None:
                    # Escapar comillas simples duplicándolas
                    codigo_escapado = str(codigo_fe).replace("'", "''")
                    dni_escapado = str(dni_cliente).replace("'", "''")
                    condiciones.append(f"(codigo_fe = '{codigo_escapado}' AND dni_cliente = '{dni_escapado}')")
            
            if not condiciones:
                continue
                
            where_clause = " OR ".join(condiciones)
            query = f"""
            SELECT codigo_fe, dni_cliente, fecha 
            FROM {TABLA_DESTINO} 
            WHERE {where_clause}
            """
            
            with engine.connect() as conn:
                result = conn.execute(text(query))
                for row in result:
                    # Usar una tupla (codigo_fe, dni_cliente) como clave del diccionario
                    registros_existentes[(row[0], row[1])] = row[2]
        
        logger.info(f"Se encontraron {len(registros_existentes)} registros existentes")
        return registros_existentes
    except Exception as e:
        logger.error(f"Error al consultar registros existentes: {e}")
        raise

def procesar_carga_incremental(engine, df):
    """Realizar la carga incremental de datos"""
    try:
        # Verificar columnas requeridas
        verificar_columnas_requeridas(df)
        
        if df.empty:
            logger.warning("No hay datos válidos para procesar")
            return {
                'nuevos': 0,
                'actualizados': 0,
                'total_procesados': 0,
                'errores': 0
            }
        
        # Obtener registros existentes usando ambas claves
        registros_existentes = obtener_registros_existentes(engine, df)
        
        # Separar registros para inserción y actualización
        nuevos = []
        actualizar = []
        
        for _, row in df.iterrows():
            codigo_fe = row[CLAVE_PRIMARIA]
            dni_cliente = row[CLAVE_SECUNDARIA]
            
            # Verificar si la combinación de claves ya existe
            if (codigo_fe, dni_cliente) in registros_existentes:
                # Comparar fechas para decidir si actualizar
                fecha_existente = registros_existentes[(codigo_fe, dni_cliente)]
                if row['fecha'] >= fecha_existente:
                    actualizar.append(row)
            else:
                nuevos.append(row)
        
        errores = 0
        nuevos_insertados = 0
        
        # Insertar registros nuevos
        for row in nuevos:
            try:
                # Convertir a diccionario y manejar NaN
                datos = row.replace({np.nan: None}).to_dict()
                
                # Crear consulta paramétrica para la inserción
                columnas = ', '.join(datos.keys())
                parametros = ', '.join([f":{col}" for col in datos.keys()])
                
                query = f"""
                INSERT INTO {TABLA_DESTINO} ({columnas})
                VALUES ({parametros})
                """
                
                with engine.connect() as conn:
                    conn.execute(text(query), datos)
                    conn.commit()
                    nuevos_insertados += 1
            except Exception as e:
                logger.error(f"Error al insertar registro {row[CLAVE_PRIMARIA]}-{row[CLAVE_SECUNDARIA]}: {e}")
                errores += 1
        
        logger.info(f"Se insertaron {nuevos_insertados} registros nuevos")
        
        # Actualizar registros existentes
        registros_actualizados = 0
        timestamp_actual = datetime.now()
        
        for row in actualizar:
            try:
                codigo_fe = row[CLAVE_PRIMARIA]
                dni_cliente = row[CLAVE_SECUNDARIA]
                
                if codigo_fe is None or dni_cliente is None:
                    continue
                    
                # Convertir la fila a diccionario 
                datos = row.replace({np.nan: None}).to_dict()
                
                # Construir la consulta de actualización
                set_clauses = []
                params = {}
                
                for column, value in datos.items():
                    if column not in [CLAVE_PRIMARIA, CLAVE_SECUNDARIA]:  # No actualizar las claves
                        set_clauses.append(f"{column} = :{column}")
                        params[column] = value
                
                # Agregar timestamp de actualización
                set_clauses.append("actualizado_el = :actualizado_el")
                params['actualizado_el'] = timestamp_actual
                params[CLAVE_PRIMARIA] = codigo_fe
                params[CLAVE_SECUNDARIA] = dni_cliente
                
                query = f"""
                UPDATE {TABLA_DESTINO}
                SET {', '.join(set_clauses)}
                WHERE {CLAVE_PRIMARIA} = :{CLAVE_PRIMARIA} AND {CLAVE_SECUNDARIA} = :{CLAVE_SECUNDARIA}
                """
                
                with engine.connect() as conn:
                    conn.execute(text(query), params)
                    conn.commit()
                    registros_actualizados += 1
            except Exception as e:
                logger.error(f"Error al actualizar registro {row.get(CLAVE_PRIMARIA, 'desconocido')}-{row.get(CLAVE_SECUNDARIA, 'desconocido')}: {e}")
                errores += 1
        
        logger.info(f"Se actualizaron {registros_actualizados} registros existentes")
        
        return {
            'nuevos': nuevos_insertados,
            'actualizados': registros_actualizados,
            'total_procesados': len(df),
            'errores': errores
        }
    except Exception as e:
        logger.error(f"Error general en la carga incremental: {e}")
        raise

def main():
    """Función principal"""
    try:
        logger.info("Iniciando proceso de carga incremental")
        
        # Crear conexión a la base de datos
        engine = crear_conexion()
        
        # Crear tabla si no existe
        crear_tabla_si_no_existe(engine)
        
        # Cargar datos del Excel
        df = cargar_datos_excel()
        
        if df.empty:
            logger.warning("No hay datos para procesar. El archivo Excel está vacío o no contiene datos válidos.")
            return {
                'nuevos': 0,
                'actualizados': 0,
                'total_procesados': 0,
                'errores': 0
            }
        
        # Procesar carga incremental
        resultados = procesar_carga_incremental(engine, df)
        
        logger.info(f"Proceso completado: {resultados['nuevos']} registros nuevos, "
                   f"{resultados['actualizados']} registros actualizados, "
                   f"{resultados['total_procesados']} registros procesados en total, "
                   f"{resultados['errores']} errores")
        
        return resultados
    except Exception as e:
        logger.error(f"Error en el proceso de carga incremental: {e}")
        raise

if __name__ == "__main__":
    main()
#tiene errores de duplicidad en tanto las key pueden ser "codigo_fe" y "dni_cliente"  la automatizacion de carga incrementacl y actualizacion
"""Detallado.xlsx dia 01-05 y 02-05 
| Usuario                             | Supervisor                        | Region        | Zonal    | DNI Vendedor | Formulario   | Cliente            | Fecha                | Código FE       | TIPO OPERACIÓN | DNI CLIENTE | NOMBRE DEL CLIENTE                     | DIRECCIÓN INSTALACIÓN                     | TELÉFONO CONTACTO | TELEFONO DE CONTACTO (Otro) | PRODUCTO                               | Nro. Pedido  | Usuario Dito         | Scoring (Dito) | ¿Es venta de hoy? |
|-------------------------------------|-----------------------------------|---------------|---------|--------------|--------------|---------------------|----------------------|------------------|----------------|-------------|----------------------------------------|-------------------------------------------|-------------------|-----------------------------|----------------------------------------|---------------|-----------------------|-----------------|-------------------|
| RENTERIA ALBITRES EDWAR ANTHONY    | CHAVEZ VIZALOTE MARIA DEL PILAR   | REGION CENTRO | CHIMBOTE| 32987649     | Venta Fija   | Cliente sin asignar  | 02/05/2025 8:36:42   | FE-1087579554    | Captación      | 32944095   | HIGO EDUARDO QUEZADA TRUJILLO       | JR. Ramón Castilla MZA O lote 10, la esperanza baja | 950434336         |                             | TRIO PLANO LOCAL 400 MBPS ESTANDAR DIGITAL HD | 1695855819    | erenteriaalb        | FINANCIADO     | Si                |
| RENTERIA ALBITRES EDWAR ANTHONY    | CHAVEZ VIZALOTE MARIA DEL PILAR   | REGION CENTRO | CHIMBOTE| 32987649     | Venta Fija   | Cliente sin asignar  | 02/05/2025 8:40:27   | FE-1087579554    | Captación      | 32944950   | Hugo Eduardo Quezada Trujillo        | JR. Ramón Castilla MZA O lote 10, la esperanza baja | 950434336         |                             | TRIO PLANO LOCAL 400 MBPS ESTANDAR DIGITAL HD | 1695855819    | erenteriaalb        | FINANCIADO     | Si                |
| ARI CUTIPA JANETH VANESSA          | ZEBALLOS ESCOBAR KAREN YULEISY   | REGION SUR    | AREQUIPA| 43927183     | Venta Fija   | Cliente sin asignar  | 02/05/2025 10:47:38  | FE-1087599232    | Captación      | 29652352   | Celestino Teofilo Tapia Roque       | Cl. Alfonso Ugarte 212 Urb. Jorge Chavez | 970299367         |                             | INTERNET NAKED 400 MBPS                 | 1695914548    | Mhurtadosala        | FINANCIADO     | No                |
| ARI CUTIPA JANETH VANESSA          | ZEBALLOS ESCOBAR KAREN YULEISY   | REGION SUR    | AREQUIPA| 43927183     | Venta Fija   | Cliente sin asignar  | 02/05/2025 12:05:55  | FE-1087689653    | Captación      | 45880470   | Ella Patricia Cervantes Rodriguez     | Cl. Ricardo Palma 102a As. Huaranguillo | 980470183         |                             | DUO PLANO LOCAL 400 MBPS                 | 1696913547    | vvaldezgarc         | FINANCIADO PILOTO | Si                |
| RETAMOZO ALBURQUEQUE MIRYAM JULISSA | CATAMO AZOCAR KATIUSKA DAYANA    | LIMA          | LIMA NORTE | 44808084   | Venta Fija   | Cliente sin asignar  | 02/05/2025 13:00:18  | FE-1087684614    | Captación      | 08962995   | .                                      | .                                         |                   | ..                          | DUO INTERNET 400 MBPS ESTANDAR DIGITAL TV HD | 1696899190    | "Usuario:	dramirezoca	 "| FINANCIADO     | Si                |
| RETAMOZO ALBURQUEQUE MIRYAM JULISSA | CATAMO AZOCAR KATIUSKA DAYANA    | LIMA          | LIMA NORTE | 44808084   | Venta Fija   | Cliente sin asignar  | 02/05/2025 15:02:32  | FE-1087684614    | Captación      | 08962998   | Elva estela zorrillagallegos         | Pj camana  545 ah José jalvez           | 980581158         |                             | DUO INTERNET 400 MBPS ESTANDAR DIGITAL TV HD | 1696899190    | "dramirezoca	 "      | FINANCIADO     | Si                |
| ARI CUTIPA JANETH VANESSA          | ZEBALLOS ESCOBAR KAREN YULEISY   | REGION SUR    | AREQUIPA| 43927183     | Venta Fija   | Cliente sin asignar  | 02/05/2025 15:48:16  | FE-1087599232    | Captación      | 29652352   | Celestino Teofilo Tapia Roque       | Cl. Alfonso Ugarte 212 Ur. Jorge Chavez | 970299367         |                             | DUO PLANO LOCAL 400 MBPS                 | 1695914548    | Mhurtadosala        | FINANCIADO     | No                |
| ARI CUTIPA JANETH VANESSA          | ZEBALLOS ESCOBAR KAREN YULEISY   | REGION SUR    | AREQUIPA| 43927183     | Venta Fija   | Cliente sin asignar  | 02/05/2025 15:51:57  | FE-1087689653    | Captación      | 45880470   | Ella Patricia Cervantes Rodriguez     | Cl. Ricardo Palma 102a As. Huaranguillo | 980470183         |                             | DUO INTERNET 400 MBPS ESTANDAR DIGITAL TV HD | 1696913547    | vvaldezgarc         | FINANCIADO PILOTO | Si                |

"""