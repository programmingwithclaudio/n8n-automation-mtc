import pandas as pd
import os
import logging
from datetime import datetime
from sqlalchemy import create_engine, text, inspect
import numpy as np

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(r'manual_migrate\carga_incremental.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

# Configuración de la conexión a la base de datos
DB_CONFIG = {
    "servidor": "localhost",
    "db": "testdbauren",
    "user": "postgres",
    "password": "localpassword"
}

# Nombre de la tabla destino
TABLA_DESTINO = "detallado"

# Configuración de ruta del archivo
ARCHIVO_EXCEL = r"C:\Users\oak\Downloads\Detallado.xlsx"

def crear_conexion():
    """Crear conexión a PostgreSQL"""
    try:
        connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['servidor']}/{DB_CONFIG['db']}"
        engine = create_engine(connection_string)
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
            # Definición de la tabla basada en la estructura del Excel
            query = f"""
            CREATE TABLE {TABLA_DESTINO} (
                id SERIAL PRIMARY KEY,
                codigo_fe VARCHAR(20) UNIQUE,
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
                actualizado_el TIMESTAMP
            )
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

def cargar_datos_excel():
    """Cargar datos desde el archivo Excel"""
    try:
        logger.info(f"Cargando datos desde {ARCHIVO_EXCEL}")
        df = pd.read_excel(ARCHIVO_EXCEL)
        
        # Renombrar columnas para estandarizar
        df.columns = [
            'usuario', 'supervisor', 'region', 'zonal', 'dni_vendedor', 
            'formulario', 'cliente', 'fecha', 'codigo_fe', 'tipo_operacion', 
            'dni_cliente', 'nombre_cliente', 'direccion_instalacion', 
            'telefono_contacto', 'telefono_contacto_otro', 'producto', 
            'nro_pedido', 'usuario_dito', 'scoring_dito', 'es_venta_hoy'
        ]
        
        # Convertir columnas de fechas
        df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce')
        
        # Reemplazar valores NaN con None para SQLAlchemy
        df = df.replace({np.nan: None})
        
        logger.info(f"Se cargaron {len(df)} registros del archivo Excel")
        return df
    except Exception as e:
        logger.error(f"Error al cargar datos del Excel: {e}")
        raise

def obtener_registros_existentes(engine, codigos_fe):
    """Obtener registros que ya existen en la base de datos basados en código FE"""
    if not codigos_fe:
        return []
    
    try:
        placeholders = ','.join([f"'{codigo}'" for codigo in codigos_fe])
        query = f"""
        SELECT codigo_fe, fecha 
        FROM {TABLA_DESTINO} 
        WHERE codigo_fe IN ({placeholders})
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query))
            existing_records = {row[0]: row[1] for row in result}
        
        logger.info(f"Se encontraron {len(existing_records)} registros existentes")
        return existing_records
    except Exception as e:
        logger.error(f"Error al consultar registros existentes: {e}")
        raise

def procesar_carga_incremental(engine, df):
    """Realizar la carga incremental de datos"""
    try:
        # Obtener códigos FE del DataFrame
        codigos_fe = df['codigo_fe'].tolist()
        
        # Obtener registros existentes
        registros_existentes = obtener_registros_existentes(engine, codigos_fe)
        
        # Separar registros para inserción y actualización
        registros_nuevos = df[~df['codigo_fe'].isin(registros_existentes.keys())]
        registros_actualizar = df[df['codigo_fe'].isin(registros_existentes.keys())]
        
        # Insertar registros nuevos
        if not registros_nuevos.empty:
            registros_nuevos.to_sql(
                TABLA_DESTINO, 
                engine, 
                if_exists='append', 
                index=False,
                method='multi'
            )
            logger.info(f"Se insertaron {len(registros_nuevos)} registros nuevos")
        
        # Actualizar registros existentes
        registros_actualizados = 0
        timestamp_actual = datetime.now()
        
        for _, row in registros_actualizar.iterrows():
            codigo_fe = row['codigo_fe']
            # Convertir la fila a diccionario y eliminar codigo_fe para la actualización
            datos = row.to_dict()
            
            # Construir la consulta de actualización
            set_clauses = []
            params = {}
            
            for column, value in datos.items():
                if column != 'codigo_fe':  # No actualizar la clave primaria
                    set_clauses.append(f"{column} = :{column}")
                    params[column] = value
            
            # Agregar timestamp de actualización
            set_clauses.append("actualizado_el = :actualizado_el")
            params['actualizado_el'] = timestamp_actual
            params['codigo_fe'] = codigo_fe
            
            query = f"""
            UPDATE {TABLA_DESTINO}
            SET {', '.join(set_clauses)}
            WHERE codigo_fe = :codigo_fe
            """
            
            with engine.connect() as conn:
                conn.execute(text(query), params)
                conn.commit()
                registros_actualizados += 1
        
        logger.info(f"Se actualizaron {registros_actualizados} registros existentes")
        
        return {
            'nuevos': len(registros_nuevos),
            'actualizados': registros_actualizados,
            'total_procesados': len(df)
        }
    except Exception as e:
        logger.error(f"Error en la carga incremental: {e}")
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
        
        # Procesar carga incremental
        resultados = procesar_carga_incremental(engine, df)
        
        logger.info(f"Proceso completado: {resultados['nuevos']} registros nuevos, "
                   f"{resultados['actualizados']} registros actualizados, "
                   f"{resultados['total_procesados']} registros procesados en total")
        
        return resultados
    except Exception as e:
        logger.error(f"Error en el proceso de carga incremental: {e}")
        raise

if __name__ == "__main__":
    main()