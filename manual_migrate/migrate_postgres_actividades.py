import pandas as pd
import os
from datetime import datetime
import logging
import hashlib
import sys
from sqlalchemy import create_engine, text, inspect
import traceback

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename= r'manual_migrate\carga_actividades.log',
    filemode='a'
)
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

# Configuración de conexión a la base de datos
DB_CONFIG = {
    "servidor": "localhost",
    "db": "testdbauren",
    "user": "postgres",
    "password": "localpassword"
}
FILE_PATH = r"C:\Users\oak\Downloads\Actividad04.xlsx"

def verify_connection(config):
    """Verifica la conexión a la base de datos"""
    connection_string = f"postgresql+psycopg2://{config['user']}:{config['password']}@{config['servidor']}/{config['db']}"
    try:
        engine = create_engine(connection_string)
        with engine.connect() as conn:
            logging.info("Conexión exitosa a PostgreSQL")
        return engine
    except Exception as e:
        logging.error(f"Error de conexión: {e}")
        return None

def create_hash_id(row):
    """Crea un hash ID consistente basado en campos clave"""
    key_fields = ['fecha', 'dni_vendedor', 'actividad', 'detalle', 'latitud', 'longitud']
    key_values = []
    
    for field in key_fields:
        if field in row and pd.notna(row[field]):
            # Manejo especial para campos numéricos
            if field in ['latitud', 'longitud']:
                val = round(float(row[field]), 6)
                key_values.append(f"{val:.6f}")
            # Manejo de campos de texto
            elif field in ['actividad', 'detalle']:
                key_values.append(str(row[field]).strip().upper())
            # Campos de fecha
            elif field == 'fecha' and isinstance(row[field], (datetime, pd.Timestamp)):
                key_values.append(row[field].strftime('%Y-%m-%d %H:%M:%S'))
            else:
                key_values.append(str(row[field]).strip())
        else:
            # Valor consistente para campos vacíos/nulos
            key_values.append('NULL')
    
    key_str = '|'.join(key_values)
    return hashlib.md5(key_str.encode()).hexdigest()

def setup_database(engine):
    """Configura la base de datos y crea tablas si no existen"""
    try:
        with engine.connect() as conn:
            # Tabla principal para actividades
            conn.execute(text('''
            CREATE TABLE IF NOT EXISTS actividades (
                id SERIAL PRIMARY KEY,
                fecha TIMESTAMP,
                nombre_usuario VARCHAR(75),
                dni_vendedor VARCHAR(15),
                superior VARCHAR(75),
                actividad VARCHAR(35),
                detalle VARCHAR(55),
                motivo VARCHAR(125),
                zonas_asignadas VARCHAR(125),
                alertas VARCHAR(125),
                latitud DECIMAL(18,8),
                longitud DECIMAL(18,8),
                zona VARCHAR(35),
                estado_carga VARCHAR(20) DEFAULT 'nuevo',
                fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                hash_id VARCHAR(32),
                fecha_actualizacion TIMESTAMP
            );
            '''))

            # Crear índice para búsqueda rápida por hash_id
            conn.execute(text('''
            CREATE INDEX IF NOT EXISTS idx_actividades_hash_id
            ON actividades (hash_id);
            '''))

            # Crear índice para búsqueda por dni_vendedor
            conn.execute(text('''
            CREATE INDEX IF NOT EXISTS idx_actividades_dni
            ON actividades (dni_vendedor);
            '''))
            
            conn.commit()
        logging.info("Tablas verificadas o creadas correctamente")
        return True
    except Exception as e:
        logging.error(f"Error al configurar la base de datos: {e}")
        return False

def parse_date(date_val):
    """Convierte fechas desde Pandas Timestamp o formato DD/MM/YYYY HH:MM a datetime"""
    if pd.isna(date_val):
        return None
    if isinstance(date_val, pd.Timestamp):
        return date_val.to_pydatetime()
    if isinstance(date_val, str):
        try:
            return datetime.strptime(date_val, '%d/%m/%Y %H:%M')
        except ValueError:
            try:
                return datetime.strptime(date_val, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                logging.warning(f"Formato de fecha desconocido: {date_val}")
                return None
    return None

def load_excel_data(filepath):
    """Carga datos con dtype específico para evitar conversiones incorrectas"""
    dtype = {
        'dni_vendedor': str,
        'motivo': str,
        'zonas_asignadas': str
    }
    
    try:
        df = pd.read_excel(filepath, dtype=dtype)
        # Limpiar espacios en strings
        for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace('nan', None)
        
        logging.info(f"Datos cargados del Excel: {len(df)} filas")
        return df
    except Exception as e:
        logging.error(f"Error carga Excel: {e}")
        return None

def preprocess_data(df):
    """Preprocesa datos para normalización"""
    # Imprimir nombres de columnas originales para depuración
    logging.info(f"Columnas originales del Excel: {list(df.columns)}")
    
    column_mapping = {
        'Fecha': 'fecha',
        'Nombre Usuario': 'nombre_usuario',
        'DNI Vendedor': 'dni_vendedor',
        'Superior': 'superior',
        'Actividad': 'actividad',
        'Detalle': 'detalle',
        'Motivo': 'motivo',
        'Zonas Asignadas': 'zonas_asignadas',
        'Alertas': 'alertas',
        'Latitud': 'latitud',
        'Longitud': 'longitud',
        'Zona': 'zona'
    }
    
    # Verificar si existen las columnas antes de renombrar
    for col, new_col in column_mapping.items():
        if col in df.columns:
            df = df.rename(columns={col: new_col})
        else:
            logging.warning(f"Columna '{col}' no encontrada en el archivo Excel")
    
    logging.info(f"Columnas después de renombrar: {list(df.columns)}")
    
    # Parsear y convertir campos de fecha
    if 'fecha' in df.columns:
        df['fecha'] = df['fecha'].apply(parse_date)
    
    # Normalizar campos de texto
    text_columns = ['nombre_usuario', 'dni_vendedor', 'superior', 'actividad', 'detalle', 
                    'motivo', 'zonas_asignadas', 'alertas', 'zona']
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace('nan', None)
    
    # Estandarizar case de texto para campos clave
    for col in ['actividad', 'detalle']:
        if col in df.columns:
            df[col] = df[col].str.upper()
    
    # Manejar campos numéricos
    for col in ['latitud', 'longitud']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].round(6)
    
    # Crear hash ID para cada registro
    df['hash_id'] = df.apply(create_hash_id, axis=1)
    
    # Añadir timestamps
    now = datetime.now()
    df['fecha_carga'] = now
    df['fecha_actualizacion'] = now
    df['estado_carga'] = 'nuevo'  # Estado por defecto para nuevos registros
    
    logging.info(f"Preprocesamiento completado. Registros: {len(df)}")
    return df

def get_existing_records(engine):
    """Obtiene registros existentes para detección de duplicados y actualizaciones"""
    query = """
    SELECT id, hash_id, fecha, nombre_usuario, dni_vendedor, superior, actividad, 
           detalle, motivo, zonas_asignadas, alertas, latitud, longitud, zona
    FROM actividades
    """
    try:
        existing_records = pd.read_sql(query, engine)
        logging.info(f"Obtenidos {len(existing_records)} registros existentes para comparación")
        return existing_records
    except Exception as e:
        logging.error(f"Error al obtener registros existentes: {e}")
        return pd.DataFrame()

def normalize_value_for_comparison(value, column_type):
    """Normaliza valores para comparación consistente"""
    if pd.isna(value):
        return None
    
    if column_type == 'numeric':
        try:
            return round(float(value), 6) if value is not None else None
        except (ValueError, TypeError):
            return None
    elif column_type == 'text':
        return str(value).strip().upper() if value is not None else None
    elif column_type == 'date':
        if isinstance(value, (datetime, pd.Timestamp)):
            return value
        try:
            return parse_date(value)
        except:
            return value
    return value

def values_are_different(old_val, new_val, column_type):
    """Comprueba si dos valores son diferentes, manejando la conversión de tipos"""
    old_normalized = normalize_value_for_comparison(old_val, column_type)
    new_normalized = normalize_value_for_comparison(new_val, column_type)
    
    # Manejar valores None/NULL
    if old_normalized is None and new_normalized is None:
        return False
    if old_normalized is None or new_normalized is None:
        return True
    
    # Caso especial para fechas
    if column_type == 'date' and isinstance(old_normalized, datetime) and isinstance(new_normalized, datetime):
        return old_normalized.strftime('%Y-%m-%d %H:%M:%S') != new_normalized.strftime('%Y-%m-%d %H:%M:%S')
    
    return old_normalized != new_normalized

def incremental_load_and_update(df, engine):
    """Realiza la carga incremental de datos identificando registros nuevos y actualizaciones"""
    try:
        existing_records = get_existing_records(engine)
        
        if existing_records.empty:
            # Si no hay registros existentes, insertar todos como nuevos
            logging.info("No hay registros existentes. Insertando todos como nuevos.")
            required_cols = ['fecha', 'nombre_usuario', 'dni_vendedor', 'superior', 
                           'actividad', 'detalle', 'motivo', 'zonas_asignadas', 
                           'alertas', 'latitud', 'longitud', 'zona', 
                           'hash_id', 'fecha_carga', 'fecha_actualizacion', 'estado_carga']
            
            # Asegurar que existan todas las columnas requeridas
            for col in required_cols:
                if col not in df.columns:
                    if col in ['fecha_carga', 'fecha_actualizacion']:
                        df[col] = datetime.now()
                    elif col == 'estado_carga':
                        df[col] = 'nuevo'
                    else:
                        df[col] = None
            
            # Insertar todos los registros
            insert_df = df[required_cols]
            insert_df.to_sql('actividades', engine, if_exists='append', 
                          index=False, chunksize=500, method='multi')
            return len(df), 0
        
        # Crear mapa de hash para búsqueda rápida
        existing_hash_map = dict(zip(existing_records['hash_id'], existing_records['id']))
        existing_hash_set = set(existing_records['hash_id'])
        
        # Identificar registros nuevos
        df['is_new'] = ~df['hash_id'].isin(existing_hash_set)
        new_records_df = df[df['is_new']]
        
        # Identificar registros existentes que podrían necesitar actualizaciones
        existing_records_df = df[~df['is_new']].copy()
        
        # Insertar registros nuevos
        inserted_count = 0
        if not new_records_df.empty:
            required_cols = ['fecha', 'nombre_usuario', 'dni_vendedor', 'superior', 
                           'actividad', 'detalle', 'motivo', 'zonas_asignadas', 
                           'alertas', 'latitud', 'longitud', 'zona', 
                           'hash_id', 'fecha_carga', 'fecha_actualizacion', 'estado_carga']
            
            insert_df = new_records_df[required_cols]
            insert_df.to_sql('actividades', engine, if_exists='append', 
                          index=False, chunksize=500, method='multi')
            inserted_count = len(new_records_df)
            logging.info(f"Nuevos registros insertados: {inserted_count}")
        
        # Procesar actualizaciones para registros existentes
        updated_count = 0
        if not existing_records_df.empty:
            records_to_update = []
            
            # Añadir columna record_id desde el mapa de hash
            existing_records_df['record_id'] = existing_records_df['hash_id'].map(existing_hash_map)
            
            # Recorrer registros que potencialmente necesitan actualizaciones
            for _, row in existing_records_df.iterrows():
                record_id = row['record_id']
                hash_id = row['hash_id']
                
                # Obtener el registro existente
                existing_row = existing_records[existing_records['id'] == record_id].iloc[0]
                
                # Verificar cambios en cada columna
                needs_update = False
                column_types = {
                    'nombre_usuario': 'text', 'dni_vendedor': 'text', 'superior': 'text',
                    'actividad': 'text', 'detalle': 'text', 'motivo': 'text',
                    'zonas_asignadas': 'text', 'alertas': 'text', 'zona': 'text',
                    'latitud': 'numeric', 'longitud': 'numeric',
                    'fecha': 'date'
                }
                
                for col, col_type in column_types.items():
                    if col in existing_row and col in row:
                        if values_are_different(existing_row[col], row[col], col_type):
                            needs_update = True
                            break
                
                # Si se detectan cambios, preparar actualización
                if needs_update:
                    row['estado_carga'] = 'actualizado'
                    records_to_update.append(row)
            
            # Ejecutar actualizaciones para registros con cambios
            with engine.begin() as conn:
                for row in records_to_update:
                    record_id = row['record_id']
                    
                    try:
                        # Actualizar registro principal
                        update_values = {
                            'nombre_usuario': row['nombre_usuario'],
                            'superior': row['superior'],
                            'actividad': row['actividad'],
                            'detalle': row['detalle'],
                            'motivo': row['motivo'],
                            'zonas_asignadas': row['zonas_asignadas'],
                            'alertas': row['alertas'],
                            'latitud': row['latitud'],
                            'longitud': row['longitud'],
                            'zona': row['zona'],
                            'fecha_actualizacion': datetime.now(),
                            'estado_carga': 'actualizado',
                            'id': record_id
                        }
                        
                        update_query = text("""
                            UPDATE actividades SET
                                nombre_usuario = :nombre_usuario,
                                superior = :superior,
                                actividad = :actividad,
                                detalle = :detalle,
                                motivo = :motivo,
                                zonas_asignadas = :zonas_asignadas,
                                alertas = :alertas,
                                latitud = :latitud,
                                longitud = :longitud,
                                zona = :zona,
                                fecha_actualizacion = :fecha_actualizacion,
                                estado_carga = :estado_carga
                            WHERE id = :id
                        """)
                        conn.execute(update_query, update_values)
                        updated_count += 1
                    except Exception as e:
                        logging.error(f"Error actualizando registro {record_id}: {e}")
                        continue
            
            logging.info(f"Registros actualizados con cambios: {updated_count}")
        
        return inserted_count, updated_count

    except Exception as e:
        logging.error(f"Error en el proceso de carga/actualización: {str(e)}")
        logging.error(traceback.format_exc())
        raise

def check_table_columns(engine, table_name):
    """Verifica y añade columnas faltantes con tipos correctos"""
    try:
        inspector = inspect(engine)
        columns = inspector.get_columns(table_name)
        existing_columns = [col['name'] for col in columns]
        
        required_columns = {
            'hash_id': 'VARCHAR(32)',
            'fecha_actualizacion': 'TIMESTAMP',
            'estado_carga': 'VARCHAR(20)'
        }
        
        with engine.begin() as conn:
            for col, col_type in required_columns.items():
                if col not in existing_columns:
                    conn.execute(text(f'ALTER TABLE {table_name} ADD COLUMN {col} {col_type}'))
                    logging.info(f"Columna {col} añadida a la tabla {table_name}")
        
        return True
    except Exception as e:
        logging.error(f"Error en verificación de columnas: {e}")
        return False

def get_data_summary(engine):
    """Obtiene un resumen de los datos almacenados"""
    try:
        summary_queries = {
            "total_records": "SELECT COUNT(*) FROM actividades",
            "records_by_status": "SELECT estado_carga, COUNT(*) FROM actividades GROUP BY estado_carga",
            "records_by_activity": "SELECT actividad, COUNT(*) FROM actividades GROUP BY actividad ORDER BY COUNT(*) DESC LIMIT 5"
        }
        
        results = {}
        with engine.connect() as conn:
            for key, query in summary_queries.items():
                result = conn.execute(text(query))
                if key == "total_records":
                    results[key] = result.scalar()
                else:
                    results[key] = [(row[0], row[1]) for row in result]
        
        return results
    except Exception as e:
        logging.error(f"Error obteniendo resumen de datos: {e}")
        return None

def main():
    """Función principal que ejecuta el proceso de carga incremental y actualización"""
    try:
        print("=== INICIANDO PROCESO DE CARGA INCREMENTAL DE DATOS ===")
        logging.info("Iniciando proceso de carga y actualización de datos")
        
        # Verificar y establecer conexión
        engine = verify_connection(DB_CONFIG)
        if not engine:
            logging.error("No se pudo establecer conexión con la base de datos. Abortando.")
            return
        
        # Configurar base de datos
        if not setup_database(engine):
            logging.error("No se pudo configurar la base de datos. Abortando.")
            return
        
        # Verificar columnas necesarias
        if not check_table_columns(engine, 'actividades'):
            logging.error("No se pudieron verificar o crear columnas necesarias. Abortando.")
            return
        
        # Verificar si el archivo existe
        if not os.path.exists(FILE_PATH):
            logging.error(f"El archivo {FILE_PATH} no existe.")
            return
        
        # Cargar datos desde Excel
        df = load_excel_data(FILE_PATH)
        if df is None or df.empty:
            logging.error("No se pudo cargar el archivo Excel o está vacío. Abortando.")
            return
        
        # Mostrar datos de muestra para depuración
        print("Primeras filas del Excel:")
        print(df.head())
        print(f"Registros en el archivo Excel: {len(df)}")
        
        # Preprocesar datos
        df = preprocess_data(df)
        
        # Mostrar datos procesados
        print("Primeras filas después del preprocesamiento:")
        print(df.head())
        print(f"Registros después del preprocesamiento: {len(df)}")
        
        # Realizar carga incremental y actualización
        inserted_count, updated_count = incremental_load_and_update(df, engine)
        
        # Obtener resumen de datos
        data_summary = get_data_summary(engine)
        
        # Generar informe
        logging.info(f"Proceso completado exitosamente. Nuevos: {inserted_count}, Actualizados: {updated_count}")
        print("\n=== PROCESO COMPLETADO EXITOSAMENTE ===")
        print(f"Se insertaron {inserted_count} registros nuevos y se actualizaron {updated_count} registros.")
        
        if data_summary:
            print("\n=== RESUMEN DE DATOS EN LA BASE ===")
            print(f"Total de registros: {data_summary['total_records']}")
            print("\nRegistros por estado:")
            for estado, count in data_summary['records_by_status']:
                print(f"  - {estado}: {count}")
            print("\nPrincipales actividades:")
            for actividad, count in data_summary['records_by_activity']:
                print(f"  - {actividad}: {count}")
        
    except Exception as e:
        logging.error(f"Error en el proceso principal: {e}")
        print(f"Error en el proceso: {e}")
        print(traceback.format_exc())

if __name__ == "__main__":
    main()