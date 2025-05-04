import pandas as pd
import os
from datetime import datetime
import logging
import hashlib
import sys
from sqlalchemy import create_engine, text, inspect
import traceback

# Configuración de logging con UTF-8 y DEBUG
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
log_filename = rf'manual_migrate\carga_usuarios_{timestamp}.log'
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=log_filename,
    filemode='w',
    encoding='utf-8'
)
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger().addHandler(console)

# Parámetros de conexión (ajustar según entorno)
DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'database': 'testdbauren',
    'user': 'postgres',
    'password': 'localpassword'
}

# Ruta del archivo Excel
FILE_PATH = r"C:\Users\oak\Downloads\usuario.xlsx"

# Nombre de la tabla destino
TABLE_NAME = 'usuarios'

def verify_connection(params):
    """Verifica la conexión a la base de datos y retorna engine."""
    conn_str = (
        f"postgresql://{params['user']}:{params['password']}@"
        f"{params['host']}:{params['port']}/{params['database']}"
    )
    try:
        engine = create_engine(conn_str)
        with engine.connect() as conn:
            conn.execute(text('SELECT 1'))
        logging.info("Conexión exitosa a PostgreSQL")
        return engine
    except Exception as e:
        logging.error(f"Error de conexión: {e}")
        print(f"Error de conexión: {e}")
        return None

def create_hash_id(row, key_fields):
    """
    Crea un hash ID consistente basado en los campos clave proporcionados.
    """
    key_values = []
    
    for field in key_fields:
        if field in row and pd.notna(row[field]):
            # Normalizar el valor para consistencia
            val = str(row[field]).strip()
            key_values.append(val)
        else:
            # Valor consistente para campos vacíos/nulos
            key_values.append('NULL')
    
    key_str = '|'.join(key_values)
    return hashlib.md5(key_str.encode()).hexdigest()

def setup_database(engine, df_columns):
    """
    Crea o ajusta la tabla para usuarios con soporte para carga incremental.
    """
    # Columnas dinámicas según el Excel
    cols_ddl = [f'"{col}" TEXT' for col in df_columns]
    
    # Columnas adicionales para gestión incremental
    additional_cols = [
        '"fecha_proceso" DATE NOT NULL',
        '"fecha_carga" TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
        '"fecha_actualizacion" TIMESTAMP',
        '"hash_id" VARCHAR(32)',
        '"estado_carga" VARCHAR(20) DEFAULT \'nuevo\''
    ]
    
    # Construir DDL
    all_cols_ddl = cols_ddl + additional_cols
    
    # Definir PRIMARY KEY tradicional sobre dni, zonal, telefono, fecha_proceso
    pk_fields = []
    for key in ['dni', 'zonal', 'telefono']:
        for col in df_columns:
            if col.lower() == key:
                pk_fields.append(f'"{col}"')
                break
    pk_fields.append('"fecha_proceso"')
    pk_ddl = f'PRIMARY KEY ({", ".join(pk_fields)})'
    
    # Crear la tabla si no existe
    ddl = (
        f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} (\n  "
        + ",\n  ".join(all_cols_ddl + [pk_ddl])
        + "\n);"
    )
    
    with engine.begin() as conn:
        conn.execute(text(ddl))
        
        # Crear índice para hash_id
        conn.execute(text(f'''
            CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_hash_id
            ON {TABLE_NAME} (hash_id);
        '''))
        
    logging.info(f"Tabla {TABLE_NAME} configurada para soporte incremental")
    return True

def load_excel_data(filepath):
    """Carga datos desde un archivo Excel."""
    try:
        df = pd.read_excel(filepath)
        
        # Mantener casing original y quitar espacios
        df.columns = [col.strip() for col in df.columns]
        
        # Limpieza básica de datos
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace('nan', None)
        
        logging.info(f"DataFrame cargado: {len(df)} filas con {len(df.columns)} columnas")
        return df
    except Exception as e:
        logging.error(f"Error cargando Excel: {e}")
        return None

def preprocess_data(df):
    """
    Preprocesa los datos para carga incremental:
    - Rellena nulos en campos de PK
    - Agrega fecha_proceso
    - Genera hash_id para cada registro
    """
    # Rellenar nulos en columnas de clave primaria
    for key in ['dni', 'zonal', 'telefono']:
        for col in df.columns:
            if col.lower() == key:
                df[col] = df[col].fillna('DESCONOCIDO')
                break
    
    # Identificar campos clave para hash
    key_fields = []
    for key in ['dni', 'zonal', 'telefono']:
        for col in df.columns:
            if col.lower() == key:
                key_fields.append(col)
                break
    
    # Fecha de proceso (mantenemos DATE para compatibilidad)
    fecha_proceso = datetime.now().date()
    df['fecha_proceso'] = fecha_proceso
    key_fields.append('fecha_proceso')
    
    # Generar hash_id basado en campos clave
    df['hash_id'] = df.apply(lambda row: create_hash_id(row, key_fields), axis=1)
    
    # Añadir timestamps y estado inicial
    now = datetime.now()
    df['fecha_carga'] = now
    df['fecha_actualizacion'] = now
    df['estado_carga'] = 'nuevo'
    
    logging.info(f"Preprocesamiento completado. {len(df)} registros preparados.")
    return df, key_fields

def get_existing_records(engine, fecha_proceso):
    """
    Obtiene registros existentes para la fecha de proceso actual.
    """
    query = text(f"""
    SELECT * FROM {TABLE_NAME}
    WHERE "fecha_proceso" = :fecha_proceso
    """)
    
    try:
        with engine.connect() as conn:
            result = conn.execute(query, {"fecha_proceso": fecha_proceso})
            columns = result.keys()
            data = result.fetchall()
            
        if not data:
            logging.info(f"No existen registros para la fecha {fecha_proceso}")
            return pd.DataFrame()
        
        # Crear DataFrame con los resultados
        existing_df = pd.DataFrame(data, columns=columns)
        logging.info(f"Obtenidos {len(existing_df)} registros existentes para comparación")
        return existing_df
    except Exception as e:
        logging.error(f"Error al obtener registros existentes: {e}")
        return pd.DataFrame()

def incremental_load_and_update(df, engine, key_fields):
    """
    Realiza la carga incremental de usuarios:
    - Identifica registros nuevos vs existentes usando hash_id
    - Inserta nuevos registros
    - Actualiza registros existentes si hay cambios
    """
    try:
        fecha_proceso = df['fecha_proceso'].iloc[0]
        existing_records = get_existing_records(engine, fecha_proceso)
        
        if existing_records.empty:
            # No hay registros existentes para esta fecha, insertar todos
            logging.info(f"No hay registros existentes para {fecha_proceso}. Insertando todos como nuevos.")
            df.to_sql(TABLE_NAME, engine, if_exists='append', index=False, chunksize=500)
            return len(df), 0
        
        # Crear hashmap de registros existentes
        existing_hash_set = set(existing_records['hash_id'].tolist())
        existing_hash_to_id = dict(zip(existing_records['hash_id'], existing_records.index))
        
        # Separar registros nuevos y existentes
        df['is_new'] = ~df['hash_id'].isin(existing_hash_set)
        new_records = df[df['is_new']].drop(columns=['is_new'])
        existing_updates = df[~df['is_new']].drop(columns=['is_new'])
        
        # Insertar nuevos registros
        inserted_count = 0
        if not new_records.empty:
            new_records.to_sql(TABLE_NAME, engine, if_exists='append', index=False, chunksize=500)
            inserted_count = len(new_records)
            logging.info(f"Insertados {inserted_count} nuevos registros")
        
        # Identificar y actualizar registros con cambios
        updated_count = 0
        if not existing_updates.empty:
            # Identificar registros que requieren actualización
            records_to_update = []
            
            for _, row in existing_updates.iterrows():
                hash_id = row['hash_id']
                existing_row = existing_records[existing_records['hash_id'] == hash_id].iloc[0]
                
                # Comprobar si hay cambios en algún campo (excluyendo campos de control)
                exclude_cols = ['hash_id', 'fecha_carga', 'fecha_actualizacion', 'estado_carga', 'fecha_proceso']
                needs_update = False
                
                for col in row.index:
                    if col not in exclude_cols and col in existing_row:
                        # Normalizar valores para comparación
                        new_val = str(row[col]).strip() if pd.notna(row[col]) else None
                        old_val = str(existing_row[col]).strip() if pd.notna(existing_row[col]) else None
                        
                        if new_val != old_val:
                            needs_update = True
                            break
                
                if needs_update:
                    row['estado_carga'] = 'actualizado'
                    row['fecha_actualizacion'] = datetime.now()
                    records_to_update.append(row)
            
            # Ejecutar actualizaciones
            if records_to_update:
                # Construir campos para actualizar
                for update_row in records_to_update:
                    # Construir condiciones para identificar el registro específico
                    conditions = []
                    params = {}
                    
                    # Campos para la condición WHERE
                    for key in key_fields:
                        if key in update_row:
                            conditions.append(f'"{key}" = :{key}')
                            params[key] = update_row[key]
                    
                    # Campos a actualizar
                    update_fields = []
                    for col in update_row.index:
                        if col not in key_fields and col not in ['hash_id', 'fecha_carga', 'is_new']:
                            update_fields.append(f'"{col}" = :{col}')
                            params[col] = update_row[col]
                    
                    # Construir y ejecutar la consulta UPDATE
                    if update_fields and conditions:
                        update_sql = f"""
                        UPDATE {TABLE_NAME}
                        SET {', '.join(update_fields)}
                        WHERE {' AND '.join(conditions)}
                        """
                        
                        with engine.begin() as conn:
                            conn.execute(text(update_sql), params)
                        updated_count += 1
                
                logging.info(f"Actualizados {updated_count} registros con cambios")
        
        return inserted_count, updated_count
        
    except Exception as e:
        logging.error(f"Error en carga incremental: {e}")
        logging.error(traceback.format_exc())
        raise

def get_data_summary(engine, fecha_proceso):
    """Obtiene un resumen de los datos por estado de carga."""
    try:
        with engine.connect() as conn:
            # Total de registros para la fecha
            total_query = text(f"""
                SELECT COUNT(*)
                FROM {TABLE_NAME}
                WHERE "fecha_proceso" = :fecha
            """)
            total_result = conn.execute(total_query, {"fecha": fecha_proceso}).scalar()
            
            # Registros por estado
            estados_query = text(f"""
                SELECT "estado_carga", COUNT(*)
                FROM {TABLE_NAME}
                WHERE "fecha_proceso" = :fecha
                GROUP BY "estado_carga"
            """)
            estados_result = conn.execute(estados_query, {"fecha": fecha_proceso})
            estados_data = [(row[0], row[1]) for row in estados_result]
        
        return {
            "total": total_result,
            "por_estado": estados_data
        }
    except Exception as e:
        logging.error(f"Error obteniendo resumen de datos: {e}")
        return None

def main():
    """Función principal del proceso de carga incremental."""
    print("=== INICIANDO PROCESO DE CARGA INCREMENTAL DE USUARIOS ===")
    logging.info("Iniciando proceso de carga incremental")
    
    try:
        # Verificar conexión
        engine = verify_connection(DB_CONFIG)
        if not engine:
            logging.error("No se pudo establecer conexión. Proceso abortado.")
            return
        
        # Verificar archivo
        if not os.path.exists(FILE_PATH):
            logging.error(f"Archivo no existe: {FILE_PATH}")
            return
        
        # Cargar datos
        df = load_excel_data(FILE_PATH)
        if df is None or df.empty:
            logging.error("No se pudieron cargar datos del Excel. Proceso abortado.")
            return
        
        # Configurar base de datos
        setup_database(engine, list(df.columns))
        
        # Preprocesar datos
        df, key_fields = preprocess_data(df)
        
        # Mostrar muestra de datos
        print("Muestra de datos a procesar:")
        print(df.head())
        
        # Procesar carga incremental
        fecha_proceso = df['fecha_proceso'].iloc[0]
        inserted, updated = incremental_load_and_update(df, engine, key_fields)
        
        # Obtener resumen
        summary = get_data_summary(engine, fecha_proceso)
        
        # Mostrar resultados
        print("\n=== RESUMEN DE CARGA ===")
        print(f"Fecha de proceso: {fecha_proceso}")
        print(f"Registros nuevos insertados: {inserted}")
        print(f"Registros actualizados: {updated}")
        
        if summary:
            print(f"\nTotal de registros para fecha {fecha_proceso}: {summary['total']}")
            print("Distribución por estado:")
            for estado, count in summary['por_estado']:
                print(f"  - {estado}: {count}")
        
        logging.info("Proceso completado exitosamente")
        print("\n=== PROCESO COMPLETADO EXITOSAMENTE ===")
        
    except Exception as e:
        logging.error(f"Error en proceso principal: {e}")
        logging.error(traceback.format_exc())
        print(f"Error: {e}")

if __name__ == "__main__":
    main()