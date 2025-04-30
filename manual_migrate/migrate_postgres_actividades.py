import pandas as pd
import os
from datetime import datetime
import logging
import hashlib
import sys
from sqlalchemy import create_engine, text, inspect
import traceback

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='data_load.log',
    filemode='a'
)
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

# Database connection configuration
servidor = "localhost"
db = "postgres"
user = "postgres"
password = "localpassword"
file_path = r"C:\Users\oak\Downloads\Actividad.xlsx"
#file_path = r"C:\Users\jimmy.atao\Downloads\Actividad.xlsx"

def verify_con(host, db, user, password):
    """Verifies database connection"""
    connection_string = f"postgresql+psycopg2://{user}:{password}@{host}/{db}"
    try:
        engine = create_engine(connection_string)
        with engine.connect() as conn:
            logging.info("Conexión exitosa a PostgreSQL")
        return engine
    except Exception as e:
        logging.error(f"Error de conexión: {e}")
        return None

def create_hash_id(row):
    """Creates a consistent hash ID based on key fields"""
    key_fields = ['fecha', 'dni_vendedor', 'actividad', 'detalle', 'latitud', 'longitud']
    key_values = []
    
    for field in key_fields:
        if field in row and pd.notna(row[field]):
            # Special handling for numeric fields
            if field in ['latitud', 'longitud']:
                # Round to 6 decimals and format to avoid scientific notation
                val = round(float(row[field]), 6)
                key_values.append(f"{val:.6f}")
            # Handling text fields
            elif field in ['actividad', 'detalle']:
                key_values.append(str(row[field]).strip().upper())
            # Date fields
            elif field == 'fecha' and isinstance(row[field], (datetime, pd.Timestamp)):
                key_values.append(row[field].strftime('%Y-%m-%d %H:%M:%S'))
            else:
                key_values.append(str(row[field]).strip())
        else:
            # Consistent value for empty/null fields
            key_values.append('NULL')
    
    key_str = '|'.join(key_values)
    return hashlib.md5(key_str.encode()).hexdigest()

def setup_database(engine):
    """Sets up database and creates tables if they don't exist"""
    try:
        with engine.connect() as conn:
            # Main table for activities
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
                fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                hash_id VARCHAR(32),
                fecha_actualizacion TIMESTAMP
            );
            '''))

            # Create index for fast search by hash_id
            conn.execute(text('''
            CREATE INDEX IF NOT EXISTS idx_actividades_hash_id
            ON actividades (hash_id);
            '''))

            # Create index for search by dni_vendedor
            conn.execute(text('''
            CREATE INDEX IF NOT EXISTS idx_actividades_dni
            ON actividades (dni_vendedor);
            '''))

            # History table for change tracking
            conn.execute(text('''
            CREATE TABLE IF NOT EXISTS actividades_historial (
                id SERIAL PRIMARY KEY,
                actividad_id INTEGER,
                campo_modificado VARCHAR(50),
                valor_anterior TEXT,
                valor_nuevo TEXT,
                fecha_modificacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                usuario_modificacion VARCHAR(50)
            );
            '''))
            
            conn.commit()
        logging.info("Tablas verificadas o creadas correctamente")
        return True
    except Exception as e:
        logging.error(f"Error al configurar la base de datos: {e}")
        return False

def parse_date(date_val):
    """Converts dates from Pandas Timestamp or DD/MM/YYYY HH:MM format to datetime"""
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
    """Loads data with specific dtype to avoid incorrect conversions"""
    dtype = {
        'dni_vendedor': str,
        'motivo': str,
        'zonas_asignadas': str
    }
    
    try:
        df = pd.read_excel(filepath, dtype=dtype)
        # Clean spaces in strings
        for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace('nan', None)
        
        logging.info(f"Datos cargados del Excel: {len(df)} filas")
        return df
    except Exception as e:
        logging.error(f"Error carga Excel: {e}")
        return None

def preprocess_data(df):
    """Preprocesses data for normalization"""
    # Print original column names for debugging
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
    
    # Check if columns exist before renaming
    for col, new_col in column_mapping.items():
        if col in df.columns:
            df = df.rename(columns={col: new_col})
        else:
            logging.warning(f"Columna '{col}' no encontrada en el archivo Excel")
    
    logging.info(f"Columnas después de renombrar: {list(df.columns)}")
    
    # Parse and convert date fields
    if 'fecha' in df.columns:
        df['fecha'] = df['fecha'].apply(parse_date)
    
    # Normalize text fields
    text_columns = ['nombre_usuario', 'dni_vendedor', 'superior', 'actividad', 'detalle', 
                    'motivo', 'zonas_asignadas', 'alertas', 'zona']
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace('nan', None)
    
    # Standardize text case for key fields
    for col in ['actividad', 'detalle']:
        if col in df.columns:
            df[col] = df[col].str.upper()
    
    # Handle numeric fields
    for col in ['latitud', 'longitud']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].round(6)
    
    # Create hash ID for each record
    df['hash_id'] = df.apply(create_hash_id, axis=1)
    
    # Add timestamps
    now = datetime.now()
    df['fecha_carga'] = now
    df['fecha_actualizacion'] = now
    
    logging.info(f"Preprocesamiento completado. Registros: {len(df)}")
    logging.info(f"Columnas finales: {list(df.columns)}")
    return df

def get_existing_records(engine):
    """Gets existing records for duplicate detection and updates"""
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
    """Normalizes values for consistent comparison"""
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
    """Checks if two values are different, handling type conversion"""
    old_normalized = normalize_value_for_comparison(old_val, column_type)
    new_normalized = normalize_value_for_comparison(new_val, column_type)
    
    # Handle None/NULL values
    if old_normalized is None and new_normalized is None:
        return False
    if old_normalized is None or new_normalized is None:
        return True
    
    # Special case for dates
    if column_type == 'date' and isinstance(old_normalized, datetime) and isinstance(new_normalized, datetime):
        return old_normalized.strftime('%Y-%m-%d %H:%M:%S') != new_normalized.strftime('%Y-%m-%d %H:%M:%S')
    
    return old_normalized != new_normalized

def incremental_load_and_update(df, engine):
    """Performs incremental data loading by identifying new records and updates"""
    try:
        existing_records = get_existing_records(engine)
        
        if existing_records.empty:
            # If no existing records, insert all as new
            logging.info("No hay registros existentes. Insertando todos como nuevos.")
            required_cols = ['fecha', 'nombre_usuario', 'dni_vendedor', 'superior', 
                           'actividad', 'detalle', 'motivo', 'zonas_asignadas', 
                           'alertas', 'latitud', 'longitud', 'zona', 
                           'hash_id', 'fecha_carga', 'fecha_actualizacion']
            
            # Ensure all required columns exist
            for col in required_cols:
                if col not in df.columns:
                    if col in ['fecha_carga', 'fecha_actualizacion']:
                        df[col] = datetime.now()
                    else:
                        df[col] = None
            
            # Insert all records
            insert_df = df[required_cols]
            insert_df.to_sql('actividades', engine, if_exists='append', 
                          index=False, chunksize=500, method='multi')
            return len(df), 0
        
        # Create hash map for quick lookup
        existing_hash_map = dict(zip(existing_records['hash_id'], existing_records['id']))
        existing_hash_set = set(existing_records['hash_id'])
        
        # Identify new records
        df['is_new'] = ~df['hash_id'].isin(existing_hash_set)
        new_records_df = df[df['is_new']]
        
        # Identify existing records that might need updates
        existing_records_df = df[~df['is_new']].copy()
        
        # Insert new records
        inserted_count = 0
        if not new_records_df.empty:
            required_cols = ['fecha', 'nombre_usuario', 'dni_vendedor', 'superior', 
                           'actividad', 'detalle', 'motivo', 'zonas_asignadas', 
                           'alertas', 'latitud', 'longitud', 'zona', 
                           'hash_id', 'fecha_carga', 'fecha_actualizacion']
            
            insert_df = new_records_df[required_cols]
            insert_df.to_sql('actividades', engine, if_exists='append', 
                          index=False, chunksize=500, method='multi')
            inserted_count = len(new_records_df)
            logging.info(f"Nuevos registros insertados: {inserted_count}")
        
        # Process updates for existing records
        updated_count = 0
        if not existing_records_df.empty:
            changes_log = []
            
            # Add record_id column from hash map
            existing_records_df['record_id'] = existing_records_df['hash_id'].map(existing_hash_map)
            
            # Loop through records that need potential updates
            for _, row in existing_records_df.iterrows():
                record_id = row['record_id']
                hash_id = row['hash_id']
                
                # Get the existing record
                existing_row = existing_records[existing_records['id'] == record_id].iloc[0]
                
                # Check for changes in each column
                changes = {}
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
                            changes[col] = {
                                'old': existing_row[col], 
                                'new': row[col]
                            }
                
                # If changes detected, prepare update
                if changes:
                    changes_log.append({
                        'record_id': record_id,
                        'changes': changes,
                        'row': row
                    })
            
            # Execute updates for records with changes
            with engine.begin() as conn:
                for change_entry in changes_log:
                    record_id = change_entry['record_id']
                    row = change_entry['row']
                    changes = change_entry['changes']
                    
                    try:
                        # Update main record
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
                                fecha_actualizacion = :fecha_actualizacion
                            WHERE id = :id
                        """)
                        conn.execute(update_query, update_values)
                        
                        # Log changes to history table
                        for col, change in changes.items():
                            historial_query = text("""
                                INSERT INTO actividades_historial 
                                (actividad_id, campo_modificado, valor_anterior, 
                                 valor_nuevo, usuario_modificacion)
                                VALUES (:id, :campo, :old_val, :new_val, :usuario)
                            """)
                            conn.execute(historial_query, {
                                'id': record_id,
                                'campo': col,
                                'old_val': str(change['old']),
                                'new_val': str(change['new']),
                                'usuario': 'sistema_etl'
                            })
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

def generate_report(engine, inserted_count, updated_count):
    """Generates a report on loaded data"""
    try:
        stats_query = """
        SELECT 
            COUNT(*) as total_registros,
            COUNT(DISTINCT dni_vendedor) as total_vendedores,
            MIN(fecha) as fecha_minima,
            MAX(fecha) as fecha_maxima
        FROM actividades
        """
        stats = pd.read_sql(stats_query, engine)
        
        top_vendedores = """
        SELECT 
            dni_vendedor,
            nombre_usuario,
            COUNT(*) as total_actividades
        FROM actividades
        GROUP BY dni_vendedor, nombre_usuario
        ORDER BY total_actividades DESC
        LIMIT 10
        """
        vendedores = pd.read_sql(top_vendedores, engine)
        
        actividades = """
        SELECT 
            actividad,
            COUNT(*) as cantidad
        FROM actividades
        GROUP BY actividad
        ORDER BY cantidad DESC
        """
        dist_actividades = pd.read_sql(actividades, engine)
        
        current_load_stats = f"""
        === RESUMEN DE LA CARGA ACTUAL ===
        Registros nuevos insertados: {inserted_count}
        Registros existentes actualizados: {updated_count}
        Total procesados: {inserted_count + updated_count}
        """
        
        print("\n=== REPORTE DE DATOS ===")
        print(current_load_stats)
        print(f"Total registros en BD: {stats['total_registros'].values[0]}")
        print(f"Total vendedores: {stats['total_vendedores'].values[0]}")
        print(f"Período de datos: {stats['fecha_minima'].values[0]} a {stats['fecha_maxima'].values[0]}")
        
        print("\nTop 5 vendedores por actividad:")
        print(vendedores.head(5))
        
        print("\nDistribución de actividades:")
        print(dist_actividades)
        
        with open('reporte_carga.txt', 'w') as f:
            f.write("=== REPORTE DE DATOS ===\n")
            f.write(current_load_stats + "\n")
            f.write(f"Total registros en BD: {stats['total_registros'].values[0]}\n")
            f.write(f"Total vendedores: {stats['total_vendedores'].values[0]}\n")
            f.write(f"Período de datos: {stats['fecha_minima'].values[0]} a {stats['fecha_maxima'].values[0]}\n\n")
            f.write("Top 10 vendedores por actividad:\n")
            f.write(vendedores.to_string(index=False))
            f.write("\n\nDistribución de actividades:\n")
            f.write(dist_actividades.to_string(index=False))
        
        logging.info("Reporte generado correctamente")
    except Exception as e:
        logging.error(f"Error al generar el reporte: {e}")
        print(f"Error al generar el reporte: {e}")

def check_table_columns(engine, table_name):
    """Verifies and adds missing columns with correct types"""
    try:
        inspector = inspect(engine)
        columns = inspector.get_columns(table_name)
        existing_columns = [col['name'] for col in columns]
        
        required_columns = {
            'hash_id': 'VARCHAR(32)',
            'fecha_actualizacion': 'TIMESTAMP'
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

def main():
    """Main function that executes the incremental load and update process"""
    try:
        print("=== INICIANDO PROCESO DE CARGA INCREMENTAL DE DATOS ===")
        logging.info("Iniciando proceso de carga y actualización de datos")
        
        # Verify and establish connection
        engine = verify_con(servidor, db, user, password)
        if not engine:
            logging.error("No se pudo establecer conexión con la base de datos. Abortando.")
            return
        
        # Set up database
        if not setup_database(engine):
            logging.error("No se pudo configurar la base de datos. Abortando.")
            return
        
        # Verify necessary columns
        if not check_table_columns(engine, 'actividades'):
            logging.error("No se pudieron verificar o crear columnas necesarias. Abortando.")
            return
        
        # Verify if file exists
        if not os.path.exists(file_path):
            logging.error(f"El archivo {file_path} no existe.")
            return
        
        # Load data from Excel
        df = load_excel_data(file_path)
        if df is None or df.empty:
            logging.error("No se pudo cargar el archivo Excel o está vacío. Abortando.")
            return
        
        # Show sample data for debugging
        print("Primeras filas del Excel:")
        print(df.head())
        print(f"Registros en el archivo Excel: {len(df)}")
        
        # Preprocess data
        df = preprocess_data(df)
        
        # Show processed data
        print("Primeras filas después del preprocesamiento:")
        print(df.head())
        print(f"Registros después del preprocesamiento: {len(df)}")
        
        # Perform incremental load and update
        inserted_count, updated_count = incremental_load_and_update(df, engine)
        
        # Generate report
        generate_report(engine, inserted_count, updated_count)
        
        logging.info(f"Proceso completado exitosamente. Nuevos: {inserted_count}, Actualizados: {updated_count}")
        print("\n=== PROCESO COMPLETADO EXITOSAMENTE ===")
        print(f"Se insertaron {inserted_count} registros nuevos y se actualizaron {updated_count} registros.")
        
    except Exception as e:
        logging.error(f"Error en el proceso principal: {e}")
        print(f"Error en el proceso: {e}")
        print(traceback.format_exc())

if __name__ == "__main__":
    main()