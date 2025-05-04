import pandas as pd
import os
from datetime import datetime
import logging
from sqlalchemy import create_engine, text
import hashlib
import psycopg2

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='cuotas_load.log'
)

# Configuración de la conexión a PostgreSQL
servidor = r"localhost"
db = "postgres"
user = "postgres"
password = "localpassword"
file_path = r"C:\Users\oak\Downloads\couta.xlsx"
#file_path = r"C:\Users\jimmy.atao\Downloads\couta.xlsx"
sheet_name = "Tabla1"
#host= "5433"
def verify_con(host, db, user, password):
    """Verifica la conexión a la base de datos"""
    connection_string = f"postgresql+psycopg2://{user}:{password}@{host}/{db}"
    try:
        engine = create_engine(connection_string)
        with engine.connect() as conn:
            print("Conexión exitosa a PostgreSQL")
        return engine
    except Exception as e:
        print(f"Error de conexión: {e}")
        return None

def setup_database(engine):
    """Configura la base de datos y crea la tabla si no existe"""
    try:
        with engine.connect() as conn:
            # En PostgreSQL, es mejor usar comillas dobles para los nombres de columnas
            conn.execute(text("""
            CREATE TABLE IF NOT EXISTS cuotas (
                id SERIAL PRIMARY KEY,
                fecha DATE,
                zonal VARCHAR(100),
                supervisor VARCHAR(100),
                valor INT,
                hash_datos VARCHAR(64),
                fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """))
            
            # Crear índice para mejorar rendimiento en búsquedas
            conn.execute(text("""
            CREATE INDEX IF NOT EXISTS IX_cuotas_hash_datos ON cuotas(hash_datos);
            """))
            
            conn.commit()
        logging.info("Tabla cuotas verificada o creada correctamente")
        return True
    except Exception as e:
        logging.error(f"Error al configurar la base de datos: {e}")
        print(f"Error al configurar la base de datos: {e}")
        return False

def parse_date(date_val):
    """Convierte fechas de Timestamp de Pandas o formato DD/MM/YYYY a datetime"""
    if pd.isna(date_val) or date_val == '' or date_val is None:
        return None
    
    # Si ya es un Timestamp de Pandas, convertirlo a datetime
    if isinstance(date_val, pd.Timestamp):
        return date_val.to_pydatetime()
    
    # Si es una cadena, intentar parsear
    if isinstance(date_val, str):
        date_formats = ['%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', '%m/%d/%Y']
        
        for fmt in date_formats:
            try:
                return datetime.strptime(date_val, fmt)
            except ValueError:
                continue
                
        logging.warning(f"Formato de fecha desconocido: {date_val}")
        return None
    else:
        logging.warning(f"Tipo de fecha no manejado: {type(date_val)}")
        return None

def load_excel_data(filepath, sheet_name):
    """Carga los datos desde un archivo Excel"""
    try:
        df = pd.read_excel(filepath, sheet_name=sheet_name)
        logging.info(f"Archivo Excel cargado correctamente: {filepath}, hoja: {sheet_name}")
        # Imprimir las primeras filas para verificar
        print("Primeras filas del Excel:")
        print(df.head())
        return df
    except Exception as e:
        logging.error(f"Error al cargar el archivo Excel: {e}")
        print(f"Error al cargar el archivo Excel: {e}")
        return None

def calculate_hash(row):
    """Calcula un hash único para cada fila basado en todos los campos excepto id y fecha_carga"""
    hash_data = ''
    for col, val in row.items():
        if col not in ['id', 'fecha_carga', 'hash_datos']:
            # Convierte todos los valores a string para el hash
            if pd.isna(val) or val is None:
                hash_data += 'NULL|'
            elif isinstance(val, datetime):
                hash_data += val.strftime('%Y-%m-%d') + '|'
            else:
                hash_data += str(val) + '|'
    
    # Crear hash SHA-256
    return hashlib.sha256(hash_data.encode()).hexdigest()

def preprocess_data(df):
    """Preprocesa los datos para normalizarlos"""
    # Verificar y crear columnas faltantes si es necesario
    expected_columns = ['Fecha', 'Zonal', 'supervisor', 'Valor']
    
    # Imprimir las columnas actuales para verificar
    print(f"Columnas originales: {df.columns.tolist()}")
    
    # Normalizar nombres de columnas (en PostgreSQL es mejor usar minúsculas)
    df.columns = [col.lower() for col in df.columns]
    print(f"Columnas después de normalizar: {df.columns.tolist()}")
    
    # Verificar si existen las columnas esperadas (ahora en minúsculas)
    expected_columns_lower = [col.lower() for col in expected_columns]
    for col in expected_columns_lower:
        if col not in df.columns:
            df[col] = None
    
    # Convertir fechas a formato estándar
    df['fecha'] = df['fecha'].apply(parse_date)
    
    # Normalizar textos
    text_columns = ['zonal', 'supervisor']
    
    for col in text_columns:
        if col in df.columns:
            # Convertir a string primero
            df[col] = df[col].astype(str)
            # Aplicar strip para eliminar espacios
            df[col] = df[col].str.strip()
            # Reemplazar 'nan' o 'None' con None para campos vacíos
            df[col] = df[col].replace(['nan', 'None', 'NaN', ''], None)
    
    # Asegurar que el valor sea un entero
    if 'valor' in df.columns:
        df['valor'] = pd.to_numeric(df['valor'], errors='coerce').fillna(0).astype(int)
    
    # Calcular hash para cada registro (utilizado para detectar cambios)
    df['hash_datos'] = df.apply(calculate_hash, axis=1)
    
    logging.info("Preprocesamiento de datos completado")
    print("Preprocesamiento de datos completado")
    
    # Imprimir los nombres de las columnas después del preprocesamiento
    print(f"Columnas finales después del preprocesamiento: {df.columns.tolist()}")
    
    # Imprimir las primeras filas para verificar que todo esté correcto
    print("Primeras filas después del preprocesamiento:")
    print(df.head())
    
    return df

def get_existing_hashes(engine):
    """Obtiene los hashes existentes en la base de datos"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT hash_datos FROM cuotas"))
            hashes = [row[0] for row in result]
            print(f"Hashes existentes encontrados: {len(hashes)}")
            return hashes
    except Exception as e:
        logging.error(f"Error al obtener hashes existentes: {e}")
        print(f"Error al obtener hashes existentes: {e}")
        return []

def incremental_load(df, engine):
    """Realiza una carga incremental de los datos"""
    try:
        # Obtener hashes existentes
        existing_hashes = get_existing_hashes(engine)
        
        # Separar registros nuevos y existentes
        new_records = df[~df['hash_datos'].isin(existing_hashes)]
        
        if new_records.empty:
            logging.info("No hay nuevos registros para cargar")
            print("No se encontraron nuevos registros para cargar")
            return 0
        
        print(f"Registros nuevos a insertar: {len(new_records)}")
        
        # Para PostgreSQL, asegurarse de que los nombres de columnas estén en minúscula
        new_records.columns = [col.lower() for col in new_records.columns]
        
        # Insertar nuevos registros
        new_records.to_sql('cuotas', engine, if_exists='append', index=False, 
                           chunksize=100, method=None)  # Cambiado method a None para PostgreSQL
        
        records_inserted = len(new_records)
        logging.info(f"Se insertaron {records_inserted} nuevos registros")
        print(f"Se insertaron {records_inserted} nuevos registros")
        return records_inserted
    except Exception as e:
        logging.error(f"Error en la carga incremental: {e}")
        print(f"Error en la carga incremental: {e}")
        return 0

def update_existing_records(df, engine):
    """Actualiza registros existentes si hay cambios"""
    try:
        # Obtener registros existentes
        with engine.connect() as conn:
            result = conn.execute(text("""
            SELECT id, fecha, zonal, supervisor, valor, hash_datos 
            FROM cuotas
            """))
            
            existing_records = {}
            for row in result:
                # Convertir fecha a string para formar la clave
                fecha_str = row[1].strftime('%Y-%m-%d') if row[1] else 'None'
                # Usamos una clave compuesta para identificar registros únicos
                key = f"{fecha_str}|{row[2]}|{row[3]}"  # fecha|zonal|supervisor
                existing_records[key] = {'id': row[0], 'valor': row[4], 'hash': row[5]}
            
            print(f"Registros existentes encontrados: {len(existing_records)}")
        
        # Registros a actualizar
        updates = []
        for _, row in df.iterrows():
            fecha = row['fecha'].strftime('%Y-%m-%d') if isinstance(row['fecha'], datetime) else str(row['fecha'])
            key = f"{fecha}|{row['zonal']}|{row['supervisor']}"
            
            if key in existing_records and existing_records[key]['hash'] != row['hash_datos']:
                updates.append({
                    'id': existing_records[key]['id'],
                    'valor': row['valor'],
                    'hash': row['hash_datos']
                })
        
        print(f"Registros que necesitan actualización: {len(updates)}")
        
        # Realizar actualizaciones
        updated_count = 0
        if updates:
            with engine.connect() as conn:
                for update in updates:
                    # PostgreSQL usa $1, $2, etc. para parámetros, pero usaremos formato seguro con text
                    conn.execute(text(f"""
                    UPDATE cuotas 
                    SET valor = :valor, 
                        hash_datos = :hash,
                        fecha_carga = CURRENT_TIMESTAMP
                    WHERE id = :id
                    """).bindparams(
                        valor=update['valor'],
                        hash=update['hash'],
                        id=update['id']
                    ))
                    updated_count += 1
                conn.commit()
        
        if updated_count > 0:
            logging.info(f"Se actualizaron {updated_count} registros existentes")
            print(f"Se actualizaron {updated_count} registros existentes")
        else:
            logging.info("No hay registros para actualizar")
            print("No hay registros existentes que requieran actualización")
            
        return updated_count
    except Exception as e:
        logging.error(f"Error al actualizar registros existentes: {e}")
        print(f"Error al actualizar registros existentes: {e}")
        return 0

def generate_report(engine):
    """Genera un informe sobre los datos de cuotas"""
    try:
        # Estadísticas generales
        stats_query = """
        SELECT 
            COUNT(*) as total_registros,
            MIN(fecha) as fecha_minima,
            MAX(fecha) as fecha_maxima,
            COUNT(DISTINCT zonal) as total_zonales,
            COUNT(DISTINCT supervisor) as total_supervisores,
            SUM(valor) as valor_total
        FROM cuotas
        """
        
        stats = pd.read_sql(stats_query, engine)
        
        # Distribución por zonales
        zonales_query = """
        SELECT 
            zonal,
            COUNT(*) as cantidad,
            SUM(valor) as valor_total,
            AVG(valor) as valor_promedio
        FROM cuotas
        GROUP BY zonal
        ORDER BY valor_total DESC
        """
        
        zonales = pd.read_sql(zonales_query, engine)
        
        # Top supervisores por valor (PostgreSQL usa LIMIT en lugar de TOP)
        supervisores_query = """
        SELECT 
            supervisor,
            zonal,
            SUM(valor) as valor_total
        FROM cuotas
        GROUP BY supervisor, zonal
        ORDER BY valor_total DESC
        LIMIT 10
        """
        
        supervisores = pd.read_sql(supervisores_query, engine)
        
        # Imprimir reporte
        print("\n=== REPORTE DE CUOTAS ===")
        print(f"Total registros: {stats['total_registros'].values[0]}")
        print(f"Período de datos: {stats['fecha_minima'].values[0]} a {stats['fecha_maxima'].values[0]}")
        print(f"Total zonales: {stats['total_zonales'].values[0]}")
        print(f"Total supervisores: {stats['total_supervisores'].values[0]}")
        print(f"Valor total de cuotas: {stats['valor_total'].values[0]}")
        
        print("\nDistribución por zonales:")
        print(zonales)
        
        print("\nTop 10 supervisores por valor total:")
        print(supervisores)
        
        # Guardar reporte en archivo
        with open(r'manual_migrate\carga_cuotas.txt', 'w', encoding='utf-8') as f:
            f.write("=== REPORTE DE CUOTAS ===\n")
            f.write(f"Total registros: {stats['total_registros'].values[0]}\n")
            f.write(f"Período de datos: {stats['fecha_minima'].values[0]} a {stats['fecha_maxima'].values[0]}\n")
            f.write(f"Total zonales: {stats['total_zonales'].values[0]}\n")
            f.write(f"Total supervisores: {stats['total_supervisores'].values[0]}\n")
            f.write(f"Valor total de cuotas: {stats['valor_total'].values[0]}\n\n")
            
            f.write("Distribución por zonales:\n")
            f.write(zonales.to_string(index=False))
            
            f.write("\n\nTop 10 supervisores por valor total:\n")
            f.write(supervisores.to_string(index=False))
        
        logging.info("Reporte de cuotas generado correctamente")
        
    except Exception as e:
        logging.error(f"Error al generar el reporte de cuotas: {e}")
        print(f"Error al generar el reporte: {e}")

def main():
    """Función principal que ejecuta el proceso de carga incremental"""
    try:
        print("=== INICIANDO PROCESO DE CARGA INCREMENTAL DE CUOTAS ===")
        
        # Verificar y establecer conexión
        engine = verify_con(servidor, db, user, password)
        if not engine:
            logging.error("No se pudo establecer conexión con la base de datos. Abortando.")
            return
        
        # Configurar base de datos
        if not setup_database(engine):
            logging.error("No se pudo configurar la base de datos. Abortando.")
            return
        
        # Verificar si el archivo existe
        if not os.path.exists(file_path):
            logging.error(f"El archivo {file_path} no existe.")
            print(f"El archivo {file_path} no existe.")
            return
        
        # Cargar datos del Excel
        df = load_excel_data(file_path, sheet_name)
        if df is None:
            logging.error("No se pudo cargar el archivo Excel. Abortando.")
            return
        
        # Mostrar información del dataframe original
        logging.info(f"Dimensiones originales del Excel: {df.shape}")
        print(f"Registros en el archivo Excel: {len(df)}")
        
        # Preprocesar datos
        df = preprocess_data(df)
        
        # Mostrar información después del preprocesamiento
        logging.info(f"Dimensiones después del preprocesamiento: {df.shape}")
        print(f"Registros después del preprocesamiento: {len(df)}")
        
        # Realizar carga incremental
        new_records = incremental_load(df, engine)
        
        # Actualizar registros existentes si hay cambios
        updated_records = update_existing_records(df, engine)
        
        # Verificar que se hayan realizado las operaciones
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM cuotas"))
            total_records = result.scalar()
            print(f"Total de registros en la tabla después de las operaciones: {total_records}")
        
        # Generar reporte final
        if new_records > 0 or updated_records > 0:
            print(f"\nResumen de operaciones:")
            print(f"- Nuevos registros insertados: {new_records}")
            print(f"- Registros existentes actualizados: {updated_records}")
            print(f"- Total de operaciones: {new_records + updated_records}")
            
            # Generar reporte completo
            generate_report(engine)
        else:
            print("\nNo se realizaron cambios en la base de datos.")
        
        logging.info("Proceso de carga incremental completado exitosamente")
        print("=== PROCESO COMPLETADO EXITOSAMENTE ===")
        
    except Exception as e:
        logging.error(f"Error en el proceso de carga incremental: {e}")
        print(f"Error en el proceso: {e}")
        import traceback
        print(traceback.format_exc())

if __name__ == "__main__":
    main()