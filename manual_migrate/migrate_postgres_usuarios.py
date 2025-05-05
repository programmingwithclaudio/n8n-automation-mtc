import pandas as pd
import os
import sys
import yaml
import logging
import hashlib
from datetime import datetime
import traceback
import re
import unicodedata
from sqlalchemy import create_engine, text, inspect
import phonenumbers


def setup_logging(config):
    """Configura el sistema de logging según el archivo de configuración."""
    log_file = config.get('log_file', f"carga_usuarios_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    
    # Asegurar que el directorio del log existe
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Configuración de logging con UTF-8
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filename=log_file,
        filemode='w',
        encoding='utf-8'
    )
    
    # Agregar handler para consola
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console.setFormatter(formatter)
    logging.getLogger().addHandler(console)
    
    logging.info(f"Logging configurado. Archivo de log: {log_file}")


def load_config(config_path='manual_migrate/config_user.yml'):
    """Carga la configuración desde un archivo YAML."""
    try:
        with open(config_path, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)
        logging.info(f"Configuración cargada desde {config_path}")
        return config
    except Exception as e:
        logging.error(f"Error cargando configuración: {e}")
        print(f"Error cargando configuración: {e}")
        return None


def verify_connection(db_config):
    """Verifica la conexión a la base de datos y retorna engine."""
    conn_str = (
        f"postgresql://{db_config['user']}:{db_config['password']}@"
        f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
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


def setup_database(engine, config, df_columns):
    """
    Crea o ajusta la tabla para usuarios con soporte para carga incremental.
    """
    table_name = config.get('tabla_destino', 'usuarios')
    field_mapping = config.get('field_mapping', {})
    
    # Mapear columnas del Excel a columnas de base de datos
    db_columns = []
    for col in df_columns:
        if col in field_mapping:
            db_columns.append(field_mapping[col])
        else:
            # Si no hay mapeo, usar el nombre original en minúsculas
            db_columns.append(col.lower())
    
    # Columnas dinámicas según el Excel y mapping
    cols_ddl = [f'"{col}" TEXT' for col in db_columns]
    
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
    
    # Obtener los campos para la clave primaria
    primary_keys = []
    for key in [config.get('clave_unica'), config.get('clave_secundaria'), 
                config.get('clave_terciaria'), config.get('clave_cuarta')]:
        if key and key in field_mapping.values():
            primary_keys.append(f'"{key}"')
    
    # Agregar fecha_proceso a la clave primaria
    primary_keys.append('"fecha_proceso"')
    
    pk_ddl = f'PRIMARY KEY ({", ".join(primary_keys)})'
    
    # Crear la tabla si no existe
    ddl = (
        f"CREATE TABLE IF NOT EXISTS {table_name} (\n  "
        + ",\n  ".join(all_cols_ddl + [pk_ddl])
        + "\n);"
    )
    
    try:
        with engine.begin() as conn:
            conn.execute(text(ddl))
            
            # Crear índice para hash_id
            conn.execute(text(f'''
                CREATE INDEX IF NOT EXISTS idx_{table_name}_hash_id
                ON {table_name} (hash_id);
            '''))
            
        logging.info(f"Tabla {table_name} configurada para soporte incremental")
        return True
    except Exception as e:
        logging.error(f"Error configurando la base de datos: {e}")
        logging.error(traceback.format_exc())
        return False


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


def normalize_document(value, min_length=8, default="DESCONOCIDO"):
    """Normaliza documentos de identidad."""
    if pd.isna(value) or str(value).strip() == '':
        return default
    
    # Convertir a string y eliminar espacios
    doc = str(value).strip()
    
    # Eliminar caracteres no numéricos
    doc = re.sub(r'[^0-9]', '', doc)
    
    # Verificar longitud mínima
    if len(doc) < min_length:
        logging.warning(f"Documento '{value}' normalizado a '{default}' por longitud insuficiente")
        return default
    
    return doc


def normalize_email(value, validate=True):
    """Normaliza y opcionalmente valida emails."""
    if pd.isna(value) or str(value).strip() == '':
        return None
    
    # Convertir a minúsculas y eliminar espacios
    email = str(value).strip().lower()
    
    # Validación básica de email
    if validate:
        email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_regex, email):
            logging.warning(f"Email inválido: '{value}'")
            return None
    
    return email


def normalize_phone(value, country='PE', validate=True):
    """Normaliza y opcionalmente valida números telefónicos."""
    if pd.isna(value) or str(value).strip() == '':
        return None
    
    # Convertir a string y eliminar espacios
    phone = str(value).strip()
    
    # Eliminar caracteres no numéricos
    phone = re.sub(r'[^0-9+]', '', phone)
    
    # Validación con phonenumbers si se solicita
    if validate:
        try:
            # Si no empieza con +, asumir código de país
            if not phone.startswith('+'):
                if country == 'PE':  # Perú
                    # Si empieza con 9 y tiene 9 dígitos, agregar +51
                    if len(phone) == 9 and phone.startswith('9'):
                        phone = '+51' + phone
                    # Si tiene 8 dígitos (fijo), agregar +51
                    elif len(phone) == 8:
                        phone = '+51' + phone
            
            # Parsear y formatear
            parsed_phone = phonenumbers.parse(phone, country)
            if not phonenumbers.is_valid_number(parsed_phone):
                logging.warning(f"Teléfono inválido: '{value}'")
                return phone  # Devolver el original limpio en caso de error
            
            # Formatear en formato internacional
            formatted_phone = phonenumbers.format_number(
                parsed_phone, phonenumbers.PhoneNumberFormat.E164)
            return formatted_phone
            
        except Exception as e:
            logging.warning(f"Error validando teléfono '{value}': {e}")
            return phone  # Devolver el original limpio en caso de error
    
    return phone


def normalize_date(value, format='%d/%m/%Y'):
    """Normaliza fechas al formato estándar en base de datos."""
    if pd.isna(value) or str(value).strip() == '':
        return None
    
    try:
        # Si es string, convertir a datetime
        if isinstance(value, str):
            date_obj = datetime.strptime(value.strip(), format)
        elif isinstance(value, datetime):
            date_obj = value
        else:
            logging.warning(f"Formato de fecha desconocido: {type(value)}")
            return None
        
        # Formatear para PostgreSQL (ISO)
        return date_obj.strftime('%Y-%m-%d')
    
    except Exception as e:
        logging.warning(f"Error normalizando fecha '{value}': {e}")
        return None


def normalize_text(value, case=None, remove_accents=False):
    """Normaliza texto según las reglas especificadas."""
    if pd.isna(value) or str(value).strip() == '':
        return None
    
    text = str(value).strip()
    
    # Eliminar acentos si se solicita
    if remove_accents:
        text = unicodedata.normalize('NFKD', text)
        text = ''.join([c for c in text if not unicodedata.combining(c)])
    
    # Aplicar transformación de caso
    if case == 'upper':
        text = text.upper()
    elif case == 'lower':
        text = text.lower()
    elif case == 'title':
        text = text.title()
    
    return text


def apply_normalization_rules(df, config):
    """Aplica reglas de normalización a cada columna según configuración."""
    field_mapping = config.get('field_mapping', {})
    normalization = config.get('normalization', {})
    
    # Mapear columnas del Excel a columnas de DB
    excel_to_db = {}
    for excel_col, db_col in field_mapping.items():
        excel_to_db[excel_col] = db_col
    
    # Aplicar reglas de normalización
    for excel_col in df.columns:
        if excel_col in excel_to_db:
            db_col = excel_to_db[excel_col]
            
            # Si hay reglas de normalización para esta columna
            if db_col in normalization:
                rules = normalization[db_col]
                field_type = rules.get('type', 'text')
                
                # Aplicar la normalización según el tipo
                if field_type == 'document':
                    min_length = rules.get('min_length', 8)
                    default = rules.get('default', 'DESCONOCIDO')
                    df[excel_col] = df[excel_col].apply(
                        lambda x: normalize_document(x, min_length, default)
                    )
                
                elif field_type == 'email':
                    validation = rules.get('validation', True)
                    df[excel_col] = df[excel_col].apply(
                        lambda x: normalize_email(x, validation)
                    )
                
                elif field_type == 'phone':
                    country = rules.get('country', 'PE')
                    validation = rules.get('validation', True)
                    df[excel_col] = df[excel_col].apply(
                        lambda x: normalize_phone(x, country, validation)
                    )
                
                elif field_type == 'date':
                    date_format = rules.get('format', '%d/%m/%Y')
                    df[excel_col] = df[excel_col].apply(
                        lambda x: normalize_date(x, date_format)
                    )
                
                elif field_type == 'text':
                    case = rules.get('case', None)
                    remove_accents = rules.get('remove_accents', False)
                    df[excel_col] = df[excel_col].apply(
                        lambda x: normalize_text(x, case, remove_accents)
                    )
    
    logging.info("Reglas de normalización aplicadas al DataFrame")
    return df


def preprocess_data(df, config):
    """
    Preprocesa los datos para carga incremental:
    - Aplica reglas de normalización
    - Rellena nulos en campos de PK
    - Agrega fecha_proceso
    - Genera hash_id para cada registro
    """
    # Obtener campos de clave primaria
    key_fields = []
    for key in [config.get('clave_unica'), config.get('clave_secundaria'), 
                config.get('clave_terciaria'), config.get('clave_cuarta')]:
        if key:
            # Buscar la columna Excel que mapea a este campo DB
            for excel_col, db_col in config.get('field_mapping', {}).items():
                if db_col == key and excel_col in df.columns:
                    key_fields.append(excel_col)
                    break
    
    # Rellenar nulos en columnas de clave primaria
    for key_field in key_fields:
        df[key_field] = df[key_field].fillna('DESCONOCIDO')
    
    # Fecha de proceso (mantenemos DATE para compatibilidad)
    fecha_proceso = datetime.now().date()
    df['fecha_proceso'] = fecha_proceso
    
    # Generar hash_id basado en campos clave más fecha_proceso
    all_key_fields = key_fields + ['fecha_proceso']
    df['hash_id'] = df.apply(lambda row: create_hash_id(row, all_key_fields), axis=1)
    
    # Añadir timestamps y estado inicial
    now = datetime.now()
    df['fecha_carga'] = now
    df['fecha_actualizacion'] = now
    df['estado_carga'] = 'nuevo'
    
    logging.info(f"Preprocesamiento completado. {len(df)} registros preparados.")
    return df, key_fields


def get_existing_records(engine, config, fecha_proceso):
    """
    Obtiene registros existentes para la fecha de proceso actual.
    """
    table_name = config.get('tabla_destino', 'usuarios')
    
    query = text(f"""
    SELECT * FROM {table_name}
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


def prepare_for_db_insert(df, config):
    """
    Prepara el DataFrame para inserción en DB, aplicando el mapeo de columnas.
    """
    field_mapping = config.get('field_mapping', {})
    
    # Crear un nuevo DataFrame con las columnas mapeadas
    db_df = pd.DataFrame()
    
    # Agregar columnas de control que ya existen en df
    control_cols = ['fecha_proceso', 'fecha_carga', 'fecha_actualizacion', 
                   'hash_id', 'estado_carga']
    for col in control_cols:
        if col in df.columns:
            db_df[col] = df[col]
    
    # Mapear columnas del Excel a columnas de DB
    for excel_col, db_col in field_mapping.items():
        if excel_col in df.columns:
            db_df[db_col] = df[excel_col]
    
    return db_df


def incremental_load_and_update(df, engine, config, key_fields):
    """
    Realiza la carga incremental de usuarios:
    - Identifica registros nuevos vs existentes usando hash_id
    - Inserta nuevos registros
    - Actualiza registros existentes si hay cambios
    """
    try:
        table_name = config.get('tabla_destino', 'usuarios')
        field_mapping = config.get('field_mapping', {})
        
        # Mapear key_fields (nombres de Excel) a db_key_fields (nombres en DB)
        db_key_fields = []
        for key in key_fields:
            for excel_col, db_col in field_mapping.items():
                if excel_col == key:
                    db_key_fields.append(db_col)
                    break
        
        # Agregar fecha_proceso a las claves
        db_key_fields.append('fecha_proceso')
        
        fecha_proceso = df['fecha_proceso'].iloc[0]
        existing_records = get_existing_records(engine, config, fecha_proceso)
        
        # Preparar DataFrame para la base de datos
        db_df = prepare_for_db_insert(df, config)
        
        if existing_records.empty:
            # No hay registros existentes para esta fecha, insertar todos
            logging.info(f"No hay registros existentes para {fecha_proceso}. Insertando todos como nuevos.")
            db_df.to_sql(table_name, engine, if_exists='append', index=False, chunksize=500)
            return len(db_df), 0
        
        # Crear hashmap de registros existentes
        existing_hash_set = set(existing_records['hash_id'].tolist())
        
        # Separar registros nuevos y existentes
        db_df['is_new'] = ~db_df['hash_id'].isin(existing_hash_set)
        new_records = db_df[db_df['is_new']].drop(columns=['is_new'])
        existing_updates = db_df[~db_df['is_new']].drop(columns=['is_new'])
        
        # Insertar nuevos registros
        inserted_count = 0
        if not new_records.empty:
            new_records.to_sql(table_name, engine, if_exists='append', index=False, chunksize=500)
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
                    for key in db_key_fields:
                        if key in update_row:
                            conditions.append(f'"{key}" = :{key}')
                            params[key] = update_row[key]
                    
                    # Campos a actualizar
                    update_fields = []
                    for col in update_row.index:
                        if col not in db_key_fields and col not in ['hash_id', 'fecha_carga']:
                            update_fields.append(f'"{col}" = :{col}')
                            params[col] = update_row[col]
                    
                    # Construir y ejecutar la consulta UPDATE
                    if update_fields and conditions:
                        update_sql = f"""
                        UPDATE {table_name}
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


def get_data_summary(engine, config, fecha_proceso):
    """Obtiene un resumen de los datos por estado de carga."""
    table_name = config.get('tabla_destino', 'usuarios')
    
    try:
        with engine.connect() as conn:
            # Total de registros para la fecha
            total_query = text(f"""
                SELECT COUNT(*)
                FROM {table_name}
                WHERE "fecha_proceso" = :fecha
            """)
            total_result = conn.execute(total_query, {"fecha": fecha_proceso}).scalar()
            
            # Registros por estado
            estados_query = text(f"""
                SELECT "estado_carga", COUNT(*)
                FROM {table_name}
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
    
    try:
        # Cargar configuración
        config = load_config()
        if not config:
            print("No se pudo cargar la configuración. Proceso abortado.")
            return
        
        # Configurar logging
        setup_logging(config)
        logging.info("Iniciando proceso de carga incremental con configuración YAML")
        
        # Verificar conexión
        engine = verify_connection(config['db'])
        if not engine:
            logging.error("No se pudo establecer conexión. Proceso abortado.")
            return
        
        # Verificar archivo
        excel_path = config['excel']['path']
        if not os.path.exists(excel_path):
            logging.error(f"Archivo no existe: {excel_path}")
            return
        
        # Cargar datos
        df = load_excel_data(excel_path)
        if df is None or df.empty:
            logging.error("No se pudieron cargar datos del Excel. Proceso abortado.")
            return
        
        # Normalizar datos según reglas
        df = apply_normalization_rules(df, config)
        
        # Configurar base de datos
        if not setup_database(engine, config, list(df.columns)):
            logging.error("Error configurando la base de datos. Proceso abortado.")
            return
        
        # Preprocesar datos
        df, key_fields = preprocess_data(df, config)
        
        # Mostrar muestra de datos
        print("Muestra de datos a procesar (después de normalización):")
        print(df.head())
        
        # Procesar carga incremental
        fecha_proceso = df['fecha_proceso'].iloc[0]
        inserted, updated = incremental_load_and_update(df, engine, config, key_fields)
        
        # Obtener resumen
        summary = get_data_summary(engine, config, fecha_proceso)
        
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
