import pandas as pd
import os
from datetime import datetime
import logging
from sqlalchemy import create_engine, text, inspect
import sys

# Configuración de logging con UTF-8 y DEBUG
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
log_filename = f'usuarios_load_postgres_{timestamp}.log'
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
db_params = {
    'host': 'localhost',
    'port': '5432',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'localpassword'
}
#file_path = r"C:\Users\jimmy.atao\Downloads\usuario.xlsx"
file_path = r"C:\Users\oak\Downloads\usuario.xlsx"
# Fecha de procesamiento actual
date_today = datetime.now()
FECHA_PROCESO = date_today.date()  # tipo DATE

# Nombre de la tabla destino
TABLE_NAME = 'usuarios'


def verify_con(params):
    """Verifica la conexión y retorna engine."""
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


def setup_database(engine, df_columns):
    """
    Crea o ajusta la tabla dinámica según las columnas del DataFrame,
    incluyendo fecha_proceso como clave.
    """
    # Columnas dinámicas según el Excel
    cols_ddl = [f'"{col}" TEXT' for col in df_columns]
    # Añadir fecha_proceso
    cols_ddl.append('"fecha_proceso" DATE NOT NULL')

    # Definir PRIMARY KEY sobre dni, zonal, telefono, fecha_proceso
    pk_fields = []
    for key in ['dni', 'zonal', 'telefono']:
        for col in df_columns:
            if col.lower() == key:
                pk_fields.append(f'"{col}"')
                break
    pk_fields.append('"fecha_proceso"')
    pk_ddl = f'PRIMARY KEY ({", ".join(pk_fields)})'

    ddl = (
        f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} (\n  "
        + ",\n  ".join(cols_ddl + [pk_ddl])
        + "\n);"
    )
    with engine.begin() as conn:
        conn.execute(text(ddl))
    logging.info(f"Tabla {TABLE_NAME} creada o ajustada con fecha_proceso como clave.")


def load_full_reload(df, engine):
    """
    Full reload: borra registros de la fecha actual e inserta datos crudos.

    - Rellena nulos en DNI, Zonal y Telefono (parte de PK).
    - Agrega columna fecha_proceso (type DATE).
    """
    # Mantener casing y quitar espacios
    df.columns = [col.strip() for col in df.columns]

    # Rellenar nulos en columnas de clave primaria
    for key in ['dni', 'zonal', 'telefono']:
        for col in df.columns:
            if col.lower() == key:
                df[col] = df[col].fillna('DESCONOCIDO')
                break

    # Agregar fecha_proceso
    df['fecha_proceso'] = FECHA_PROCESO

    # Obtener columnas de la tabla
    insp = inspect(engine)
    cols_meta = [col['name'] for col in insp.get_columns(TABLE_NAME)]
    insert_cols = [col for col in df.columns if col in cols_meta]
    df_ins = df[insert_cols]

    # Eliminar snapshot previo
    with engine.begin() as conn:
        conn.execute(
            text(f'DELETE FROM {TABLE_NAME} WHERE "fecha_proceso" = :fp'),
            {'fp': FECHA_PROCESO}
        )
    logging.info(f"Eliminados registros de fecha {FECHA_PROCESO} en {TABLE_NAME}")

    # Bulk insert
    try:
        df_ins.to_sql(TABLE_NAME, engine, if_exists='append', index=False)
        logging.info(f"Insertadas {len(df_ins)} filas en {TABLE_NAME} para {FECHA_PROCESO}")
    except Exception as e:
        logging.error(f"Error al insertar registros: {e}")
        print(f"Error al insertar: {e}")


def main():
    logging.info(f"Inicio full reload para fecha: {FECHA_PROCESO}")
    engine = verify_con(db_params)
    if not engine:
        return

    if not os.path.exists(file_path):
        logging.error(f"Archivo no existe: {file_path}")
        return

    try:
        df = pd.read_excel(file_path)
        logging.info(f"DataFrame cargado: {len(df)} filas.")
    except Exception as e:
        logging.error(f"Error cargando Excel: {e}")
        return

    setup_database(engine, list(df.columns))
    load_full_reload(df, engine)
    logging.info("Proceso completado.")


if __name__ == '__main__':
    main()


"""registros de usuarios.xlsx 
muestra de registros en 
| Usuario   | Empresa | Nombre                             | DNI        | Rol                     | Poblacion | Zonal   | Email                             | Telefono  | Region       | Genero | Superior                                | Direccion                                      | Pais | CodigoPostal | ZonaHoraria                          | FechaIngreso | FechaCese | Estado    |
|-----------|---------|------------------------------------|------------|-------------------------|-----------|---------|-----------------------------------|-----------|--------------|--------|-----------------------------------------|------------------------------------------------|------|--------------|--------------------------------------|--------------|-----------|-----------|
| 29398271  |         | ACHAHUANCO MERMA YOLANDA          | 29398271   | VENDEDOR - PLANILLA     |           | AREQUIPA| yolanda.achahunco@gmail.com      | 959670343 | REGION SUR   | Mujer  | ZEBALLOS ESCOBAR KAREN YULEISY       | San Martín 1817 Chapi Chico                    | Perú |              | (UTC-05:00) Bogota, Lima, Quito    | 01/03/2024   |           | En campo  |
| 44137762  |         | ACOSTA HERRERA MILAGROS DE MARIA  | 44137762   | VENDEDOR - COMISIONISTA  |           | CHIMBOTE| milagrosacostaherrera@gmail.com   | 944316201 | REGION CENTRO| Mujer  | GONZALES GUTIERREZ ALEXANDER ISMAEL  | UB Nicolas garatea mz 118 LT 21                | Perú |              | (UTC-05:00) Bogota, Lima, Quito    | 12/03/2025   |           | En campo  |
| 003495147 |         | ACUÑA SANCHEZ JACOB               | 003495147  | VENDEDOR - COMISIONISTA  | Arequipa  | AREQUIPA| vcordero.auren@gmail.com          | 926642555 | REGION SUR   | Hombre | ZARRAGA HENRIQUEZ SIMON ENRIQUE      |                                                | Perú |              | (UTC-05:00) Bogota, Lima, Quito    |              |           | En campo  |
| 71583936  |         | ACUÑA VEGA FLOR MARIA             | 71583936   | VENDEDOR - PLANILLA     | Áncash    | HUARAZ  | estefaniam10.16@gmail.com         | 956440655 | REGION CENTRO| Mujer  | GIRALDO LOPEZ RIVALDO ANDRES         | Jr. Esteban Castromonte -S/N- Barrio Pedregal Medio | Perú | **           | (UTC-05:00) Bogota, Lima, Quito    | 13/03/2025   |           | En campo  |
| 43863338  |         | ACUÑA VEGA RONALD ELIOT           | 43863338   | VENDEDOR - COMISIONISTA  |           | HUARAZ  | eronald2020@gmail.com             | 958123625 | REGION CENTRO| Hombre | GIRALDO LOPEZ RIVALDO ANDRES         | Jr. Esteban Castromonte S/N Barr. Pedregal Medio | Perú |              | (UTC-05:00) Bogota, Lima, Quito    | 18/03/2025   |           | En campo  |
| 43002051  |         | ADCO GÓMEZ DUDLEY DANIEL          | 43002051   | BACK OFFICE             | Arequipa  | AREQUIPA| adudley.auren@gmail.com           | 978589177 | REGION SUR   | Hombre | AREVALO LAIMITO CARLOS ALBERTO      |                                                | Perú |              | (UTC-05:00) Bogota, Lima, Quito    |              |           | Sin especificar |
| 75114101  |         | AGUILAR LAURA RUTH ESTHER         | 75114101   | VENDEDOR - PLANILLA     |           | AREQUIPA| ruthesteraguilarlaura123@gmail.com| 918077535 | REGION SUR   | Hombre | ALVAREZ VELARDE GABRIEL ANDRE       | 3 DE OCTUBRE MZ D LT 1                        | Perú |              | (UTC-05:00) Bogota, Lima, Quito    |              |           | En campo  |
| 70133406  |         | AGUILAR LEON MARYORI ELIZABETH    | 70133406   | VENDEDOR - COMISIONISTA  |           | CHIMBOTE|                                   | 936494671 | REGION CENTRO| Mujer  | RAMOS JARA FAUSTO ADALBERTO         |                                                | Perú |              | (UTC-05:00) Bogota, Lima, Quito    |              |           | En campo  |
| 70600373  |         | AGUILAR SALAS GUILLERMO DANIEL    | 70600373   | VENDEDOR - PLANILLA     |           | TACNA   | raquel.torres@grupoauren.pe      | 926216788 | REGION SUR   | Hombre | TORRES ARIAS RAQUEL NATALIA         | Conjunto Habitacional Habitat II Nro k-02 Tacna | Perú |              | (UTC-05:00) Bogota, Lima, Quito    |              |           | En campo  |
| 47926807  |         | ALBERTO HENOSTROZA ISAMAR JENIFFER| 47926807   | VENDEDOR - COMISIONISTA  |           | HUARAZ  | jenifferalbertohernostroza@gmail.com | 900903304 | REGION CENTRO| Mujer  | ALBERTO HENOSTROZA LILIANA ARACELLI | Ca. Caserio Chequio S/N Cas. Chequio          | Perú |              | (UTC-05:00) Bogota, Lima, Quito    |              |           | En campo  |
| 76649551  |         | ALBERTO HENOSTROZA LILIANA ARACELLI| 76649551  | SUPERVISOR              | Áncash    | HUARAZ  |                                   | 975893933 | REGION CENTRO| Mujer  | ANGELITA LETICIA ESQUIVEL           |                                                | Perú |              | (UTC-05:00) Bogota, Lima, Quito    |              |           | Sin especificar |
| 40504105  |         | ALBIÑO ALAMO LAURA YENIFER       | 40504105   | VENDEDOR - COMISIONISTA  |           | CHIMBOTE| sh4n3c1t4@gmail.com               | 977301497 | REGION CENTRO| Mujer  | CHAVEZ VIZALOTE MARIA DEL PILAR     | Bellamar 2etpa MZ N5 lt 9 Nuevo Chimbote      | Perú |              | (UTC-05:00) Bogota, Lima, Quito    |              |           | En campo  |
| 61833676  |         | ALDAIR FELIX TOLEDO ALARCON       | 61833676   | BACK OFFICE             |           | LIMA ESTE| atoledo.auren@gmail.com           | 937735468 | LIMA         | Hombre |                                         | CALLE SANTA TERESA 179 URB LOS SAUCES - ATE - LIMA | Perú |              | (UTC-05:00) Bogota, Lima, Quito    |              |           | Sin especificar |
| 40157205  |         | ALVAREZ MARTELL JAQUELINE MARIBEL | 40157205   | VENDEDOR - COMISIONISTA  |           | TRUJILLO| jaquelinemaribel2@gmail.com       | 975015881 | REGION NORTE | Mujer  | ROMERO PANDURO JUAN DE DIOS         | Los Pinos Mz P lt 2- AA.HH Víctor Raúl        | Perú |              | (UTC-05:00) Bogota, Lima, Quito    |              |           | En campo  |
| 74162049  |         | ALVAREZ RAMOS CLAUDIA ALESSANDRA  | 74162049   | VENDEDOR - PLANILLA     |           | TACNA   | pollonet1977@gmail.com            | 929668363 | REGION SUR   | Mujer  | TORRES ARIAS RAQUEL NATALIA         | Promuvi Viñani MZ 337 LT 14                    | Perú |              | (UTC-05:00) Bogota, Lima, Quito    |              |           | En campo  |

"""

"""errores


"""