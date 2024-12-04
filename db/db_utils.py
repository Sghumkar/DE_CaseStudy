from psycopg2 import pool
from config.settings import DB_CONFIG, TABLES
from src.logger import get_logger

logger = get_logger(__name__)

schema_name = DB_CONFIG['schema']
raw_sensor_table = f"{schema_name}.{TABLES['raw_sensor_data']}"
aggregated_metrics_table = f"{schema_name}.{TABLES['aggregated_metrics']}"

connection_pool = pool.SimpleConnectionPool(
    DB_CONFIG['min_connections'],  
    DB_CONFIG['max_connections'],  
    host=DB_CONFIG['host'],
    database=DB_CONFIG['database'],
    user=DB_CONFIG['user'],
    password=DB_CONFIG['password']
)

def get_db_connection():
    return connection_pool.getconn()

def release_db_connection(conn):
    connection_pool.putconn(conn)

def close_connection_pool():
    connection_pool.closeall()

def create_schema_if_not_exist(conn):
    with conn.cursor() as cursor:
        cursor.execute(f"""CREATE SCHEMA IF NOT EXISTS {schema_name};""")
        

def table_exists(conn, table_name):
    with conn.cursor() as cursor:
        cursor.execute("""SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            );
        """, (schema_name, table_name))
        return cursor.fetchone()[0]

def index_exists(conn, table_name, index_name):
    
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM pg_indexes
                WHERE schemaname = %s AND tablename = %s AND indexname = %s
            );
        """, (schema_name, table_name, index_name))
        return cursor.fetchone()[0]

def create_tables_if_not_exist(conn):

    create_schema_if_not_exist(conn)

    with conn.cursor() as cursor:
        if not table_exists(conn, TABLES['raw_sensor_data']):
            cursor.execute(f"""
            CREATE TABLE {raw_sensor_table} (
                station_name TEXT NOT NULL,
                measurement_timestamp TIMESTAMP NOT NULL,
                measurement_id TEXT NOT NULL,
                air_temperature REAL,
                humidity INT,
                barometric_pressure REAL,
                processed_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (measurement_id)
            );
            """)
            logger.info(f"Table '{raw_sensor_table}' created.")
        else:
            logger.info(f"Table '{raw_sensor_table}' already exists.")

        if not index_exists(conn, TABLES['raw_sensor_data'], 'idx_raw_sensor_station_time'):
            cursor.execute(f"""
            CREATE INDEX idx_raw_sensor_station_time
            ON {raw_sensor_table} (station_name, measurement_timestamp);
            """)
            logger.info(f"Index 'idx_raw_sensor_station_time' created for '{raw_sensor_table}'.")
        else:
            logger.info(f"Index 'idx_raw_sensor_station_time' already exists for '{raw_sensor_table}'")

        if not table_exists(conn, TABLES['aggregated_metrics']):
            cursor.execute(f"""
            CREATE TABLE {aggregated_metrics_table} (
                source_file TEXT NOT NULL,
                station_name TEXT NOT NULL,
                min_temp REAL,
                max_temp REAL,
                avg_temp REAL,
                std_temp REAL,
                min_humidity INT,
                max_humidity INT,
                avg_humidity REAL,
                std_humidity REAL,
                min_pressure REAL,
                max_pressure REAL,
                avg_pressure REAL,
                std_pressure REAL,
                processed_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (source_file, station_name)
            );
            """)
            logger.info(f"Table '{aggregated_metrics_table}' created.")
        else:
            logger.info(f"Table '{aggregated_metrics_table}' already exists.")

        # Create index for aggregated_metrics table
        if not index_exists(conn, TABLES['aggregated_metrics'], 'idx_aggregated_metrics_station_name'):
            cursor.execute(f"""
            CREATE INDEX idx_aggregated_metrics_station_name
            ON {aggregated_metrics_table} (station_name, source_file);
            """)
            logger.info(f"Index 'idx_aggregated_metrics_station_name' created for '{aggregated_metrics_table}'.")
        else:
            logger.info(f"Index 'idx_aggregated_metrics_station_name' already exists for '{aggregated_metrics_table}'")

        conn.commit()

def insert_raw_data(conn, cursor,raw_data):
    raw_data_query = f"""
            INSERT INTO {raw_sensor_table} (
                station_name,
                measurement_id,
                measurement_timestamp,
                air_temperature,
                humidity,
                barometric_pressure,
                processed_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (measurement_id) DO NOTHING;
        """
    cursor.executemany(raw_data_query, raw_data)
    conn.commit()
    logger.info(f"{len(raw_data)} rows inserted into {raw_sensor_table}.")

def insert_aggregated_data(conn, cursor,aggregated_metrics):
    aggregated_data_query = f"""
            INSERT INTO {aggregated_metrics_table} (
                source_file,
                station_name,
                min_temp,
                max_temp,
                avg_temp,
                std_temp,
                min_humidity,
                max_humidity,
                avg_humidity,
                std_humidity,
                min_pressure,
                max_pressure,
                avg_pressure,
                std_pressure,
                processed_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (source_file, station_name) DO NOTHING;
        """
    cursor.executemany(aggregated_data_query, aggregated_metrics)

    conn.commit()

    logger.info(f"{len(aggregated_metrics)} rows inserted into {aggregated_metrics_table}.")