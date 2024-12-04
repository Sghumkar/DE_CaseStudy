import os
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), '..', 'db', '.env')
load_dotenv(dotenv_path)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, '../data'))
LOG_DIR = os.path.abspath(os.path.join(BASE_DIR, '../logs'))
QUARANTINE_DIR = os.path.abspath(os.path.join(BASE_DIR, '../quarantine'))

TABLES = {
    'raw_sensor_data': 'raw_sensor_data',  
    'aggregated_metrics': 'aggregated_metrics'  
    
    }
VALIDATION_RULES = {
    "Air Temperature": (-50, 50),  # In degrees Celsius
    "Barometric Pressure": (850, 1080),
    "Humidity": (0, 100),  # Percentage
}

CRITICAL_COLUMNS = ['Station Name', 'Measurement Timestamp','Measurement ID'] + list(VALIDATION_RULES.keys())
DB_CONFIG = {
    'database': os.getenv("DB_NAME"),
    'schema':os.getenv("DB_SCHEMA"),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT', 5432))  ,
    
    'min_connections':1,
    'max_connections':10

}

DEBUG = os.getenv('DEBUG', 'false').lower() == 'true'

