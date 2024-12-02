import os

# Base directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Ensure absolute path for the data directory
DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, '../data'))
LOG_DIR = os.path.abspath(os.path.join(BASE_DIR, '../logs'))
QUARANTINE_DIR = os.path.abspath(os.path.join(BASE_DIR, '../quarantine'))

# Validation rules
VALIDATION_RULES = {
    "Air Temperature": (-50, 50),  # In degrees Celsius
    "Wet Bulb Temperature": (-50, 50),
    "Humidity": (0, 100),  # Percentage
    #"Wind Speed": (0, 150),  # Max reasonable wind speed
    #"Barometric Pressure": (850, 1080),  # hPa
}

CRITICAL_COLUMNS = ['Station Name', 'Measurement Timestamp'] + list(VALIDATION_RULES.keys())
