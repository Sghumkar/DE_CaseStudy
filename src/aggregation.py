import os
from src.logger import get_logger

logger = get_logger(__name__)

def calculate_aggregated_metrics(df,file_path):


    df['Source File'] = os.path.basename(file_path)
    
    metrics = df.groupby(['Source File', 'Station Name']).agg(
        min_temp=('Air Temperature', 'min'),
        max_temp=('Air Temperature', 'max'),
        avg_temp=('Air Temperature', 'mean'),
        std_temp=('Air Temperature', 'std'),
        min_humidity=('Humidity', 'min'),
        max_humidity=('Humidity', 'max'),
        avg_humidity=('Humidity', 'mean'),
        std_humidity=('Humidity', 'std'),
        min_pressure=('Barometric Pressure', 'min'),
        max_pressure=('Barometric Pressure', 'max'),
        avg_pressure=('Barometric Pressure', 'mean'),
        std_pressure=('Barometric Pressure', 'std')
    ).reset_index()
    
    logger.info(f'Aggregation completed for file: {file_path}')
    
    
    return metrics
