# src/aggregation.py
import pandas as pd
from src.logger import get_logger
from src.metadata import add_metadata
from datetime import datetime
logger = get_logger(__name__)

def calculate_aggregated_metrics(df,file_path):

    logger.info(f"DataFrame dtypes:\n\n{df}")
    
    metrics_with_metadata=add_metadata(df, file_path)
    
    metrics = metrics_with_metadata.groupby(['Source File', 'Station Name']).agg(
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
    
    metrics['Processed At'] = datetime.now()
    metrics.rename(columns={'Station Name': 'Data Source'}, inplace=True)

    
    return metrics
