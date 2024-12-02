# src/aggregation.py
import pandas as pd

def calculate_aggregated_metrics(df):
    """Calculate aggregated metrics for each sensor type."""
    metrics = df.groupby('Source File').agg(
        min_temp=('Air Temperature', 'min'),
        max_temp=('Air Temperature', 'max'),
        avg_temp=('Air Temperature', 'mean'),
        std_temp=('Air Temperature', 'std'),
        min_humidity=('Humidity', 'min'),
        max_humidity=('Humidity', 'max'),
        avg_humidity=('Humidity', 'mean'),
        std_humidity=('Humidity', 'std'),
    ).reset_index()
    return metrics
