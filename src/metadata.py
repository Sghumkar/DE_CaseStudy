
import os
from datetime import datetime

def add_metadata(df, file_path):
    """Add metadata columns (Processed At, Source File) to valid rows."""
    df['Processed At'] = datetime.now()
    df['Source File'] = os.path.basename(file_path)
    return df
