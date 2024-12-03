
import os

def add_metadata(df, file_path):
    df['Source File'] = os.path.basename(file_path)
    return df
