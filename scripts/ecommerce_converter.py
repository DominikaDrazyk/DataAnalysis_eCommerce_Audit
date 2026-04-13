# =================================================================================================
# ETL - Olist E-Commerce Data Pre-processing
# =================================================================================================
# PRE-STAGING: Unicode Normalization & Encoding Correction
# PURPOSE: Sanitizes raw Olist CSV files prior to Database Ingestion (ETL)
#          Ensures global compatibility and prevents character corruption
#   - Detects and fixes encoding issues (UTF-8, Latin1, CP1252 fallbacks)
#   - Transliterates special characters (accents, cedillas) to ASCII using unidecode
#   - Automates batch processing of all source files in the /data directory
#   - Outputs standardized UTF-8 CSVs ready for PostgreSQL COPY commands
# AUTHOR: Dominika A. Drazyk
# DEPENDENCIES: pandas, unidecode, pathlib
# =================================================================================================

import pandas as pd
import unidecode
import os
from pathlib import Path

# Target directory containing the raw Olist dataset
DATA_TARGET = os.path.join(os.getcwd(), 'data')

def convert_csv(input_file, output_file):
    """
    Performs encoding detection and text normalization on a single file.
    
    Args:
        input_file (str): Path to the raw source CSV.
        output_file (str): Path to save the sanitized UTF-8 CSV.
    """
    # Fallback mechanism to handle common encoding variations in the Olist dataset
    encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']
    df = None
    
    for enc in encodings:
        try:
            df = pd.read_csv(input_file, encoding=enc, low_memory=False)
            break
        except (UnicodeDecodeError, LookupError):
            continue
    
    if df is None:
        raise ValueError(f"Could not read file: {input_file}")

    # Isolate text-based columns for transliteration (removing accents/special chars)
    text_columns = [col for col in df.columns if df[col].dtype == 'object']
    for col in text_columns:
        # unidecode ensures 'São Paulo' becomes 'Sao Paulo' for consistent SQL indexing
        df[col] = df[col].apply(lambda x: unidecode.unidecode(str(x)) if pd.notna(x) else x)
    # Save as standardized UTF-8 without index to maintain schema alignment
    df.to_csv(output_file, index=False, encoding='utf-8')

def run_pre_staging_conversion(directory_path, output_suffix='_conv'):
    """
    Automates the discovery and processing of all CSV assets in the target path.
    """
    directory = Path(directory_path)
    csv_files = list(directory.glob('*.csv'))
    
    if not csv_files:
        # Idempotency check: Skip files that have already been converted
        print(f"No CSV files found in {directory_path}")
        return
        
    for csv_file in csv_files:
        if output_suffix in csv_file.stem:
            continue
        
        output_file = csv_file.parent / f"{csv_file.stem}{output_suffix}{csv_file.suffix}"
        
        try:
            convert_csv(str(csv_file), str(output_file))
        except Exception as e:
            print(f"Error processing {csv_file.name}: {e}")

if __name__ == "__main__":
    # Execute batch conversion for the current workspace
    run_pre_staging_conversion(directory_path=DATA_TARGET)
    