import pandas as pd
import os

# Define the base folders and subfolders (products)
base_folders = ['Catch22', 'TsCesium', 'TsFeatures', 'TsFel', 'TsFlex', 'TsFresh']
subfolders = ['etanolhidratado', 'gasolinac', 'gasolinadeaviacao', 'glp', 
              'oleocombustivel', 'oleodiesel', 'querosenedeaviacao', 'queroseneiluminante']
windows = [24]  # Assumed windows based on your initial code
states = ['ac', 'al', 'am', 'ap', 'ba', 'ce', 'df', 'es', 'go', 'ma', 'mg', 'ms', 'mt', 'pa', 'pb', 'pe', 'pi', 'pr', 'rj', 'rn', 'ro', 'rr', 'rs', 'sc', 'se', 'sp', 'to']

# Directory to save concatenated DataFrames
output_folder = 'concatenate'
os.makedirs(output_folder, exist_ok=True)

# Function to remove all 'timestamp' columns except for the first one
def process_timestamp_columns(df, base_folder, subfolder):
    timestamp_columns = [col for col in df.columns if 'timestamp' in col]
    
    if base_folder == 'Catch22' and subfolder in subfolders:
        # Keep the first 'timestamp' column and drop the rest
        if len(timestamp_columns) > 1:
            first_timestamp_column = timestamp_columns[0]
            columns_to_drop = timestamp_columns[1:]
            df = df.drop(columns=columns_to_drop)
    else:
        # Drop all 'timestamp' columns
        if timestamp_columns:
            df = df.drop(columns=timestamp_columns)
    
    return df

# Dictionary to store concatenated dataframes
concatenated_dataframes = {}

# Iterate over subfolders (products), windows, and states
for product in subfolders:
    for window in windows:
        for state in states:
            dataframes = []
            
            # Iterate over base folders to find and read the corresponding CSV files
            for base_folder in base_folders:
                file_name = f'FEAT_{base_folder}_{product}_{window}_{state}.csv'
                file_path = os.path.join(base_folder, product, file_name)
                
                if os.path.exists(file_path):
                    df = pd.read_csv(file_path)
                    
                    # Remove the first 12 rows in 'timestamp' column except for 'TsFeatures'
                    if base_folder != 'TsFeatures' and 'timestamp' in df.columns:
                        df = df.iloc[12:].reset_index(drop=True)
                    
                    # Process 'timestamp' columns according to base folder and subfolder
                    df = process_timestamp_columns(df, base_folder, product)
                    
                    dataframes.append(df)
                else:
                    print(f"File not found: {file_path}")
            
            # Concatenate dataframes for the current product, window, and state
            if dataframes:
                concatenated_df = pd.concat(dataframes, axis=1, ignore_index=False)
                key = f'{product}_{window}_{state}'
                concatenated_dataframes[key] = concatenated_df

# Save the concatenated dataframes to CSV files in the 'concatenate' folder
for key, dataframe in concatenated_dataframes.items():
    output_path = os.path.join(output_folder, f'{key}24
                               _concatenated.csv')
    dataframe.to_csv(output_path, index=False)
    print(f'Saved: {output_path}')
