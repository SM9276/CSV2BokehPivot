import os
import collections
import pandas as pd

# Assuming the preprocessing functions are already defined:
# sum_over_cols, sum_over_months, sum_over_days, sum_over_hours, scale_column

def generate_results_meta(runs_folder):
    # Walk through all subdirectories and files within 'runs' folder
    results_meta = collections.OrderedDict()

    print(f"Searching for CSV files in '{runs_folder}' and its subfolders...\n")
    
    for root, dirs, files in os.walk(runs_folder):
        # Filter for CSV files
        csv_files = [f for f in files if f.endswith('.csv')]
        
        if csv_files:
            print(f"Found CSV files in folder: {root}")
            for file in csv_files:
                print(f"\nProcessing file: {file} in folder: {root}")
                # Read the file to get columns and check for expected format
                file_path = os.path.join(root, file)
                df = pd.read_csv(file_path)

                # Displaying the first few rows to inspect the structure
                print(f"First few rows of {file}:\n{df.head()}")

                # Identify common columns across the files
                common_columns = ['tech', 'year', 'month', 'day', 'hour', 'value']

                # Checking for expected columns in the dataframe
                if 'Capacity (GW)' in df.columns:
                    value_column = 'Capacity (GW)'
                elif 'Load (GW)' in df.columns:
                    value_column = 'Load (GW)'
                elif 'Generation (TWh)' in df.columns:
                    value_column = 'Generation (TWh)'
                elif 'Emissions (tonne)' in df.columns:
                    value_column = 'Emissions (tonne)'
                else:
                    print(f"Skipping {file} since it does not have an expected value column")
                    continue  # Skip files that don't have the expected columns

                print(f"Identified value column: {value_column}")

                # Preprocess function to sum over columns (example for "sum_over_cols" function)
                def sum_over_cols(df, drop_cols, group_cols):
                    print(f"Summing over columns, dropping {drop_cols}, grouping by {group_cols}")
                    return df.drop(columns=drop_cols).groupby(group_cols).sum()

                # Create the metadata for the current file
                key = f"{value_column} National"
                results_meta[key] = {
                    'file': file,
                    'folder': root,
                    'columns': ['tech', 'rb', 'year', 'month', 'day', 'hour', value_column],
                    'preprocess': [
                        {'func': sum_over_cols, 'args': {'drop_cols': ['rb'], 'group_cols': ['tech', 'year', 'month', 'day', 'hour']}},
                        {'func': scale_column, 'args': {'scale_factor': 1e-6, 'column': value_column}},
                    ],
                    'index': ['tech', 'year', 'month', 'day', 'hour'],
                    'presets': collections.OrderedDict((
                        ('Stacked Bars', {
                            'x': 'hour',
                            'y': value_column,
                            'series': 'tech',
                            'explode': 'scenario',
                            'chart_type': 'Bar',
                            'bar_width': '1'
                        }),
                    )),
                }

                print(f"Metadata for {file} generated: {results_meta[key]}")

    return results_meta


# Example usage
runs_folder = './runs'  # Directory containing the subfolders with CSV files
results_meta = generate_results_meta(runs_folder)

# Printing the generated results_meta
print("\nFinal results_meta structure:")
for key, value in results_meta.items():
    print(f"Key: {key}, Value: {value}")

