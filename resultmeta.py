import os
import collections
import pandas as pd

# Placeholder for preprocessing functions
def sum_over_cols(df, drop_cols, group_cols):
    print(f"Summing over columns, dropping {drop_cols}, grouping by {group_cols}")
    return df.drop(columns=drop_cols).groupby(group_cols).sum()

def scale_column(df, scale_factor, column):
    print(f"Scaling column {column} by factor {scale_factor}")
    df[column] = df[column] * scale_factor
    return df

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

                # Identify possible value columns (excluding common metadata columns)
                common_columns = ['Tech', 'Year', 'Month', 'Day', 'Hour']
                value_columns = [col for col in df.columns if col not in common_columns]

                if not value_columns:
                    print(f"Skipping {file} as it does not contain any value columns apart from {common_columns}.")
                    continue  # Skip files without valid value columns

                # Generate metadata for each value column
                for value_column in value_columns:
                    print(f"Processing value column: {value_column}")

                    # Create the metadata for the current column
                    key = f"{value_column} National"
                    results_meta[key] = {
                        'file': file,
                        'folder': root,
                        'columns': common_columns + [value_column],
                        'preprocess': [
                            {'func': sum_over_cols, 'args': {'drop_cols': ['rb'], 'group_cols': ['Tech', 'Year', 'Month', 'Day', 'Hour']}},
                            {'func': scale_column, 'args': {'scale_factor': 1e-6, 'column': value_column}},
                        ],
                        'index': ['Tech', 'Year', 'Month', 'Day', 'Hour'],
                        'presets': collections.OrderedDict((
                            ('Stacked Bars', {
                                'x': 'Hour',
                                'y': value_column,
                                'series': 'Tech',
                                'explode': 'Scenario',
                                'chart_type': 'Bar',
                                'bar_width': '1'
                            }),
                        )),
                    }

                    print(f"Metadata for value column '{value_column}' in file '{file}' generated: {results_meta[key]}")

    return results_meta


# Example usage
runs_folder = './runs'  # Directory containing the subfolders with CSV files
results_meta = generate_results_meta(runs_folder)

# Printing the generated results_meta
print("\nFinal results_meta structure:")
for key, value in results_meta.items():
    print(f"Key: {key}, Value: {value}")

