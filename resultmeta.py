import os
import collections
import pandas as pd

# Placeholder for preprocessing functions
def sum_over_cols(df, drop_cols, group_cols):
    return df.drop(columns=drop_cols).groupby(group_cols).sum()

def scale_column(df, scale_factor, column):
    df[column] = df[column] * scale_factor
    return df

def generate_results_meta(runs_folder):
    results_meta = collections.OrderedDict()

    def create_granularity_meta(file, folder, columns):
        print(f"Creating granularity metadata for file: {file}")
        
        base_meta = {
            'file': file,
            'columns': columns,
            'preprocess': [],
            'index': [],
            'presets': collections.OrderedDict()
        }

        granularities = [
            {
                'name': 'Yearly',
                'drop_cols': ['month', 'day', 'hour'],
                'group_cols': ['tech', 'year'],
                'index': ['tech', 'year'],
                'x': 'year',
                'filter': {}
            },
            {
                'name': 'Monthly',
                'drop_cols': ['day', 'hour'],
                'group_cols': ['tech', 'year'],
                'index': ['tech', 'year', 'month'],
                'x': 'month',
                'filter': {'year': 'last'}
            },
            {
                'name': 'Daily',
                'drop_cols': ['hour'],
                'group_cols': ['tech', 'year'],
                'index': ['tech', 'year', 'month', 'day'],
                'x': 'day',
                'filter': {'year': 'last', 'month': 'last'}
            },
            {
                'name': 'Hourly',
                'drop_cols': [],
                'group_cols': ['tech', 'year'],
                'index': ['tech', 'year', 'month', 'day', 'hour'],
                'x': 'hour',
                'filter': {'year': 'last', 'month': 'last', 'day': 'last'}
            },
        ]

        meta_entries = []
        for granularity in granularities:
            print(f" - Generating metadata for granularity: {granularity['name']}")
            meta = base_meta.copy()
            meta['preprocess'] = [
                {'func': sum_over_cols, 'args': {'drop_cols': granularity['drop_cols'], 'group_cols': granularity['group_cols']}},
                {'func': scale_column, 'args': {'scale_factor': 1e-6, 'column': columns[-1]}}  # Assumes last column is value column
            ]
            meta['index'] = granularity['index']
            meta['presets'] = collections.OrderedDict((
                ('Stacked Bars', {
                    'x': granularity['x'],
                    'y': columns[-1],  # Assumes last column is value column
                    'series': 'tech',
                    'explode': 'scenario',
                    'chart_type': 'Bar',
                    'bar_width': '1',
                    'filter': granularity['filter']
                }),
            ))
            key = f"{columns[-1]} ({granularity['name']})"
            meta['file'] = file
            meta['columns'] = columns
            meta_entries.append((key, meta))
            print(f"   -> Key added: {key}")
        return meta_entries

    # Iterate over all files in the folder
    print(f"Scanning folder: {runs_folder}")
    for root, _, files in os.walk(runs_folder):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                print(f"Processing file: {file_path}")
                
                try:
                    df = pd.read_csv(file_path)
                    print(f"  Loaded CSV successfully. Columns: {list(df.columns)}")
                except Exception as e:
                    print(f"  Error loading {file_path}: {e}")
                    continue

                common_columns = ['tech', 'year', 'month', 'day', 'hour']
                value_columns = [col for col in df.columns if col not in common_columns]
                print(f"  Identified value columns: {value_columns}")

                if not value_columns:
                    print(f"  Skipping {file}: No value columns found.")
                    continue

                columns = common_columns + value_columns
                meta_entries = create_granularity_meta(file, root, columns)
                for key, meta in meta_entries:
                    results_meta[key] = meta
                    print(f"  -> Metadata entry added for key: {key}")

    print(f"Finished generating metadata. Total keys: {len(results_meta)}")
    return results_meta


# Example usage
runs_folder = './runs'  # Directory containing subfolders with CSV files
results_meta = generate_results_meta(runs_folder)

# Print the generated metadata
print("\nGenerated Metadata Summary:")
for key, value in results_meta.items():
    print(f"Key: {key}")
    print(f"  File: {value['file']}")
    print(f"  Columns: {value['columns']}")
    print(f"  Index: {value['index']}")
    print(f"  Presets: {list(value['presets'].keys())}\n")

