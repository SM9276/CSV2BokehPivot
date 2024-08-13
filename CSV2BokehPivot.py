import os
import pandas as pd
import json

CONFIG_FILE = 'configurations.json'
INPUT_FOLDER = 'inputs'
OUTPUT_FOLDER = 'runs'

def load_configurations():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as file:
            return json.load(file)
    return {}

def save_configurations(configurations):
    with open(CONFIG_FILE, 'w') as file:
        json.dump(configurations, file, indent=4)

def list_all_csv_files(folder):
    """ List all unique CSV files with their paths. """
    csv_files = {}
    for root, _, files in os.walk(folder):
        for file in files:
            if file.endswith('.csv'):
                relative_path = os.path.relpath(os.path.join(root, file), folder)
                csv_files[file] = os.path.join(root, file)
    return csv_files

def list_all_csv_files_with_repeats(folder):
    """ List all CSV files with their full paths, including duplicates. """
    csv_files = []
    for root, _, files in os.walk(folder):
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(os.path.join(root, file))
    return csv_files

def map_columns(old_csv):
    df = pd.read_csv(old_csv)
    columns = df.columns.tolist()
    return columns

def mapping_mode():
    configurations = load_configurations()
    csv_files = list_all_csv_files(INPUT_FOLDER)
    
    if not csv_files:
        print("No CSV files found in the 'inputs' folder.")
        return

    print("Available CSV files (names only):")
    unique_names = set(os.path.basename(path) for path in csv_files.values())
    for idx, file_name in enumerate(unique_names):
        print(f"{idx + 1}. {file_name}")
    
    file_idx = int(input("Select a CSV file by number: ")) - 1
    selected_file_name = list(unique_names)[file_idx]
    old_csv_path = csv_files[selected_file_name]
    columns = map_columns(old_csv_path)
    
    print("Available columns to map:")
    for idx, col in enumerate(columns):
        print(f"{idx + 1}. {col}")
    
    dimensions = {}
    dimension_count = 1
    
    while True:
        print(f"Enter column for dimension {dimension_count} (or type 'value' to select value column, or type 'constant' to enter a constant string):")
        user_input = input()
        
        if user_input.lower() == 'constant':
            constant_value = input(f"Enter constant value for dimension {dimension_count}: ")
            dimensions[f"dimension_{dimension_count}"] = constant_value
            dimension_count += 1
        elif user_input.lower() == 'value':
            break
        else:
            try:
                dim_col = int(user_input) - 1
                if 0 <= dim_col < len(columns):
                    dimensions[f"dimension_{dimension_count}"] = columns[dim_col]
                    dimension_count += 1
                else:
                    print("Invalid selection. Please try again.")
            except ValueError:
                print("Invalid input. Please enter a number, 'constant', or 'value'.")
    
    print("Available columns for value:")
    for idx, col in enumerate(columns):
        print(f"{idx + 1}. {col}")
    
    value_column = int(input("Select column for values by number: ")) - 1
    if 0 <= value_column < len(columns):
        dimensions["value"] = columns[value_column]
    else:
        print("Invalid selection. No value column selected.")
        return
    
    new_file_name = input("Enter the new file name (without extension): ")
    if not new_file_name:
        print("Invalid file name. Configuration not saved.")
        return
    
    configurations[selected_file_name] = {
        "dimensions": dimensions,
        "new_file_name": new_file_name
    }
    
    save_configurations(configurations)
    print("Configuration saved.")

def execute_mode():
    configurations = load_configurations()
    all_csv_files = list_all_csv_files_with_repeats(INPUT_FOLDER)
    
    # Create a reverse mapping from output file names to their configurations
    config_by_file = {}
    for file_name, config in configurations.items():
        config_by_file[file_name] = config
    
    # Process all CSV files
    for full_path in all_csv_files:
        file_name = os.path.basename(full_path)
        if file_name in config_by_file:
            config = config_by_file[file_name]
            df = pd.read_csv(full_path)
            new_df = pd.DataFrame()
            dimensions = config["dimensions"]
            
            # Add dimensions to new DataFrame
            for key in dimensions:
                if key.startswith("dimension"):
                    column_name = dimensions[key]
                    if isinstance(column_name, str):
                        # If the dimension is a constant string, add it directly
                        new_df[key] = [column_name] * len(df)
                    else:
                        # If the dimension is a column name, map it
                        if column_name in df.columns:
                            new_df[key] = df[column_name]
                        else:
                            print(f"Warning: Column '{column_name}' not found in '{file_name}'.")

            # Add value column to new DataFrame
            if "value" in dimensions:
                value_col = dimensions["value"]
                if value_col in df.columns:
                    new_df['Value'] = df[value_col]
                else:
                    print(f"Warning: Column '{value_col}' not found in '{file_name}'.")

            # Generate new CSV file
            new_file_name = config["new_file_name"]
            
            # Determine the output folder structure
            original_folder = os.path.dirname(full_path)
            relative_folder = os.path.relpath(original_folder, INPUT_FOLDER)
            model_folder = os.path.basename(relative_folder)
            output_folder = os.path.join('runs', model_folder, 'outputs')
            
            new_csv = os.path.join(output_folder, f"{new_file_name}.csv")
            os.makedirs(os.path.dirname(new_csv), exist_ok=True)
            new_df.to_csv(new_csv, index=False)
            print(f"New CSV file created: {new_csv}")
        else:
            print(f"Warning: Configuration for '{file_name}' but file not found in the inputs.")


def main():
    mode = input("Enter mode (Mapping/Execute): ").strip().lower()
    if mode == 'mapping':
        mapping_mode()
    elif mode == 'execute':
        execute_mode()
    else:
        print("Invalid mode.")

if __name__ == "__main__":
    main()
