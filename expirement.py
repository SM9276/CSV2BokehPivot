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
    """List all unique CSV files with their paths."""
    csv_files = {}
    for root, _, files in os.walk(folder):
        for file in files:
            if file.endswith('.csv'):
                relative_path = os.path.relpath(os.path.join(root, file), folder)
                csv_files[file] = os.path.join(root, file)
    return csv_files

def list_all_csv_files_with_repeats(folder):
    """List all CSV files with their full paths, including duplicates."""
    csv_files = []
    for root, _, files in os.walk(folder):
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(os.path.join(root, file))
    return csv_files

def map_columns(old_csv):
    df = pd.read_csv(old_csv)
    return df.columns.tolist()

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
        print(f"Enter column for dimension {dimension_count} (or type 'constant' to enter a constant string):")
        user_input = input()
        
        if user_input.lower() == 'constant':
            constant_value = input(f"Enter constant value for dimension {dimension_count}: ")
            dimensions[f"Dim{dimension_count}"] = constant_value
            dimension_count += 1
            break
        else:
            try:
                dim_col = int(user_input) - 1
                if 0 <= dim_col < len(columns):
                    dimensions[f"Dim{dimension_count}"] = columns[dim_col]
                    dimension_count += 1
                    break
                else:
                    print("Invalid selection. Please try again.")
            except ValueError:
                print("Invalid input. Please enter a number or 'constant'.")
    
    default_values = {"year": None, "month": 12, "day": 31, "hour": 23}
    
    print("Looking for 'year', 'month', 'day', and 'hour' columns. If not found, default values will be used.")
    for dim, default in default_values.items():
        found = False
        for idx, col in enumerate(columns):
            if dim in col.lower():
                dimensions[dim] = col
                found = True
                break
        if not found:
            print(f"'{dim}' column not found, defaulting to {default}.")
            dimensions[dim] = default

    print("Available columns for value:")
    for idx, col in enumerate(columns):
        print(f"{idx + 1}. {col}")
    
    value_column = int(input("Select column for values by number: ")) - 1
    if 0 <= value_column < len(columns):
        dimensions["val"] = columns[value_column]
    else:
        print("Invalid selection. No value column selected.")
        return
    
    new_file_name = input("Enter the new file name (without extension): ")
    if not new_file_name:
        print("Invalid file name. Configuration not saved.")
        return
    
    configurations[new_file_name] = {
        "original_file": selected_file_name,
        "dimensions": dimensions
    }
    
    save_configurations(configurations)
    print("Configuration saved.")

def execute_mode():
    configurations = load_configurations()
    all_csv_files = list_all_csv_files_with_repeats(INPUT_FOLDER)
    
    print(f"Loaded configurations: {configurations}")
    print(f"Found CSV files: {all_csv_files}")
    
    for full_path in all_csv_files:
        file_name = os.path.basename(full_path)
        print(f"Processing file: {file_name}")
        
        for new_file_name, config in configurations.items():
            if config["original_file"] == file_name:
                print(f"Found configuration for {file_name}. Processing...")
                df = pd.read_csv(full_path)
                new_df = pd.DataFrame()
                dimensions = config["dimensions"]
                
                for key, column_name in dimensions.items():
                    if key.startswith("Dim"):
                        if column_name in df.columns:
                            new_df[key] = df[column_name]
                        else:
                            new_df[key] = [column_name] * len(df)
                    elif key in ["year", "month", "day", "hour"]:
                        if column_name in df.columns:
                            new_df[key] = df[column_name]
                        else:
                            new_df[key] = [column_name] * len(df)

                if "val" in dimensions:
                    value_col = dimensions["val"]
                    if value_col in df.columns:
                        new_df[value_col] = df[value_col]
                    else:
                        new_df[value_col] = [value_col] * len(df)

                original_folder = os.path.dirname(full_path)
                relative_folder = os.path.relpath(original_folder, INPUT_FOLDER)
                output_folder = os.path.join(OUTPUT_FOLDER, relative_folder, 'outputs')
                
                # Create the output folder if it doesn't exist
                os.makedirs(output_folder, exist_ok=True)
                
                # Create a dummy BP.csv file in the output folder
                dummy_file_path = os.path.join(output_folder, 'BP.csv')
                if not os.path.exists(dummy_file_path):
                    with open(dummy_file_path, 'w') as dummy_file:
                        dummy_file.write("This is a placeholder file.\n")
                
                # Save the new CSV file
                new_csv = os.path.join(output_folder, f"{new_file_name}.csv")
                new_df.to_csv(new_csv, index=False)
                print(f"New CSV file created: {new_csv}")

        if not any(config["original_file"] == file_name for config in configurations.values()):
            print(f"Warning: Configuration for '{file_name}' not found in the configurations.")

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

