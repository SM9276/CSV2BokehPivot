import pandas as pd
import os

# Define the root input folder and output base folder
input_base_folder = 'Input'
output_base_folder = 'runs'

def process_files():
    for scenario in os.listdir(input_base_folder):
        scenario_path = os.path.join(input_base_folder, scenario)
        if not os.path.isdir(scenario_path):
            continue
        
        print(f'Processing scenario: {scenario}')
        
        files_to_process = {
            'Generators.csv': process_generators_file,
            'Emissions.csv': process_emissions_file,
            'Batteries.csv': process_batteries_file
        }
        
        # Process each file if it exists
        for file_name, process_function in files_to_process.items():
            file_path = os.path.join(scenario_path, file_name)
            if os.path.exists(file_path):
                process_function(scenario, file_path)
        
        # Create cap.csv after processing Generators and Batteries
        create_cap_csv(scenario, 
                       os.path.join(scenario_path, 'Generators.csv'), 
                       os.path.join(scenario_path, 'Batteries.csv'))

    print("All files have been processed successfully.")

def process_generators_file(scenario, file_path):
    df_gen = read_and_transform_csv(file_path)
    output_dir = create_output_directory(scenario)
    save_transformed_files(df_gen, output_dir)

def process_batteries_file(scenario, file_path):
    df_bat = read_and_transform_csv(file_path)
    output_dir = create_output_directory(scenario)
    append_data_to_files(df_bat, output_dir)

def process_emissions_file(scenario, file_path):
    df_emi = pd.read_csv(file_path)
    df_emi['year'] = extract_year(df_emi['_date'])
    output_dir = create_output_directory(scenario)
    
    df_emit_r = pd.DataFrame({
        "Dim1": df_emi["category_name"],
        "Dim2": 'p1',
        "Dim3": df_emi["year"],
        "Val": df_emi["Production (ton)"]
    })
    df_emit_r.to_csv(os.path.join(output_dir, 'emit_r.csv'), index=False)

def read_and_transform_csv(file_path):
    df = pd.read_csv(file_path)
    df['year'] = extract_year(df['_date'])
    df['hour'] = 'h' + df['_date'].str.split(' ').str[1].str.split(':').str[0]
    df['month'] = 'p' + df['_date'].str.split('/').str[0]
    return df

def extract_year(date_series):
    return date_series.str.split(' ').str[0].str.split('/').str[2]

def create_output_directory(scenario):
    output_dir = os.path.join(output_base_folder, scenario, 'outputs')
    os.makedirs(output_dir, exist_ok=True)
    return output_dir

def save_transformed_files(df, output_dir):
    data_mapping = {
        'gen_h.csv': {
            "Dim1": df["category_name"],
            "Dim2": df["month"],
            "Dim3": df["hour"],
            "Dim4": df["year"],
            "Val": df["Generation (GWh)"]
        },
        'gen_ivrt.csv': {
            "Dim1": df["category_name"],
            "Dim2": df["child_name"],
            "Dim3": 'p1',
            "Dim4": df["year"],
            "Val": df["Generation (GWh)"]
        },
        'gen_ann.csv': {
            "Dim1": df["category_name"],
            "Dim2": 'p1',
            "Dim3": df["year"],
            "Val": df["Generation (GWh)"]
        }
    }

    for file_name, data in data_mapping.items():
        pd.DataFrame(data).to_csv(os.path.join(output_dir, file_name), index=False)

def append_data_to_files(df, output_dir):
    data_mapping = {
        'gen_h.csv': {
            "Dim1": df["category_name"],
            "Dim2": df["month"],
            "Dim3": df["hour"],
            "Dim4": df["year"],
            "Val": df["Generation (GWh)"]
        },
        'gen_ivrt.csv': {
            "Dim1": df["category_name"],
            "Dim2": df["child_name"],
            "Dim3": 'p1',
            "Dim4": df["year"],
            "Val": df["Generation (GWh)"]
        },
        'gen_ann.csv': {
            "Dim1": df["category_name"],
            "Dim2": 'p1',
            "Dim3": df["year"],
            "Val": df["Generation (GWh)"]
        }
    }

    for file_name, data in data_mapping.items():
        append_to_csv(os.path.join(output_dir, file_name), pd.DataFrame(data))

def append_to_csv(file_path, df):
    if os.path.exists(file_path):
        df = pd.concat([pd.read_csv(file_path), df], ignore_index=True)
    df.to_csv(file_path, index=False)

def create_cap_csv(scenario, generators_file_path, batteries_file_path):
    df_list = []
    
    if os.path.exists(generators_file_path):
        df_gen = read_and_transform_csv(generators_file_path)
        df_gen_cap = df_gen[['category_name', 'year', 'Installed Capacity (MW)']].copy()
        df_gen_cap['Dim2'] = 'p1'
        df_gen_cap.rename(columns={'category_name': 'Dim1', 'Installed Capacity (MW)': 'Val'}, inplace=True)
        df_list.append(df_gen_cap)
    
    if os.path.exists(batteries_file_path):
        df_bat = read_and_transform_csv(batteries_file_path)
        df_bat_cap = df_bat[['category_name', 'year', 'Installed Capacity (MWh)']].copy()
        df_bat_cap['Dim2'] = 'p1'
        df_bat_cap.rename(columns={'category_name': 'Dim1', 'Installed Capacity (MWh)': 'Val'}, inplace=True)
        df_list.append(df_bat_cap)
    
    if df_list:
        df_combined = pd.concat(df_list, ignore_index=True)
        df_combined['Dim3'] = df_combined['year']
        df_combined = df_combined[['Dim1', 'Dim2', 'Dim3', 'Val']]

        output_dir = create_output_directory(scenario)
        df_combined.to_csv(os.path.join(output_dir, 'cap.csv'), index=False)

if __name__ == "__main__":
    process_files()
