import os
import pandas as pd
import json
import tkinter as tk
from tkinter import filedialog, messagebox, simpledialog

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


def map_columns(file_path):
    df = pd.read_csv(file_path)
    return df.columns.tolist()


class CSVProcessorApp:
    def __init__(self, root):
        self.root = root
        self.root.title("CSV Processor")
        self.configurations = load_configurations()

        # Main buttons
        self.btn_mapping = tk.Button(root, text="Mapping Mode", command=self.mapping_mode)
        self.btn_mapping.pack(pady=10)

        self.btn_execute = tk.Button(root, text="Execute Mode", command=self.execute_mode)
        self.btn_execute.pack(pady=10)

        self.btn_exit = tk.Button(root, text="Exit", command=root.quit)
        self.btn_exit.pack(pady=10)

    def mapping_mode(self):
        csv_files = list_all_csv_files(INPUT_FOLDER)
        if not csv_files:
            messagebox.showerror("Error", "No CSV files found in the 'inputs' folder.")
            return

        # File selection window
        file_selection_window = tk.Toplevel(self.root)
        file_selection_window.title("Select CSV File")

        tk.Label(file_selection_window, text="Select a CSV File:").pack(pady=5)
        listbox = tk.Listbox(file_selection_window, selectmode=tk.SINGLE, width=50, height=15)
        listbox.pack(pady=10)

        # Populate Listbox
        file_names = list(csv_files.keys())
        file_paths = list(csv_files.values())
        for file_name in file_names:
            listbox.insert(tk.END, file_name)

        def select_file():
            selected_index = listbox.curselection()
            if not selected_index:
                messagebox.showerror("Error", "No file selected.")
                return

            selected_file = file_paths[selected_index[0]]
            file_selection_window.destroy()
            self.configure_mapping(selected_file, file_names[selected_index[0]])

        tk.Button(file_selection_window, text="Select", command=select_file).pack(pady=5)

    def configure_mapping(self, file_path, file_name):
        columns = map_columns(file_path)
        dimensions = {}
        dimension_count = 1

        mapping_window = tk.Toplevel(self.root)
        mapping_window.title("Map Dimensions")
        tk.Label(mapping_window, text="Map Dimensions").pack(pady=5)

        column_listbox = tk.Listbox(mapping_window, selectmode=tk.SINGLE, width=50, height=15)
        column_listbox.pack(pady=10)

        for col in columns:
            column_listbox.insert(tk.END, col)

        def add_dimension():
            nonlocal dimension_count
            selected_index = column_listbox.curselection()
            if selected_index:
                dimensions[f"Dim{dimension_count}"] = columns[selected_index[0]]
            else:
                constant_value = simpledialog.askstring("Constant Value", f"Enter constant value for dimension {dimension_count}:")
                if constant_value:
                    dimensions[f"Dim{dimension_count}"] = constant_value
                else:
                    return

            dimension_count += 1
            messagebox.showinfo("Info", f"Dimension {dimension_count - 1} added successfully!")

        def finish_mapping():
            value_col_index = simpledialog.askinteger("Value Column", "Enter the column index for values:")
            if value_col_index and 0 <= value_col_index - 1 < len(columns):
                dimensions["val"] = columns[value_col_index - 1]
            else:
                messagebox.showerror("Error", "Invalid value column.")
                return

            new_file_name = simpledialog.askstring("File Name", "Enter the new file name (without extension):")
            if not new_file_name:
                messagebox.showerror("Error", "Invalid file name.")
                return

            self.configurations[new_file_name] = {
                "original_file": file_name,
                "dimensions": dimensions
            }
            save_configurations(self.configurations)
            mapping_window.destroy()
            messagebox.showinfo("Success", "Configuration saved successfully!")

        tk.Button(mapping_window, text="Add Dimension", command=add_dimension).pack(pady=5)
        tk.Button(mapping_window, text="Finish Mapping", command=finish_mapping).pack(pady=5)

    def execute_mode(self):
        csv_files = list_all_csv_files_with_repeats(INPUT_FOLDER)
        if not csv_files:
            messagebox.showerror("Error", "No CSV files found in the 'inputs' folder.")
            return

        for file_path in csv_files:
            file_name = os.path.basename(file_path)
            for new_file_name, config in self.configurations.items():
                if config["original_file"] == file_name:
                    df = pd.read_csv(file_path)
                    new_df = pd.DataFrame()
                    dimensions = config["dimensions"]

                    for key, column_name in dimensions.items():
                        if key.startswith("Dim"):
                            new_df[key] = df[column_name] if column_name in df.columns else [column_name] * len(df)

                    if "val" in dimensions and dimensions["val"] in df.columns:
                        new_df["val"] = df[dimensions["val"]]

                    output_folder = os.path.join(OUTPUT_FOLDER, file_name)
                    os.makedirs(output_folder, exist_ok=True)

                    dummy_file = os.path.join(output_folder, "BP.csv")
                    with open(dummy_file, "w") as f:
                        f.write("This is a placeholder file.\n")

                    output_file = os.path.join(output_folder, f"{new_file_name}.csv")
                    new_df.to_csv(output_file, index=False)

        messagebox.showinfo("Success", "All files processed successfully!")


if __name__ == "__main__":
    if not os.path.exists(INPUT_FOLDER):
        os.makedirs(INPUT_FOLDER)
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)

    root = tk.Tk()
    app = CSVProcessorApp(root)
    root.mainloop()

