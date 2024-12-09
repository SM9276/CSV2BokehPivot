import os
import pandas as pd
import json
import tkinter as tk
from tkinter import filedialog, messagebox, simpledialog, scrolledtext

CONFIG_FILE = 'configurations.json'
INPUT_FOLDER = 'inputs'
OUTPUT_FOLDER = 'runs'


def load_configurations():
    """Load saved configurations."""
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as file:
            return json.load(file)
    return {}


def save_configurations(configurations):
    """Save configurations to a JSON file."""
    with open(CONFIG_FILE, 'w') as file:
        json.dump(configurations, file, indent=4)


def list_all_csv_files_with_repeats(folder):
    """List all CSV files in the folder, including those with duplicate names."""
    csv_files = []
    for root, _, files in os.walk(folder):
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(os.path.join(root, file))
    return csv_files


def map_columns(file_path):
    """Get column names from a CSV file."""
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

        self.btn_view_mappings = tk.Button(root, text="View Mappings", command=self.view_mappings)
        self.btn_view_mappings.pack(pady=10)

        self.btn_exit = tk.Button(root, text="Exit", command=root.quit)
        self.btn_exit.pack(pady=10)

    def mapping_mode(self):
        csv_files = list_all_csv_files_with_repeats(INPUT_FOLDER)
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
        file_names = list(csv_files)
        for file in file_names:
            listbox.insert(tk.END, os.path.basename(file))

        def select_file():
            selected_index = listbox.curselection()
            if not selected_index:
                messagebox.showerror("Error", "No file selected.")
                return

            selected_file = file_names[selected_index[0]]
            file_selection_window.destroy()
            self.configure_mapping(selected_file)

        tk.Button(file_selection_window, text="Select", command=select_file).pack(pady=5)

    def configure_mapping(self, file_path):
        columns = map_columns(file_path)
        dimensions = {}
        dimension_count = 1

        # Auto-map time columns
        default_values = {"year": None, "month": 12, "day": 31, "hour": 23}
        for time_unit, default in default_values.items():
            found = False
            for col in columns:
                if time_unit in col.lower():
                    dimensions[time_unit] = col
                    found = True
                    break
            if not found:
                dimensions[time_unit] = default

        mapping_window = tk.Toplevel(self.root)
        mapping_window.title("Map Dimensions")
        tk.Label(mapping_window, text="Map Dimensions").pack(pady=5)

        column_listbox = tk.Listbox(mapping_window, selectmode=tk.SINGLE, width=50, height=15)
        column_listbox.pack(pady=10)

        for col in columns:
            column_listbox.insert(tk.END, col)

        def add_dimension():
            nonlocal dimension_count
            if dimension_count == 1:
                # Ensure Dim1 is added first
                selected_index = column_listbox.curselection()
                if selected_index:
                    dimensions[f"Dim{dimension_count}"] = columns[selected_index[0]]
                    dimension_count += 1
                    messagebox.showinfo("Info", f"Dimension {dimension_count - 1} (Dim1) added successfully!")
                else:
                    messagebox.showerror("Error", "No column selected for Dim1.")
                return

            # Add additional dimensions
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

        def map_value_column():
            selected_index = column_listbox.curselection()
            if selected_index:
                dimensions["val"] = columns[selected_index[0]]
                messagebox.showinfo("Info", f"Value column set to: {columns[selected_index[0]]}")
            else:
                messagebox.showerror("Error", "No value column selected.")

        def finish_mapping():
            # Ensure column order in configurations
            ordered_dimensions = {}
            if "Dim1" in dimensions:
                ordered_dimensions["Dim1"] = dimensions["Dim1"]
            for key in ["year", "month", "day", "hour"]:
                if key in dimensions:
                    ordered_dimensions[key] = dimensions[key]
            if "val" in dimensions:
                ordered_dimensions["val"] = dimensions["val"]
            for key, value in dimensions.items():
                if key.startswith("Dim") and key != "Dim1":
                    ordered_dimensions[key] = value

            new_file_name = simpledialog.askstring("File Name", "Enter the new file name (without extension):")
            if not new_file_name:
                messagebox.showerror("Error", "Invalid file name.")
                return

            self.configurations[new_file_name] = {
                "original_file": os.path.basename(file_path),
                "dimensions": ordered_dimensions
            }
            save_configurations(self.configurations)
            mapping_window.destroy()
            messagebox.showinfo("Success", "Configuration saved successfully!")

        tk.Button(mapping_window, text="Add Dimension", command=add_dimension).pack(pady=5)
        tk.Button(mapping_window, text="Set Value Column", command=map_value_column).pack(pady=5)
        tk.Button(mapping_window, text="Finish Mapping", command=finish_mapping).pack(pady=5)

    def view_mappings(self):
        mappings_window = tk.Toplevel(self.root)
        mappings_window.title("View Mappings")

        tk.Label(mappings_window, text="Existing Mappings:").pack(pady=5)
        listbox = tk.Listbox(mappings_window, width=50, height=15)
        listbox.pack(pady=10)

        mapping_keys = list(self.configurations.keys())
        for key in mapping_keys:
            listbox.insert(tk.END, key)

        def delete_mapping():
            selected_index = listbox.curselection()
            if selected_index:
                selected_key = mapping_keys[selected_index[0]]
                del self.configurations[selected_key]
                save_configurations(self.configurations)
                listbox.delete(selected_index[0])
                messagebox.showinfo("Success", f"Mapping '{selected_key}' deleted successfully!")
            else:
                messagebox.showerror("Error", "No mapping selected.")

        def view_mapping_details():
            selected_index = listbox.curselection()
            if selected_index:
                selected_key = mapping_keys[selected_index[0]]
                config = self.configurations[selected_key]
                details = f"File: {config['original_file']}\nDimensions:\n"
                for key, val in config['dimensions'].items():
                    details += f"{key}: {val}\n"
                messagebox.showinfo("Mapping Details", details)
            else:
                messagebox.showerror("Error", "No mapping selected.")

        tk.Button(mappings_window, text="Delete Mapping", command=delete_mapping).pack(pady=5)
        tk.Button(mappings_window, text="View Mapping Details", command=view_mapping_details).pack(pady=5)

    def execute_mode(self):
        configurations = load_configurations()
        all_csv_files = list_all_csv_files_with_repeats(INPUT_FOLDER)

        # Create a window to display the log
        log_window = tk.Toplevel(self.root)
        log_window.title("Execution Log")

        log_text = scrolledtext.ScrolledText(log_window, width=80, height=20)
        log_text.pack(pady=10)

        log_text.insert(tk.END, "Starting file processing...\n")
        log_text.yview(tk.END)

        for full_path in all_csv_files:
            file_name = os.path.basename(full_path)
            log_text.insert(tk.END, f"Processing file: {file_name}\n")
            log_text.yview(tk.END)

            for new_file_name, config in configurations.items():
                if config['original_file'] == file_name:
                    df = pd.read_csv(full_path)
                    mapped_df = pd.DataFrame()

                    for dim, col in config['dimensions'].items():
                        if col in df.columns:
                            mapped_df[dim] = df[col]
                        else:
                            mapped_df[dim] = col  # Assign constant values

                    output_file = os.path.join(OUTPUT_FOLDER, f"{new_file_name}.csv")
                    mapped_df.to_csv(output_file, index=False)
                    log_text.insert(tk.END, f"Output saved to: {output_file}\n")
                    log_text.yview(tk.END)

        log_text.insert(tk.END, "Processing complete.\n")
        log_text.yview(tk.END)


if __name__ == "__main__":
    root = tk.Tk()
    app = CSVProcessorApp(root)
    root.mainloop()

