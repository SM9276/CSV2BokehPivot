# CSV2BokehPivot

CSV2BokehPivot is a Python application that allows you to process CSV files, map their columns, configure dimensions, and generate new CSV files based on your mappings. The app uses a simple graphical interface for easy interaction and is designed to help you manage and manipulate CSV files efficiently.

## Features

- **Mapping Mode**: Select and map the columns of CSV files to specific dimensions (e.g., year, month, day).
- **Execute Mode**: Run the saved mappings on CSV files and generate new output files.
- **View Mappings**: View, manage, or delete the mappings you've saved.

## Requirements

To run CSV2BokehPivot, you'll need:

- **Python 3.x**
- **Tkinter** (for the graphical user interface)
- **Pandas** (for handling CSV files)
- **Miniforge** (for setting up the Python environment)

## Setup

1. **Install Dependencies**:
   - Download and install [Miniforge](https://github.com/conda-forge/miniforge) if you don't have it already.
   - Use `setup.bat` to automatically download and install the required Python environment.

   ```batch
   setup.bat
   ```

2. **Launch the Application**:
   - Run `launch.bat` to activate the Miniforge environment and start the application.

   ```batch
   launch.bat
   ```

## How to Use

### 1. **Mapping Mode**:
   - Click on the **Mapping Mode** button.
   - Choose a CSV file from the "inputs" folder.
   - Map the columns to the required dimensions (e.g., year, month, day, etc.).
   - Add any custom dimensions or constant values if needed.
   - Save your mappings.

### 2. **Execute Mode**:
   - Click the **Execute Mode** button to apply your saved mappings to all CSV files in the "inputs" folder.
   - The app will process the files and generate new CSV files, which will be stored in the "runs" folder.

### 3. **View Mappings**:
   - Click the **View Mappings** button to see and manage the list of saved mappings.
   - You can delete or view details of any mapping.

## Folder Structure

- `inputs/`: The folder where input CSV files are placed.
- `runs/`: The folder where the processed output files are saved.
- `configurations.json`: The file that stores your saved mappings.

## Notes

- Ensure your CSV files are placed in the `inputs` folder before using the app.
- After executing the processing, check the `runs` folder for the output files.

## License

This project is open-source and free to use.
