# CSV2BokehPivot

CSV2BokehPivot is a Python application that allows you to process CSV files, map their columns, configure dimensions, and generate new CSV files based on your mappings. The app uses a simple graphical interface for easy interaction and is designed to help you manage and manipulate CSV files efficiently.

## Features

- **Mapping Mode**: Select and map the columns of CSV files to specific dimensions.
- **Execute Mode**: Run the saved mappings on CSV files and generate new output files.
- **View Mappings**: View, manage, or delete the mappings you've saved.

## Setup

1. **Install Dependencies**:
   - Use `setup.bat` to automatically download and install the required Python environment.

   ```batch
   setup.bat
   ```

2. **Launch the Application**:
   - Run `launch.bat` by double clicking the icon or running the script in the terminal.

   ```batch
   launch.bat
   ```

## How to Use

### 1. **Mapping Mode**:
   - Click on the **Mapping Mode** button.
   - Choose a CSV file from the "inputs" folder.
   - A test dataset is provided.
   - click a property that you would like to group the data with, for example category_name in the fake.csv
   - click 'add dimention'
   - Then select the value you would like the graph to represent, for example generation in the fake.csv
   - Click Finish mapping and input a name for the csv to be called.
   - Save your mappings.

### 2. **Execute Mode**:
   - Click the **Execute Mode** button to apply your saved mappings to all CSV files in the "inputs" folder.
   - The app will process the files and generate new CSV files, which will be stored in the "runs" folder.

### 3. **View Mappings**:
   - Click the **View Mappings** button to see and manage the list of saved mappings.
   - You can delete or view details of any mapping.

### 4. **BokehPivot**:
   - Navigate to the X2BokehPivot folder and double click `launch.bat` to start the program 
   - Input the folderpath leading to the runs folder in the searchbar.
   - Click on which dataset you would like to view, in the fakedata usecase it would be generation.
   - Inorder to use the pivot feature click operations
    - First Operation: diffrences
    - Operate Across: scenario
    - Base: DemoData1

## Folder Structure

- `inputs/`: The folder where input CSV files are placed.
- `runs/`: The folder where the processed output files are saved.
- `configurations.json`: The file that stores your saved mappings.

## Notes

- Ensure your CSV files are placed in the `inputs` folder before using the app.
- Ensure all CSV files have at least year column, you can have breakdown to months,days and hours.
- After executing the processing, check the `runs` folder for the output files.

## License

This project is open-source and free to use.
