# CSV2BokehPivot

Welcome to CSV2BokehPivot! This project enables you to visualize your CSV files using the Bokeh Pivot tool. Follow these steps to successfully map, generate, and visualize your data.

## Installation

1. **Set Up the Environment:**
   - Locate the `setup.bat` file in your project directory.
   - Double-click `setup.bat` to run it. This script will install all necessary tools and libraries for the project.

## Preparing Your Data

1. **Organize Your CSV Files:**
   - Place all your CSV solution files into the `inputs` folder. If you have multiple files, organize them into subfolders within `inputs`.

2. **Choose a Visualization in Bokeh Pivot:**
   - Open Bokeh Pivot.
   - Load example files from the `ReEDS_example` folder and experiment with different graphs to determine which visualization best suits your needs. For example, you might choose the `Generation ivrt (GW)` option.

3. **Understand the Required Format:**
   - Open the `ReEDS_example` folder. Look at the example file `gen_ivrt.csv` located in the `ReEDS_example/output/` folder.
   - This example file will show you the format you need:
     - `Dim1`: Technology
     - `Dim2`: Generator Name
     - `Dim3`: Region
     - `Dim4`: Year
     - `Val`: Generation in GW

4. **ReEDS Specifics:**
   - **Regions:** In ReEDS, regions are denoted with a prefix `p`. For example, `p1`, `p2`, `p3`, etc.
   - **Time Slices:** ReEDS time slices are split into 17 segments, each prefixed with `h`. For example, `h1`, `h2`, `h3`, etc.

## Configuring Your Mapping

1. **Launch the Mapping Tool:**
   - Double-click `launch.bat` to start the tool.
   - Select "Mapping mode" when prompted.

2. **Select Your CSV File:**
   - The tool will list CSV files from the `inputs` folder. Choose the file you want to map, such as `generation.csv`.

3. **Map Your Columns to Dimensions:**
   - The tool will display the columns from your selected file.
   - Match these columns to the dimensions `Dim1`, `Dim2`, `Dim3`, `Dim4`, and `Val`:
     - For example, if you have a column named "Technology," map it to `Dim1`.
   - If a dimension requires a constant value (e.g., a fixed string), type `constant` and enter the value.
   - Type `value` to select the column for `Val`.

4. **Save Your Configuration:**
   - Enter a name for your mapping configuration to easily identify it later.
   - The tool will automatically save your mapping settings.

## Generating Output Files

1. **Run the Execution Tool:**
   - Double-click `launch.bat` again.
   - Choose "Execute mode" when prompted.

2. **Locate the Output Files:**
   - The tool will generate new CSV files in the `runs` folder.
   - Each file will be named according to the mapping configuration you created.

## Visualizing the Data

1. **Load and Visualize Your Data:**
   - Import the newly created CSV files from the `runs` folder into Bokeh Pivot.
   - Apply the visualization you selected to see your data represented effectively.

## Example Workflow

1. **Choose a Visualization:**
   - Start by using Bokeh Pivot to explore example files in the `ReEDS_example` folder.
   - Experiment with various graphs to find one that aligns with your data needs. For example, you might select the `Generation ivrt (GW)` graph.

2. **Review Example File:**
   - After selecting a suitable graph, open the `gen_ivrt.csv` file from `ReEDS_example/output/` to understand the required data format.

3. **Configure Your Mapping:**
   - Use `launch.bat` to map columns from your CSV file to match the dimensions used in the example file format.

4. **Generate Output Files:**
   - Execute the tool to create new CSV files based on your mapping configuration.

5. **Visualize Results:**
   - Load the generated files into Bokeh Pivot and apply the chosen visualization to your data.

By following these steps, you will be able to map your CSV data, generate the appropriate output files, and visualize your results effectively using Bokeh Pivot.
