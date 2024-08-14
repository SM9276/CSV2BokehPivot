# CSV2BokehPivot
This project aims to visualize any csv solution file through the use of ReEDS Bokeh Pivot.

## Installation
Run the setup.bat file to install the enviorment needed for this project.

## Getting Started 
Inside the inputs folder place the multiple solution files seperated by folder. 

For example :

-inputs
--model1
---generation.csv
--model2
---generation.csv

Now copy the path to "ReEDS_example" folder and launch Bokeh Pivot, Look for a graph that fits the needs of the data,
For Example:

Generation ivrt 
   
    place picture here

Find the corresponding file in the ReEDs_example/output/. In this example the file would be gen_ivrt.csv
Take a look at the format, 
    
    place table of example 

notice Dim1 is the technology, Dim2 is the name of the generator, Dim3 is the Region, Dim4 is the year and Val is the Generation in GW.

Run launch.bat, enter mapping mode, set the mapping between the solution files and the ReEDS equivilent. 
Select generation.csv, this will list all the columns of the csv file now match the closest match to the Dim1 and so forth.
If you type constant the string will be copied in the Dim choosen. enter Value to match the Val col.
Now type the name of the file that is trying to be replicated

Run launch.bat again, enter execute. The files will be saved to runs folder. Copy the folder path and run Bokeh Pivot. 
Now the data is represented in bokehPivot.




