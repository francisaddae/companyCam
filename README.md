# companyCam

## Data Engineering 

### dependencies 

'pip install ' 

 - prefect 
- prefect-dbt 
- sqlalchemy 
- sqlalchemy_clickhouse 
- pandas 
 
** Extract and Load Data
Gathering Data a Google Sheets using its Authenication Key. The Key is provided within the 'creds.py' file. Data is loaded and housed in CLICKHOUSE WAREHOUSE. Data transformation is done using DBT. 


  - creds.py : A file containing all the auth_key, name of the sheet, table name and schema.
  - extract_load: Contains 3 functions. Extracting the data from the sheet, loadibg the data into a specifief Warehouse, Retrieveing final output into a csv.( Used for visualizations)
  - extract_load_runner.py: Runs the entire infrastucture using a Prefect Orchestration. A single run should run the model and also trigger the DBT workframe to instanciate the system.

ASSUMPTIONS: 

 Warehouse Instance is Set Up 
 Database and Schema are to set up before running the model. 
 env variables are to be set to be set up. 


 ### Data Visualization 
 

 
      
