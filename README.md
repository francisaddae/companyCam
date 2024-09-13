# companyCam

This project is an Analytical Pipeline orchestrated entirely using Prefect Cloud, focusing on an ELT approach to extract, load, and house data efficiently. The process begins by pulling data from multiple sources and storing it in a data warehouse, ensuring seamless integration and scalability.

Next, DBT (Data Build Tool) is applied for data modeling, documentation, and testing, transforming raw data into a structured, analytics-ready format. DBT ensures that data is well-organized, accurate, and aligned with business needs, automating the testing and documentation processes for enhanced reliability.

Finally, the pipeline showcases the visualization excellence of Tableau, where the processed data is used to build dynamic and interactive dashboards. These visualizations allow stakeholders to gain insights and make informed decisions based on up-to-date, clean data.

In summary, the project integrates Prefect Cloud, DBT, and Tableau to create a robust, end-to-end analytics solution that ensures data quality, efficiency, and powerful reporting.

## Data Engineering

### dependencies

'pip install '

- prefect
- prefect-dbt
- sqlalchemy
- sqlalchemy_clickhouse
- pandas

### *Extract and Load Data*

Gathering Data a Google Sheets using its Authenication Key. The Key is provided within the 'creds.py' file. Data is loaded and housed in CLICKHOUSE WAREHOUSE. Data transformation is done using DBT.

  - `creds.py` : A file containing all the auth_key, name of the sheet, table name and schema.
  - `extract_load.py`: Contains 3 functions. Extracting the data from the sheet, loadibg the data into a specifief Warehouse, Retrieveing final output into a csv.( Used for visualizations)
  - `extract_load_runner.py` : Runs the entire infrastucture using a Prefect Orchestration. A single run should run the model and also trigger the DBT workframe to instanciate the system.

### *Data Modeling*

*Normalizing the table and modeling the table using a star schema*

   - Fact Table: transaction
   - Dimension Table: categories, customers, payments, products, regions

![Star Schema](/pictures/star_schema.png)


DBT Architecture:
  - grooming: Cleaning up data types and removing dealing with missing datas
  - staging: data modeling and additional applied bussiness logic
  - production: tables denormalized for reporting.

![DBT System](/pictures/dbt.png)


### *FLOW DIAGRAM*
![Prefect Flow](/pictures/flow_process.png)

ASSUMPTIONS:

 1. Warehouse Instance is Set Up
 2. Database and Schema are to set up before running the model.
 3. env variables are to be set up.
 4. schema is to be run before code is implemented.( In production, this will be done using Terrform and make setup)


 ## Data Visualization

<https://public.tableau.com/app/profile/francisaddae/viz/companyCam/Story1>



