import pandas as pd, os, sqlalchemy
from prefect import task, get_run_logger
from sqlalchemy.exc import OperationalError, IntegrityError, DataError


@task(name="Getting data from GoogleSheets")
def extract_data_from_source(source_locator_auth :str, sheet_name: str) -> pd.DataFrame:
    """This function takes in the google sheet authenicator ID as well as the sheet name.
    Extracting the data within this source is how we

    Args:
        source_locator_auth (str): Authenicator ID of the Google Sheet
        sheet_name (str): Name of the sheet used for

    Returns:
        pd.DataFrame: _description_
    """

    logger = get_run_logger()
    try:
        gsheet_url = (f"https://docs.google.com/spreadsheets/d/{source_locator_auth}/gviz/tq?tqx=out:csv&sheet={sheet_name}")

        df = pd.read_csv(gsheet_url)
        logger.info(df.dtypes)
        df = df.convert_dtypes()
        df.rename( {i : i.lower().replace(' ', '_') for i in df.columns}, axis=1, inplace=True)
        logger.info(df.head(10))
        logger.info(df.dtypes)
        return df

    except Exception as err:
        logger.info(f"Error Message: {err}")
        return False

@task(name="Loading data into Clickhouse")
def load_data_into_warehouse(data:pd.DataFrame, table_name:str) -> bool:
    """This function sets up an authenicator to the WAREHOUSE and loades the data.The function returns a boolean(True) if the data was successfully inserted else False.
    The boolean will trigger a DBT transformation to finish this ELT process.

    DBT will be used to clean the dataset and also help refine any additional key points needed.

    Args:
        data (pd.DataFrame): A raw tabular data from data source
        table_name (str): The name of the table

    Returns:
        bool: True if the upload was successfully else False.
    """
    logger = get_run_logger()
    logger.info(f"Loading {table_name.upper()} data  into CLICKHOUSE warehouse")
    try:
        #instantiate db connection
        engine = sqlalchemy.create_engine(
            f"clickhouse+http://{os.environ.get('CLICKHOUSE_USER')}:{os.environ.get('CLICKHOUSE_CRED')}@{os.environ.get('CLICKHOUSE_HOST')}:{int(os.environ.get('CLICKHOUSE_NATIVE_PORT'))}/{os.environ.get('CLICKHOUSE_DATABASE')}?protocol=https")
        logger.info("***** ENGINE CONNECTED SUCCESSFULLY ******")
    except OperationalError as sql_error:
        logger.info(f"Cannot connect to DB: {sql_error}")

    try:
        if isinstance(data, pd.DataFrame):
            logger.info(f"Loading {table_name.upper()} data  into CLICKHOUSE warehouse")
            data.to_sql(table_name, engine, if_exists='append', index=False)
            logger.info(f"**** {table_name.upper()} TABULAR DATA LOADED SUCCESSFULLY!!! ****")

            return True
    except OperationalError as sql_error:
        logger.info(f"Loading {table_name.upper()} data error: {sql_error}")
        return False
    except IntegrityError as integ_error:
        logger.info(f"Data Integrity Compromised! See underlying data issue. \n Error Message: {integ_error}")
        return False
    except DataError as data_error:
        logger.info(f"Incorrect data type or data_format: {data_error}")
        return False
    finally:
        engine.dispose()


@task(name="download_dbt_model")
def get_dbt_model_data( query, file_name):
    """Downlaods a dbt model from data warehouse and names the file.

    Args:
        type (str): Warehouse Name, default='Postgres'
        query (str): SQL Query
        file_name (str): Name of the file
    """
    logger = get_run_logger()
    try:
        #instantiate db connection
        engine = sqlalchemy.create_engine(
            f"clickhouse+http://{os.environ.get('CLICKHOUSE_USER')}:{os.environ.get('CLICKHOUSE_CRED')}@{os.environ.get('CLICKHOUSE_HOST')}:{int(os.environ.get('CLICKHOUSE_NATIVE_PORT'))}/{os.environ.get('CLICKHOUSE_DATABASE')}?protocol=https")
        logger.info("***** ENGINE CONNECTED SUCCESSFULLY ******")

    except OperationalError as sql_error:
        raise(f"Cannot connect to DB: {sql_error}")

    try:
        result = pd.read_sql(sql=query, con=engine)
        logger.info(result.head())
        result.to_csv(f'{file_name}.csv', index=False, header=True)
    except Exception as e:
        logger.info('DBT or Connection Error Message: %s' %e)
    finally:
        engine.dispose()