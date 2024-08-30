from prefect import flow, task
from prefect_dbt.cli.commands import DbtCoreOperation
from extract_load import extract_data_from_source
from extract_load import load_data_into_warehouse
from extract_load import get_dbt_model_data
from creds import AUTH_KEY, SHEET_NAME, TABLE_NAME


@task(name="companyCAM_DBT")
def trigger_dbt_flow(initaiter=False) -> str:
    if initaiter:
        DbtCoreOperation(
            commands=["dbt run -s commerce+ --full-refresh -t dev"],
            project_dir="/Users/Francis/Desktop/CODE/companyCam/transform",
            profiles_dir="~/.dbt",
        ).run()
        return "yes"
    else:
        return "no"

@flow(name="companyCamELT")
def companyCamELTFlow():
    e = extract_data_from_source(AUTH_KEY, SHEET_NAME)
    l = load_data_into_warehouse(e, TABLE_NAME)
    trigger_dbt_flow(l)
    get_dbt_model_data(
        'SELECT * FROM prod.ecommerce ORDER BY transaction_id',
        'ecommerce'
    )

if __name__ == '__main__':
    companyCamELTFlow()#.serve(name="companyCAM-DEPLOYMENT")