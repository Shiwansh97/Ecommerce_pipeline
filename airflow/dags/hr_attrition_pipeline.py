from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="hr_attrition_pipeline",
    start_date=datetime(2024,1,1),
    schedule="@daily",
    catchup=False
) as dag:

    dbt_run = BashOperator(
        task_id="run_dbt_models",
        bash_command="cd /absolute/path/to/dbt_project && dbt run"
    )

    dbt_test = BashOperator(
        task_id="test_dbt_models",
        bash_command="cd /absolute/path/to/dbt_project && dbt test"
    )

    dbt_run >> dbt_test