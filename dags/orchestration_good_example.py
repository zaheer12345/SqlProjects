from airflow import DAG
from airflow.providers.databricks.operator.databricks import DatabricksSubmitRunOperator, DatabricksRunNoOperator
from airflow.providers.postgress.operators.postgress import PostgressOperator
from datetime import datetime, timedelta

# example usecase : we need to refresh a materialized view in a postgress database , then perfoem multiple transformations this data is very large in gigabyte


# define params for submit run operator
new_cluster = {
    'spark_version': '7.3.x-scala2.12',
    'num_workers': 2,
    'node_type_id': 'i3.xlarge',
}

notebook_task = {
    'notebook_path' : '/user/hassanzaheer777@gmail.com/quickstart_notebook'
}

# define params for run Now operator
notebook_params = {
    "variable": 5
}

with DAG('orchestration_good_practises',
    start_date=datetime(2021, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args={
        'owner': 'airflow',
        'email_on_failure': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
    },) as dag:

    opr_refresh_mat_view = PostgressOperator(
        task_id = 'refresh_mat_view',
        postgres_conn_id = 'postgres_default',
        sql='REFRESH MATERIALIZED VIEW example_view;',
    )

    opr_submit_run = DatabricksSubmitRunOperator(
        task_id = 'submit_run',
        databricks_conn_id = 'databricks',
        new_cluster = new_cluster,
        notebook_task = notebook_task
    )

    opr_run_now = DatabricksRunNoOperator(
        task_id = 'run_now',
        databricks_conn_id = 'databricks',
        job_id = 5,
        notebook_params = notebook_params
    )


    opr_refresh_mat_view >> opr_submit_run >> opr_run_now




