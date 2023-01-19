import os
import json
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from airflow.providers.http.sensor.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator 
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook 
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from Operators.transform_operator import SqlTransformOperator

default_args ={
    'owner': 'airflow',
    'depends_on_past': False,
    'tags': ['etl', 'example'],
    'params': {'pipeline': 'users'}
}


# def custom_failure_function(context):
#     ## 'Define custom failure notification function'
#     dag_run = context.get('dag_run')
#     error = context['exception']
#     task_instances = context["task_instance"].task
#     print("This task instance failed:", task_instances.task_id, error)


# default_args ={
#     'owner': 'airflow',
#     'start_date': datetime(2022, 1, 1),
#     'on_failure_callback': custom_failure_function,
#     'retries' : 1
# } 







class HttpOperator(SimpleHttpOperator):
    def __init__(self, local_path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.local_path = local_path

    def execute(self, context):
        data = json.loads(super().execute(context))
        with open(self.local_path, 'w') as f:
            for row in data:
                f.write(json.dumps(row) + '\n')


@dag(default_args = default_args, start_date=datetime(2022,1,1), schedule_interval=None, catchup=False)
def users_transform():

    task_check_api_active = HttpSensor(
        task_id = 'is_api_active',
        http_conn_id = 'api_test',
        endpoint = 'users/'
    )

    task_read_api_data = HttpOperator(
        task_id= 'read_api_data',
        method = 'GET',
        http_conn_id = 'api_test',
        endpoint = 'users/',
        local_path=os.path.join(os.getcwd(), 'api_data.json')
    )

    task_upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        src=os.path.join(os.getcwd(), 'api_data.json'),
        dst='incoming/',
        bucket = 'astro-fe-demo'
    )

    task_gcs_to_bigquery =GCSToBigQueryOperator(
        task_id = 'gcs_to_bigquery',
        bucket = 'astro-fe-demo',
        source_objects = ['incoming/api_data.json'],
        destination_project_dataset_table = "astronmer-field.airflow_test_1.users",
        write_disposition = 'WRITE_TRUNCATE',
        source_format = 'NEWLINE_DELIMATED_JSON',
        autodetect = False,
        schema_fields =[
            {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "email", "type": "STRING", "mode": "REQUIRED"},
            {"name": "gender", "type": "STRING", "mode": "REQUIRED"},
            {"name": "status", "type": "STRING", "mode": "REQUIRED"},

        ]

   ) 

    task_transform = SqlTransformOperator(
        task_id = 'transform'
         conn_id = 'google_cloud_default',
         table_name = 'users',
         schema_name= 'airflow_core')


    # @task
    # def transform ():
    #     print(get_current_context())
    #     params = get_current_context()['parmas']
    #     table_name = params['pipeline']
    #     with open(os.path.join(os.getcwd(), 'dags', 'sql', 'users.sql'), 'r') as f:
    #         raw_sql = f.read()

    #     ## Run the query and store the data in the temp table
    #     bq_hook = BigQueryHook(use_legacy_sql = False)
    #     bq_hook.run_query(
    #         sql = raw_sql,
    #         destination_dataset_table = f'astronmer-field.airflow_test_1.temp_{table_name}',
    #         write_disposition = 'WRITE_TRUNCATE'
    #     )


    #     ##DQ check
    #     with open(os.path.join(os.getcwd(), 'dags', 'sql', f'dq_{table_name}.sql')) as f:
    #         row_sql = f.read()
    #     check_passed = bq_hook.get_pandas_df(sql = raw_sql) ['check'] [0]
    #     print(check_passed)

    #     if check_passed:
    #         bq_hook.run_query(
    #             sql = f"""create or replace table 'astronmer-field.airflow_core.{table_name}' copy 'astronmer-field.airflow_test_1.temp_{table_name}'""",
    #             location = 'US'
    #         )
    #     else:
    #         raise ValueError("DQ checks failed, skipping overwriting Target Table")

    # task_check_api_active >> task_read_api_data >> task_upload_to_gcs >> task_gcs_to_bigquery >> transform()
      
    task_check_api_active >> task_read_api_data >> task_upload_to_gcs >> task_gcs_to_bigquery >> task_transform

my_dag = users_transform() 