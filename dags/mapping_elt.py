from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3_copy_object import S3CopyObjectOperator
from airflow.providers.amazon.aws.operators.s3_delete_objects import S3DeleteObjectsOperator

from datetime import datetime

@task
def get_s3_files(current_prefix):
    s3_hook = S3Hook(aws_conn_id='s3')
    current_files = s3_hook.list_keys(bucket_name='airflow-kenten', prefix=current_prefix + "/", start_after_key= current_prefix + "/")
    return [[file] for file in current_files]

with DAG(dag_id = 'mapping_elt',
        start_date = datetime(2022, 4, 2),
        catchup=False,
        schedule_interval='@daily') as dag:
    
    copy_to_snowfalke = S3ToSnowflakeOperator.partial(
        task_id= 'load_files_to_snowflake',
        stage = 'kenten_s3_Demo_Stage',
        table = 'COMBINED_HOMES',
        schema = 'KENTENDANS',
        file_format = "(type = 'CSV', field_delimeter = ',', skip_header=1)",
        snowflake_conn_id='snowflake').expand(s3_keys=get_s3_files(current_prefix="{{ds_nodash}}"))

    
    move_s3 = S3CopyObjectOperator(
        task_id = 'move_files_to_proceed',
        aws_conn_id= 's3',
        source_bucket_name='airflow-kenten',
        source_bucket_key= "{{ds_nodash}}" + "/",
        dest_bucket_name = 'airflow-kenten',
        dest_bucket_key = "proceed/"+"{{ds_nodash}}"+"/"
    )

    delete_landing_files = S3DeleteObjectsOperator(
        task_id = 'delete_landing_files',
        bucket = 'airflow-kenten',
        prefix= "{{ds_nodash}}"+"/"
    )

    
    