from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.decorators import dag,task

from datetime import datetime, timedelta 

from Groups.process_tasks import process_tasks



from typing import Dict



@task.python(task_id="extract_partners", do_xcom_push=False,multiple_outputs=True)
def extract():
    partner_name = "netflix"
    partner_path = '/partners/netflix'
    #return partner_name
    return {"partner_name": partner_name, "partner_path": partner_path}

default_args={
    "start_date": datetime(2022, 7, 26)
}

@dag(description="DAG in charge pf processing customer data" 
    ,default_args=default_args,schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=5), tags=["data_science", "customers"],
    catchup =False, max_active_runs=1) 

def my_dag1():

    partner_settings = extract()

    process_tasks(partner_settings)
    
dag = my_dag1()
