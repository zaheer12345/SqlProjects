from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator

from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.decorators import dag,task

from datetime import datetime, timedelta 

from Groups.process_tasks import process_tasks

from airflow.operators.dummy import DummyOperator

from airflow.sensors.date_time import DateTimeSensor



from typing import Dict

partners ={
    "partner_snowflake":
    {
        "name": "snowflake",
        "path": "/partners/snowflake",
        "priority": 2

    },
    "partner_netflix":
    {
        "name": "netflix",
        "path": "/partners/netflix",
        "priority": 3
    },
    "partner_astronmer":
    {
        "name": "astronmer",
        "path": "/partners/astronmer",
        "priority": 1
    },
}



default_args={
    "start_date": datetime(2022, 7, 26)
}

def _choosing_partner_based_on_day(execution_date):
    day = execution_date.day_of_week

    if (day == 1):
        return 'extract_partner_snowflake'
    
    if (day == 3):
        return 'extract_partner_netflix'

    if (day == 5):
        return 'extract_partner_astronmer'

    return 'stop'


@dag(description="DAG in charge pf processing customer data" 
    ,default_args=default_args,schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=5), tags=["data_science", "customers"],
    catchup =False, max_active_runs=1) 

def dynamic_dag1():


    start = DummyOperator(task_id ="start")

    delay = DateTimeSensor(
        task_id = 'delay',
        target_time="{{execution_date.add(hours=9)}}",
        poke_interval = 60 * 60,
        mode = 'reschedule',
        timeout =60 * 60 * 10,

        # check condition is true or not

    )

    choosing_partner_based_on_day = BranchPythonOperator(
        task_id = 'choosing_partner_based_on_day',
        python_callable = _choosing_partner_based_on_day
    )

    stop = DummyOperator(task_id ='stop', wait_for_down_stream =True)

    storing = DummyOperator(task_id='storing', trigger_rule='none_failed_or_skipped')

    choosing_partner_based_on_day >> stop


    for partner, details in partners.items():

          @task.python(task_id=f"extract_{partner}",priority_weight=details['priority'] ,do_xcom_push=False,multiple_outputs=True)
          def extract(partner_name, partner_path): 
          #return partner_name
                return {"partner_name": partner_name, "partner_path": partner_path}
          extracted_values = extract(details['name'], details['path']) 
          start >> delay >>choosing_partner_based_on_day >>extracted_values
          process_tasks(extracted_values)>> storing

    
dag = dynamic_dag1()