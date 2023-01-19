from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.decorators import dag,task

from datetime import datetime, timedelta 

from typing import Dict

"""
class CustomPostgresOperator(PostgresOperator):


    template_fields = ('sql', 'parameters')
"""

"""
def _extract(partner_name):
    # partner = Variable.get("my_dag_partner_secret", deserialize_json=True)
    # name = partner['name']
    # api_key = partner['api_secret']
    # path = partner['path']

    print(partner_name)
"""
"""
# 1st Way
def _extract(ti):
    partner_name = "netflix"
    ti.xcom_push(key="partner_name", value=partner_name)

def _process(ti):
    partner_name = ti.xcom_pull(key="partner_name", task_ids="extract")
    print(partner_name)
"""

""""
def _extract(ti):
    partner_name = "netflix"
    partner_path = '/partners/netflix'
    return {"partner_name": partner_name, "partner_path": partner_path}

def _process(ti):
    partner_settings = ti.xcom_pull(task_ids="extract")
    print(partner_settings['partner_name'])
"""

"""
@task.python(task_id="extract_partners", do_xcom_push=False,multiple_outputs=True)
def extract():
    partner_name = "netflix"
    partner_path = '/partners/netflix'
    #return partner_name
    return {"partner_name": partner_name, "partner_path": partner_path}

@task.python
def process(partner_name, partner_path):
    print(partner_name)
    print(partner_path)


@dag(description="DAG in charge pf processing customer data",
    start_date= datetime(2022, 7, 26), schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=5), tags=["data_science", "customers"],
    catchup =False, max_active_runs=1) 

def my_dag():
    
    partner_settings = extract()
    process(partner_settings['partner_name'], partner_settings['partner_path'])

dag = my_dag()

"""


@dag(description="DAG in charge pf processing customer data",
    start_date= datetime(2021, 7, 26), schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=5), tags=["data_science", "customers"],
    catchup =False, max_active_runs=1) 

def my_dag():
    
    @task
    def extract() -> Dict[str, str] :
         
        return {"path": "/tmp/data", "filename" : "data.csv"}

    @task
    def process(path):
        print(path)
  
        return 1

    @task
    def store(is_processed):
        print('store')
    
    tasks = []
    for i in range(4):
        tasks.append(process(extract)['path'])

    tasks >> store()

dag = my_dag()







"""
  with DAG("my_dag", description="DAG in charge pf processing customer data",
    start_date= datetime(2022, 7, 26), schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=5), tags=["data_science", "customers"],
    catchup =False, max_active_runs=1 
    ) as dag :
    
    
    extract =PythonOperator(
        task_id = "extract",
        python_callable=_extract
    )
    
    process = PythonOperator(
        task_id="process",
        python_callable=_process
    )
    """

   

"""
    extract = PythonOperator(
        task_id = "extract",
        python_callable= _extract,
             op_args = [Variable.get("my_dag_partner_secret", deserialize_json=True)['name']]
        op_args = ["{{ var.json.my_dag_partner.name}}"] 
    )
    
    # fetching_data = PostgresOperator(
    #     task_id="fetching_data",
    #     sql ="include/MY_REQUEST.sql"
    # )

    fetching_data = CustomPostgresOperator(
        task_id="fetching_data",
        sql="include/MY_REQUEST.sql",
        parameters={
            'next_ds': '{{next_ds}}',
            'prev_ds': '{{prev_ds}}',
            'partner_name': '{{ var.json.my_dag_partner.name}}'
        }
    )
    """