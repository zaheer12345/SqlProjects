from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator 
from airflow.providers.postgrres.operator.postgress import PostgressOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta


#variables used by tasks
states = ['CO', 'WA', 'OR', 'CA']
email_to = 'hassanzaheer777@gmail.com'


#instantiate DAG
with DAG ('design_good_practices',
          start_date = datetime(2022, 1, 1),
          max_active_runs=3,
          schedule_interval='@daily',
          default_args= {
            'owner': 'airflow',
            'email_on_fialure': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=1)
          },
          catchup=False,
          template_searchpath='/usr/local/airflow/include' # include path to look for external files
          ) as dag:

    t0 = DummyOperator(task_id='start')

    # Define TaskGroup with postgress Queries, Loop through states provided
    with TaskGroup('covid_table_queries') as covid_table_queries:
        for state in states:
            queries = PostgressOperator(
                task_id = 'covid_query_{0}'.format(state),
                postgres_conn_id='postgres_default',
                sql='covid_state_query.sql',
                params={'state': "'"+ state + "'"}

            )

    #define task to send email
    send_email = EmailOperator(
        task_id = 'send_email',
        to = email_to,
        subject = 'covid Queries DAG',
        html_content= '<p> the covid queries Dag completed succesfully. <p>'
    )

    # Define task dependencies
    t0 >> covid_table_queries >> send_email
