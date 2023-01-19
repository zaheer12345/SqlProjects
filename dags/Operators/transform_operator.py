import os
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.models.baseOperator import BaseOperator

class SqlTransformOperator(BaseOperator):

# construtor method
    def __init__(self, 
                 table_name: str,
                 schema_name: str,
                 conn_id: str,
                 use_legacy_sql: bool = False, 
                 *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table_name = table_name
        self.schema_name = schema_name
        self.use_legacy_sql = use_legacy_sql


    def execute(self, context):
        with open(os.path.join(os.getcwd(), 'dags', 'sql', 'users.sql'), 'r') as f:
            raw_sql = f.read()

        ## Run the query and store the data in the temp table
        bq_hook = BigQueryHook(use_legacy_sql = False)
        bq_hook.run_query(
            sql = raw_sql,
            destination_dataset_table = f'astronmer-field.airflow_test_1.temp_{table_name}',
            write_disposition = 'WRITE_TRUNCATE'
        )


        ##DQ check
        with open(os.path.join(os.getcwd(), 'dags', 'sql', f'dq_{table_name}.sql')) as f:
            row_sql = f.read()
        check_passed = bq_hook.get_pandas_df(sql = raw_sql) ['check'] [0]
        print(check_passed)

        if check_passed:
            bq_hook.run_query(
                sql = f"""create or replace table 'astronmer-field.airflow_core.{table_name}' copy 'astronmer-field.airflow_test_1.temp_{table_name}'""",
                location = 'US'
            )
        else:
            raise ValueError("DQ checks failed, skipping overwriting Target Table")

